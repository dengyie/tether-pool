"""
api/server.py - FastAPI service for the IP Pool System
"""

import os
import time
import hmac
import yaml
import asyncio
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import Optional, List, Dict
from fastapi import FastAPI, Query, BackgroundTasks, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, HTMLResponse, RedirectResponse, JSONResponse

from core.models import (
    ProxyNode, PoolStats, LeaseInfo, ProbeResult, CircuitEvent,
    RegistrationLease, IpCooldownRecord, BatteryEventReport,
    BatchAllocationRequest, BatchAllocationResponse, AccountAllocationItem,
    AllocationOverviewResponse, NodeCreateRequest, NodeUpdateRequest,
    HealthStatus, BanReportRequest, BanReportResponse, BanRecord, BanReason
)
from core.storage import Storage
from core.prober import Prober
from core.router import Router
from core.ban import node_isp
from core.metrics import MetricsCollector
from core.allocator import AllocationEngine
from core.quality import IPQualityEvaluator
from core.target_health import validate_probe_target
from core.locks import BINDING_WRITE_LOCK

CONFIG_FILE = os.environ.get("IP_POOL_CONFIG", os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "nodes.yaml"))
DASHBOARD_HTML = os.path.join(os.path.dirname(os.path.dirname(__file__)), "web", "templates", "dashboard.html")

# Optional bearer-token auth for the control plane. When set, /health stays public and
# every other endpoint requires the token, provided as `Authorization: Bearer <token>`,
# a `?token=` query parameter, or the `ip_pool_token` cookie set by /dashboard.
API_TOKEN = os.environ.get("IP_POOL_API_TOKEN", "").strip()
# Per-service tokens (需求 §8, 2026-11-14): 独立于全局 token 的 service 级令牌,
# 格式 `service=token[,service2=token2]` (token 用 hex, 不含逗号/等号)。
# 语义: 请求目标 service (query 或 JSON body) 的专用 token 与其匹配即放行 ——
# 单独吊销某一消费方的令牌不影响其他端点; 全局 API_TOKEN 未设时其余 service
# 维持 Mesh 内网现状 (与需求 §8 "本次接入作为设 token 的契机" 同口径)。
SERVICE_TOKENS: Dict[str, str] = {}
for _pair in os.environ.get("IP_POOL_SERVICE_TOKENS", "").split(","):
    if "=" in _pair:
        _svc, _tok = _pair.split("=", 1)
        if _svc.strip() and _tok.strip():
            SERVICE_TOKENS[_svc.strip()] = _tok.strip()
# 2026-11-19 review P0: 两类令牌都未设时鉴权中间件整体旁路, 破坏性端点
# (DELETE 节点 / bulk-disable / config reload / release) 全靠网络层隔离。
# 不能在这里 fail-closed (会把未配令牌的消费方全部打断), 但必须告警可见。
if not API_TOKEN and not SERVICE_TOKENS:
    print("[auth] ⚠️ IP_POOL_API_TOKEN / IP_POOL_SERVICE_TOKENS 均未设置: "
          "全部端点 (含 DELETE/bulk-disable/config reload 等破坏性写) 仅靠 mesh 网络隔离保护; "
          "生产部署请至少配置其一")
AUTH_COOKIE_NAME = "ip_pool_token"

PROBE_INTERVAL_SECONDS = int(os.environ.get("IP_POOL_PROBE_INTERVAL", "300"))
# 全池 sweep 的并发度。50 并发下 347 节点一轮 ~1 分钟（12s 超时 + warp 高延迟），
# 25 并发会把死节点多的轮次拖到 2.5 分钟以上。可用 env 调回保守值。
PROBE_CONCURRENCY = int(os.environ.get("IP_POOL_PROBE_CONCURRENCY", "50"))
PROBE_HISTORY_KEEP_DAYS = int(os.environ.get("IP_POOL_PROBE_HISTORY_KEEP_DAYS", "7"))
# 病号加速探测 (Traefik unhealthyInterval / HAProxy downinter, 2026-11-15):
# DEGRADED 节点单独走 60s 快轮, 恢复确认 (rise=2) 从最坏 600s 缩到 ~120s;
# healthy 节点保持 300s 全量轮不变。
PROBE_SICK_INTERVAL_SECONDS = int(os.environ.get("IP_POOL_PROBE_SICK_INTERVAL", "60"))
PROBE_SICK_CONCURRENCY = int(os.environ.get("IP_POOL_PROBE_SICK_CONCURRENCY", "20"))
PROBE_SICK_CAP = int(os.environ.get("IP_POOL_PROBE_SICK_CAP", "100"))
# 节点生命周期 janitor (2026-11-15): 连续 DEAD 24h -> 冻结 (frozen tag, 全量探活
# 排除), 连续 7d -> 驱逐 (DB 删除 + tombstone + yaml 同步删除)。env 可调。
JANITOR_INTERVAL_SECONDS = int(os.environ.get("IP_POOL_JANITOR_INTERVAL", "3600"))
JANITOR_FREEZE_HOURS = int(os.environ.get("IP_POOL_JANITOR_FREEZE_HOURS", "24"))
JANITOR_EVICT_DAYS = int(os.environ.get("IP_POOL_JANITOR_EVICT_DAYS", "7"))
# Prober 每次拨号的超时 (settings.probe_timeout_seconds / env 可覆盖)
DEFAULT_PROBE_TIMEOUT = 12.0
# Config files below this node count are treated as incomplete (e.g. a scratch
# override), so startup never acts on the registry based on them.
MIN_CONFIG_NODES_FOR_PRUNE = 10
# Deleting nodes missing from config also deletes their bindings. Default is to mark them
# DEAD and let self-healing re-home; set to 1 to accept the destructive cleanup.
PRUNE_MISSING_NODES = os.environ.get("IP_POOL_PRUNE_MISSING_NODES", "").strip() in ("1", "true", "yes")

storage = Storage()
router = Router(storage)
prober = Prober(storage, router=router,
                timeout=float(os.environ.get("IP_POOL_PROBE_TIMEOUT", str(DEFAULT_PROBE_TIMEOUT))))
allocator = AllocationEngine(storage)
metrics_collector = MetricsCollector(storage)

# Single-flight guard so overlapping sweeps (startup + manual trigger) cannot
# both hammer the pool at once.
_probe_lock = asyncio.Lock()
# host-level force sweep 独立 single-flight (不占全池 _probe_lock, 见 /v1/probe/host)
_host_probe_lock = asyncio.Lock()
HOST_PROBE_CONCURRENCY = int(os.environ.get("IP_POOL_HOST_PROBE_CONCURRENCY", "10"))
_last_probe_trigger_at: float = 0.0
_probe_pending: bool = False  # 已排队但尚未跑完的 sweep (sweep 排队时长可能超过合并窗口)
PROBE_TRIGGER_COALESCE_SECONDS = 15  # 同窗口重复触发合并 (含 start.sh 确认式重试)
_EXPORT_CHANGES_CAP = 20000


def _apply_config_settings(data: dict) -> None:
    """#6 (二轮 review): nodes.yaml 的 settings: 块此前被静默忽略 —— 文件里写的
    probe_interval_seconds 等根本不生效, 误导运维。契约: 配置文件值作为 env 未设
    时的缺省 (env 优先); 同步刷新模块常量 (模块级读取发生在 import 时, 早于本函数)。"""
    global PROBE_INTERVAL_SECONDS, PROBE_CONCURRENCY, PROBE_HISTORY_KEEP_DAYS
    s = data.get("settings") or {}
    if not isinstance(s, dict):
        return
    mapping = {
        "probe_interval_seconds": "IP_POOL_PROBE_INTERVAL",
        "probe_timeout_seconds": "IP_POOL_PROBE_TIMEOUT",
        "probe_concurrency": "IP_POOL_PROBE_CONCURRENCY",
        "probe_history_keep_days": "IP_POOL_PROBE_HISTORY_KEEP_DAYS",
    }
    for key, env_name in mapping.items():
        val = s.get(key)
        if val is None or os.environ.get(env_name):
            continue  # 文件未写该键, 或 env 显式覆盖 -> 尊重 env
        try:
            os.environ[env_name] = str(int(val))
        except (TypeError, ValueError):
            print(f"[-] settings.{key}={val!r} is not an int; ignored")
            continue
    # env 已定稿 (文件缺省已并入), 常量按最终值重读
    PROBE_INTERVAL_SECONDS = int(os.environ.get("IP_POOL_PROBE_INTERVAL", "300"))
    PROBE_CONCURRENCY = int(os.environ.get("IP_POOL_PROBE_CONCURRENCY", "50"))
    PROBE_HISTORY_KEEP_DAYS = int(os.environ.get("IP_POOL_PROBE_HISTORY_KEEP_DAYS", "7"))


def load_initial_nodes():
    if not os.path.exists(CONFIG_FILE):
        print(f"[-] Config file not found: {CONFIG_FILE}")
        return 0
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
        if not data or not isinstance(data, dict):
            print(f"[-] Config file {CONFIG_FILE} parsed empty; skipping node load")
            return 0
        _apply_config_settings(data)
    nodes_data = data.get("nodes", []) or []
    # geo-fixed 护栏 (区域纠偏 2026-11-14): yaml 是配置源而探活是事实源 —— 带
    # geo-fixed 标记的 gen 节点, 其 region 由探活事务按出口真实国家校正过
    # (update_probe_result), yaml 快照可能滞后。upsert ON CONFLICT 无条件覆盖
    # region, 重启会把滞后标签写回去 (下一轮探活虽会再纠正, 但产生错误路由窗口),
    # 故重启加载时对这类节点直接以 DB 权威值覆盖 yaml 条目。
    db_region_by_name = {}
    try:
        for row in storage.list_nodes():
            if "geo-fixed" in (row.tags or []):
                db_region_by_name[row.name] = (row.region, row.provider_region)
    except Exception as e:
        print(f"[!] geo-fixed guard skipped (list_nodes failed: {e})")
    overridden = 0
    for item in nodes_data:
        db_vals = db_region_by_name.get(item.get("name"))
        if db_vals and db_vals[0]:
            item["region"] = db_vals[0]
            item["provider_region"] = db_vals[1]
            overridden += 1
    if overridden:
        print(f"[+] geo-fixed guard: {overridden} node(s) region taken from DB (probe is source of truth)")
    # tombstone 护栏 (janitor 2026-11-15): 连续 DEAD 7d 已被驱逐的节点不在
    # yaml 重载/自上报里复活 —— 否则驱逐会被每次重启撤销。
    tombstones = storage.get_tombstones()
    skipped_tombstoned = 0
    if tombstones:
        before = len(nodes_data)
        nodes_data = [item for item in nodes_data if item.get("name") not in tombstones]
        skipped_tombstoned = before - len(nodes_data)
        if skipped_tombstoned:
            print(f"[+] tombstone guard: {skipped_tombstoned} evicted node(s) skipped on reload "
                  f"(POST /v1/nodes/{{name}}/revive to lift)")
    for item in nodes_data:
            configured_tier = item.get("tier")
            score, init_tier, _ = IPQualityEvaluator.evaluate_node(
                tags=item.get("tags", []),
                egress_type=item.get("egress_type"),
                status="unprobed",
                configured_tier=configured_tier
            )
            node = ProxyNode(
                name=item["name"],
                host=item["host"],
                port=item["port"],
                protocol=item.get("protocol", "socks5h"),
                region=item["region"],
                egress_type=item["egress_type"],
                tags=item.get("tags", []),
                weight=item.get("weight", 100),
                relay_mode=item.get("relay_mode", "relay_fallback"),
                relay_hop_node=item.get("relay_hop_node"),
                # Fault-domain identity may be declared in the authority config; when
                # omitted, ProxyNode derives it from host (see effective_fault_domain).
                fault_domain=item.get("fault_domain", "") or "",
                provider_region=item.get("provider_region", "") or "",
                # Node-level probe target override (mesh CN nodes bypass GFW-blocked
                # Cloudflare probe segments; empty = default ladder in prober).
                probe_url=item.get("probe_url", "") or "",
                tier=init_tier,
                configured_tier=configured_tier,
                quality_score=score
            )
            storage.upsert_node(node)
    print(f"[+] Loaded/upserted {len(nodes_data)} nodes into storage.")

    # Legacy `leases` rows without an allocation predate the universal layer; backfill
    # them so capacity accounting, monitoring and re-homing share one source of truth.
    migrated = storage.migrate_legacy_leases()
    if migrated:
        print(f"[+] Migrated {migrated} legacy lease(s) into account_allocations.")

    # Nodes absent from the authority config are marked DEAD so the existing self-healing
    # path can re-home their bindings. Hard deletion is destructive (it cascades to
    # account_allocations and leases) and therefore requires an explicit opt-in.
    if len(nodes_data) >= MIN_CONFIG_NODES_FOR_PRUNE:
        keep_names = [item["name"] for item in nodes_data]
        if PRUNE_MISSING_NODES:
            pruned = storage.prune_nodes_except(keep_names)
            if pruned:
                print(f"[!] Deleted {len(pruned)} nodes absent from {os.path.basename(CONFIG_FILE)} "
                      f"(IP_POOL_PRUNE_MISSING_NODES=1)")
        else:
            stale = storage.mark_nodes_missing_as_dead(keep_names)
            if stale:
                print(f"[+] Marked {len(stale)} nodes absent from {os.path.basename(CONFIG_FILE)} as DEAD "
                      f"(bindings preserved for self-healing)")
    return len(nodes_data)


async def _scheduled_probe_sweep():
    """One scheduled-sweep tick (extracted from the loop so tests can drive it).

    与手动触发共享 `_probe_pending`（P3-2 根修）：定时 sweep 排队/运行期间手动
    POST /v1/probe 被合并；反之定时 tick 看到已有 pending 的手动触发时跳过——
    双向互斥，任何时刻最多一个全量探活在排队/执行，不会背靠背重复扫全池。
    """
    global _probe_pending
    if _probe_pending or _probe_lock.locked():
        print("[*] Probe sweep already queued/in progress; skipping this tick.")
        return
    _probe_pending = True
    try:
        async with _probe_lock:
            print("[*] Starting scheduled full pool probe...")
            results = await prober.probe_all(concurrency=PROBE_CONCURRENCY)
            healthy = sum(1 for r in results if r.success)
            print(f"[+] Scheduled probe finished: {healthy}/{len(results)} nodes healthy.")
            # prune 是 7 天历史的大 DELETE (百万行级), 事务持有写锁可达秒级;
            # 必须移出事件环 (三轮 review #5: 事件环上一次锁等待冻结全部端点 10s)
            pruned = await asyncio.to_thread(storage.prune_event_tables,
                                             keep_days=PROBE_HISTORY_KEEP_DAYS)
            pruned_total = {k: v for k, v in pruned.items() if v}
            if pruned_total:
                detail = ", ".join(f"{k}={v}" for k, v in pruned_total.items())
                print(f"[+] Pruned stale event rows (>{PROBE_HISTORY_KEEP_DAYS}d): {detail}")
    finally:
        _probe_pending = False


async def background_probe_loop():
    while True:
        try:
            await _scheduled_probe_sweep()
        except Exception as e:
            print(f"[-] Scheduled probe error: {e}")
        await asyncio.sleep(PROBE_INTERVAL_SECONDS)


async def _sick_probe_sweep():
    """One sick-sweep tick: fast re-probe of DEGRADED nodes only (extracted
    for test drivability, same pattern as _scheduled_probe_sweep)."""
    # list_nodes 是全表查询 (3600 行), 移出事件环 (review #5)
    sick = (await asyncio.to_thread(storage.list_nodes,
                                    status=HealthStatus.DEGRADED.value))[:PROBE_SICK_CAP]
    if not sick:
        return
    results = await prober.probe_all(concurrency=PROBE_SICK_CONCURRENCY, nodes=sick)
    recovered = sum(1 for r in results if r.success)
    print(f"[+] Sick probe finished: {recovered}/{len(sick)} degraded nodes recovering.")


async def background_sick_probe_loop():
    while True:
        try:
            await _sick_probe_sweep()
        except Exception as e:
            print(f"[-] Sick probe error: {e}")
        await asyncio.sleep(PROBE_SICK_INTERVAL_SECONDS)


def _remove_from_config(name: str) -> bool:
    """把被驱逐的节点条目从 nodes.yaml 中同步删除 (块级: 从该条目的 'name:' 行
    到下一个 '- name:' 行或文件尾), 使驱逐与配置仓库一致, 防 reload 复活。
    删除前先写 .bak 备份。返回是否删了条目。

    三轮 review P0 修 (2026-11-15): 3426 条目的大文件绝不能 truncate-then-write
    —— 进程崩溃/并发 reload 读到半截文件, 若条目数 > MIN_CONFIG_NODES_FOR_PRUNE
    还会触发毁灭性 prune。tmp + os.replace 原子落盘。"""
    import tempfile as _tempfile
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()
    start = None
    for i, line in enumerate(lines):
        if line.rstrip("\n") == f"- name: {name}" or line.strip() == f"- name: {name}":
            start = i
            break
    if start is None:
        return False
    end = len(lines)
    for j in range(start + 1, len(lines)):
        if lines[j].startswith("- name:") or lines[j].strip() == "- name:":
            end = j
            break
    with open(CONFIG_FILE + ".bak", "w", encoding="utf-8") as f:
        f.writelines(lines)
    del lines[start:end]
    fd, tmp_path = _tempfile.mkstemp(dir=os.path.dirname(CONFIG_FILE) or ".",
                                     prefix=".nodes.yaml.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.writelines(lines)
        os.replace(tmp_path, CONFIG_FILE)  # 原子: 读者要么看旧全量要么看新全量
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise
    return True


def _node_lifecycle_janitor_sweep():
    """One janitor tick (sync; DB/文件操作为主, 不需要 async)。

    探活优先级分层的最末端 (用户需求 2026-11-15): healthy 300s 全量轮 /
    degraded 60s 病号轮 / dead 正常退避冷却 / **连续 DEAD 24h 冻结 (frozen tag,
    全量轮不再探它)** / **连续 DEAD 7d 驱逐 (DB 删 + tombstone + yaml 删)**。
    冻结与驱逐都以 dead_since 为锚, 节点一旦探活恢复 (DEAD 离开) anchor 即被
    update_probe_result 清空, 自然退出冻结/驱逐倒计时。
    """
    # 2026-11-15 review P2: dead_since 锚点是 naive local 时间 (storage 全部
    # datetime.now().isoformat()), cutoff 必须同钟面 —— aware UTC 的 isoformat
    # 带 +00:00 后缀, 字符串比较混两种格式会偏移整个时区差 (CST=8h)。
    now = datetime.now()
    evicted, frozen = [], []
    # ---- 驱逐层 (7d): 必须先于冻结层跑, 否则同批节点先被 freeze 改 tag ----
    evict_cutoff = (now - timedelta(days=JANITOR_EVICT_DAYS)).isoformat()
    for node in storage.list_dead_since_before(evict_cutoff):
        reason = f"janitor_evicted_dead_{JANITOR_EVICT_DAYS}d"
        try:
            storage.record_tombstone(node.name, reason)
            with BINDING_WRITE_LOCK:
                storage.update_node(node.name, {"status": HealthStatus.DEAD.value,
                                                "circuit_state": HealthStatus.DEAD.value})
                router.rehome_leases_for_node(node.name)
                storage.delete_node(node.name, prune_bindings=True)
            _remove_from_config(node.name)
            evicted.append(node.name)
        except Exception as e:
            print(f"[-] Janitor evict {node.name} failed: {e}")
    # ---- 冻结层 (24h): 只处理未冻结的 (idempotent) ----
    freeze_cutoff = (now - timedelta(hours=JANITOR_FREEZE_HOURS)).isoformat()
    for node in storage.list_dead_since_before(freeze_cutoff):
        if "frozen" in node.tags:
            continue
        if storage.freeze_node(node.name):
            frozen.append(node.name)
    if evicted:
        print(f"[JANITOR] evicted {len(evicted)} node(s) dead >= {JANITOR_EVICT_DAYS}d "
              f"(DB + tombstone + yaml): {', '.join(evicted[:20])}")
    if frozen:
        print(f"[JANITOR] frozen {len(frozen)} node(s) dead >= {JANITOR_FREEZE_HOURS}h "
              f"(probe downgraded): {', '.join(frozen[:20])}")
    return {"evicted": evicted, "frozen": frozen}


async def background_node_janitor_loop():
    while True:
        try:
            await asyncio.to_thread(_node_lifecycle_janitor_sweep)
        except Exception as e:
            print(f"[-] Node janitor error: {e}")
        await asyncio.sleep(JANITOR_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_initial_nodes()
    # Initial probe in background on startup
    probe_task = asyncio.create_task(background_probe_loop())
    sick_task = asyncio.create_task(background_sick_probe_loop())
    janitor_task = asyncio.create_task(background_node_janitor_loop())
    yield
    # Await the cancellation so in-flight probe sockets are closed before shutdown
    # instead of being torn down mid-await during interpreter exit.
    probe_task.cancel()
    sick_task.cancel()
    janitor_task.cancel()
    await asyncio.gather(probe_task, sick_task, janitor_task, return_exceptions=True)


app = FastAPI(
    title="IP Pool System",
    description="Unified Global Egress IP Pool System with Dual-Stack Health Probing & Sticky Routing",
    version="1.0.0",
    lifespan=lifespan
)


def _provided_token(request: Request) -> str:
    """Token from the Authorization header, cookie, or query parameter."""
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[len("Bearer "):].strip()
    cookie = request.cookies.get(AUTH_COOKIE_NAME, "")
    if cookie:
        return cookie
    return request.query_params.get("token", "").strip()


async def _request_service(request: Request) -> Optional[str]:
    """返回请求声明的唯一 service scope，冲突时返回不可授权哨兵。

    支持通用 `service` 与反女巫/电池接口使用的 `target_service`；同时检查
    query 和 JSON body，避免额外参数遮蔽 handler 实际消费的业务作用域。
    Starlette 会缓存窥视过的 body，端点仍可正常解析。"""
    scopes = [
        value.strip()
        for key in ("service", "target_service")
        for value in request.query_params.getlist(key)
        if value and value.strip()
    ]
    if (request.headers.get("content-type", "") or "").startswith("application/json"):
        try:
            body = await request.json()
        except Exception:
            body = None
        if isinstance(body, dict):
            scopes.extend(
                value.strip()
                for key in ("service", "target_service")
                for value in (body.get(key),)
                if isinstance(value, str) and value.strip()
            )
    unique_scopes = set(scopes)
    if len(unique_scopes) > 1:
        return "__ambiguous_service_scope__"
    return next(iter(unique_scopes), None)


@app.middleware("http")
async def bearer_auth_middleware(request: Request, call_next):
    if API_TOKEN or SERVICE_TOKENS:
        if request.url.path != "/health":
            token = _provided_token(request)
            svc = await _request_service(request) if SERVICE_TOKENS else None
            service_token_admin_path = (
                request.url.path == "/v1/nodes"
                or request.url.path.startswith("/v1/nodes/")
                or request.url.path.startswith("/v1/config/")
                or request.url.path == "/v1/probe"
                or request.url.path.startswith("/v1/probe/")
                or request.url.path.startswith("/v1/circuit/")
            )

            def _tok_eq(a: Optional[str], b: Optional[str]) -> bool:
                # 时序安全 + bytes 比较 (与网关 _password_ok 同源; str 比较对非 ASCII 也会抛)
                if not a or not b:
                    return False
                return hmac.compare_digest(a.encode("utf-8", "replace"), b.encode("utf-8"))

            if API_TOKEN and _tok_eq(token, API_TOKEN):
                authorized = True
            elif service_token_admin_path:
                # A consumer token must never become an admin credential merely by
                # adding ?service=<consumer> to an infrastructure route.
                authorized = False
            elif SERVICE_TOKENS and svc is not None and token and _tok_eq(token, SERVICE_TOKENS.get(svc)):
                authorized = True
            elif API_TOKEN:
                # 全局令牌模式 (既有行为): 除 /health 外一律要求全局令牌,
                # dashboard 走 ?token= 换 cookie 握手。
                authorized = False
            elif svc is None:
                # 仅 per-service 模式: 读类端点 (GET/HEAD: dashboard / metrics /
                # node_pipeline) 维持 Mesh 内网现状放行 (需求 §8)。
                # 2026-11-15 review api#2: 破坏性端点 (POST/DELETE — delete /
                # bulk-disable / config reload / circuit reset / disable) 若也
                # fail-open, "tokenized 部署" 实际形同虚设 —— 任何无 service
                # 请求都能删节点。写操作一律要求凭据 (全局令牌或对应 service 令牌)。
                if request.method in ("GET", "HEAD", "OPTIONS"):
                    authorized = True
                else:
                    authorized = False
            elif svc not in SERVICE_TOKENS:
                # 未配专用令牌的 service: 读放行; 写一律要求凭据。
                # 2026-11-19 review P0: 旧写法 `not token or method in GET...` 在
                # "无凭据 + 写端点 + ?service=<任意未配名>" 时 fail-open, 一句
                # query 参数就绕过了整个 fail-closed 设计 (注释声称的方向相反)。
                authorized = request.method in ("GET", "HEAD", "OPTIONS")
            else:
                # 目标 service 配了专用令牌: 必须持匹配令牌。
                authorized = False
            if not authorized:
                if API_TOKEN:
                    # Browsers cannot attach an Authorization header to a navigation request,
                    # so entering the dashboard is a two-step handshake: ?token=... is exchanged
                    # for an HttpOnly cookie, after which the page's same-origin fetches carry it.
                    if request.url.path in ("/", "/dashboard") and request.method in ("GET", "HEAD"):
                        return HTMLResponse(
                            "<html><body style='font-family:system-ui;padding:2rem'>"
                            "<h3>IP Pool control plane</h3>"
                            "<p>Access requires a token. Open <code>/dashboard?token=&lt;YOUR_TOKEN&gt;</code>.</p>"
                            "</body></html>",
                            status_code=401,
                        )
                    return JSONResponse({"detail": "Unauthorized"}, status_code=401)
                # 仅有 per-service token 的部署: dashboard 走既有现状, 其余拒绝
                return JSONResponse({"detail": "Unauthorized"}, status_code=401)
        if request.query_params.get("token") and request.url.path == "/dashboard":
            response = RedirectResponse(url="/dashboard", status_code=302)
            response.set_cookie(
                AUTH_COOKIE_NAME, token, httponly=True, samesite="lax", path="/"
            )
            return response
    return await call_next(request)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def index():
    return RedirectResponse(url="/dashboard")


@app.api_route("/dashboard", methods=["GET", "HEAD"], response_class=HTMLResponse)
def get_dashboard():
    if os.path.exists(DASHBOARD_HTML):
        with open(DASHBOARD_HTML, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Dashboard file not found</h1>", status_code=404)


@app.api_route("/health", methods=["GET", "HEAD"])
def health():
    return {"status": "ok", "service": "ip-pool"}


@app.get("/v1/stats", response_model=PoolStats)
def get_stats():
    return storage.get_stats()


@app.get("/v1/nodes", response_model=List[ProxyNode])
def list_nodes(
    region: Optional[str] = None,
    egress_type: Optional[str] = None,
    status: Optional[str] = None,
    tier: Optional[str] = Query(None, description="Optional quality tier filter: premium or standard"),
    # 2026-11-19 review P2: 全表 ~3600 行 × 30 字段无界返回, 加游标式上限
    limit: int = Query(0, ge=0, le=20000, description="Cap the number of returned nodes (0 = no cap)")
):
    nodes = storage.list_nodes(region=region, egress_type=egress_type, status=status, tier=tier)
    return nodes[:limit] if limit else nodes


@app.get("/v1/nodes/{name}", response_model=ProxyNode)
def get_node(name: str):
    """Retrieves a single proxy node by its unique identifier."""
    node = storage.get_node(name)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node '{name}' not found")
    return node


@app.post("/v1/nodes", response_model=ProxyNode)
def create_or_upsert_node(req: NodeCreateRequest):
    """
    Directly creates or updates a proxy node in the pool via API.
    Evaluates quality scores and configures quality tiers dynamically.
    Tags node with 'api_managed' to prevent pruning or demotion during static config reloads.

    2026-11-15 review P2: 被驱逐 (7d 死亡) 的节点名带 tombstone —— 任何消费方
    重新 POST 同名节点都会绕过 reload 护栏无声复活。必须走 /revive 显式通道。
    """
    if req.name in storage.get_tombstones():
        raise HTTPException(
            status_code=409,
            detail=f"Node '{req.name}' is tombstoned (janitor-evicted); "
                   f"use POST /v1/nodes/{req.name}/revive to restore it")
    configured_tier = req.configured_tier or req.tier
    tags = list(req.tags)
    if "api_managed" not in tags:
        tags.append("api_managed")

    score, init_tier, _ = IPQualityEvaluator.evaluate_node(
        tags=tags,
        egress_type=req.egress_type,
        status="unprobed",
        configured_tier=configured_tier
    )
    final_tier = req.tier or init_tier
    node = ProxyNode(
        name=req.name,
        host=req.host,
        port=req.port,
        protocol=req.protocol,
        region=req.region,
        egress_type=req.egress_type,
        tags=tags,
        weight=req.weight,
        relay_mode=req.relay_mode,
        relay_hop_node=req.relay_hop_node,
        tier=final_tier,
        configured_tier=configured_tier,
        quality_score=score,
        fault_domain=req.fault_domain,
        provider_region=req.provider_region,
        probe_url=req.probe_url
    )
    storage.upsert_node(node)
    saved = storage.get_node(req.name)
    if not saved:
        raise HTTPException(status_code=500, detail="Failed to save node")
    return saved


@app.patch("/v1/nodes/{name}", response_model=ProxyNode)
def update_node(name: str, req: NodeUpdateRequest):
    """
    Updates specific attributes of an existing proxy node (weight, tags, tier, status, relay).
    If status is set to dead, automatically triggers rehoming of active account bindings.
    """
    existing = storage.get_node(name)
    if not existing:
        raise HTTPException(status_code=404, detail=f"Node '{name}' not found")

    updates = req.model_dump(exclude_unset=True)
    if not updates:
        return existing

    if "tier" in updates and "configured_tier" not in updates:
        updates["configured_tier"] = updates["tier"]

    updated_node = storage.update_node(name, updates)
    if not updated_node:
        raise HTTPException(status_code=404, detail=f"Node '{name}' not found")

    if updates.get("status") == HealthStatus.DEAD.value:
        router.rehome_leases_for_node(name)

    return updated_node


@app.post("/v1/nodes/bulk-disable")
def bulk_disable_nodes(req: dict):
    """批量管理员禁用: body {"tag": "skyward"} 或 {"region": "hk"} (单池级熔断处置)。
    语义同单节点 disable: 探活不碰、不入分配、重启后仍生效; rehome 全部绑定。
    返回 {disabled: [...], skipped: [...]}。"""
    tag = (req.get("tag") or "").strip()
    region = (req.get("region") or "").strip()
    if not tag and not region:
        raise HTTPException(status_code=422, detail="body must provide 'tag' or 'region'")
    if tag and region:
        raise HTTPException(status_code=422, detail="provide only one of 'tag' or 'region'")
    reason = (req.get("reason") or f"bulk_disable:{tag or region}")[:80]
    pool = [n for n in storage.list_nodes(region=region or None)
            if tag in (n.tags or [])] if tag else storage.list_nodes(region=region)
    targets = [n for n in pool if n.status != HealthStatus.DISABLED.value]
    disabled, skipped = [], []
    with BINDING_WRITE_LOCK:
        for n in targets:
            updated = storage.update_node(n.name, {"status": HealthStatus.DISABLED.value,
                                                   "circuit_state": HealthStatus.DISABLED.value})
            if not updated:
                skipped.append(n.name)
                continue
            storage.record_circuit_event(n.name, n.status, HealthStatus.DISABLED.value, reason)
            router.rehome_leases_for_node(n.name)
            disabled.append(n.name)
    return {"disabled": disabled, "skipped": skipped, "count": len(disabled)}


@app.post("/v1/nodes/{name}/disable", response_model=ProxyNode)
def disable_node(name: str, req: dict = None):
    """管理员禁用 (2026-11-15): 与自动 DEAD 彻底分离 —— 探活跳过、永不自动恢复、
    不入分配; 重启/yaml 重载后仍生效 (upsert 保 DISABLED); 订阅池刷新重建节点
    条目时自然解除。节点上的绑定立即 rehome。"""
    existing = storage.get_node(name)
    if not existing:
        raise HTTPException(status_code=404, detail=f"Node '{name}' not found")
    if existing.status == HealthStatus.DISABLED.value:
        return existing
    reason = ((req or {}).get("reason") or "admin_disabled")[:80]
    with BINDING_WRITE_LOCK:
        updated = storage.update_node(name, {"status": HealthStatus.DISABLED.value,
                                             "circuit_state": HealthStatus.DISABLED.value})
        if not updated:
            raise HTTPException(status_code=500, detail="Failed to disable node")
        storage.record_circuit_event(name, existing.status, HealthStatus.DISABLED.value, reason)
        router.rehome_leases_for_node(name)
    return storage.get_node(name)


@app.post("/v1/nodes/{name}/enable", response_model=ProxyNode)
def enable_node(name: str):
    """解除管理员禁用: 回 UNPROBED, 由下一轮探活重新定级 (不直接 HEALTHY,
    避免 disabled 期间上游状态未知导致假阳性)。同时清 frozen tag —— 管理员
    明确按了恢复键, janitor 冻结层应让节点重新进全量探活。"""
    existing = storage.get_node(name)
    if not existing:
        raise HTTPException(status_code=404, detail=f"Node '{name}' not found")
    if existing.status != HealthStatus.DISABLED.value:
        return existing
    new_tags = [t for t in (existing.tags or []) if t != "frozen"]
    storage.update_node(name, {"status": HealthStatus.UNPROBED.value,
                               "circuit_state": HealthStatus.UNPROBED.value,
                               "consecutive_failures": 0,
                               "consecutive_successes": 0,
                               "next_probe_allowed_at": None,
                               "dead_since": None,
                               "tags": new_tags})
    storage.record_circuit_event(name, HealthStatus.DISABLED.value,
                                 HealthStatus.UNPROBED.value, "admin_enabled")
    return storage.get_node(name)


@app.post("/v1/nodes/{name}/revive", response_model=ProxyNode)
def revive_node(name: str):
    """人工复活被 janitor 驱逐的节点 (tombstone 护栏只在 load_initial_nodes 挡
    yaml 重载; 管理员确认节点真的回来了, 显式清 tombstone + 重建条目)。

    三轮 review P1 修 (2026-11-15): 必须【先验证 yaml/DB 条目可复活, 再清
    tombstone】—— 反过来时, 若 yaml 里已无该条目 (驱逐路径本就删了它), 404
    已经把 tombstone 清掉了, 之后配置仓库同步回来该节点就绕过护栏无声复活。
    验证失败时 tombstone 原样保留。"""
    # 1) 先取 yaml 条目 (只读, 不动任何状态)
    import yaml as _yaml
    yaml_item = None
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        cfg = _yaml.safe_load(f) or {}
    for item in (cfg.get("nodes") or []):
        if item.get("name") == name:
            yaml_item = item
            break
    if not yaml_item and not storage.get_node(name):
        raise HTTPException(
            status_code=404,
            detail=f"Cannot revive '{name}': not in config file (tombstone kept)")
    # 2) 验证通过才清 tombstone (无 tombstone 也放行, 幂等)
    storage.revive_tombstoned(name)
    if not storage.get_node(name) and yaml_item:
        storage.upsert_node(ProxyNode(**{k: v for k, v in yaml_item.items()
                                         if k in ProxyNode.model_fields}))
    node = storage.get_node(name)
    if not node:
        raise HTTPException(status_code=404, detail=f"'{name}' not found after revive attempt")
    storage.update_node(name, {"status": HealthStatus.UNPROBED.value,
                               "circuit_state": HealthStatus.UNPROBED.value,
                               "consecutive_failures": 0,
                               "next_probe_allowed_at": None,
                               "dead_since": None,
                               "tags": [t for t in (node.tags or []) if t != "frozen"]})
    storage.record_circuit_event(name, HealthStatus.DEAD.value,
                                 HealthStatus.UNPROBED.value, "admin_revived_from_tombstone")
    return storage.get_node(name)


@app.delete("/v1/nodes/{name}")
def delete_node(
    name: str,
    rehome: bool = Query(True, description="Automatically rehome attached accounts before deleting")
):
    """
    Deletes a proxy node from the pool.
    Optionally auto-rehomes any bound accounts before removal.
    Uses BINDING_WRITE_LOCK and pre-marks node as DEAD so concurrent acquisitions cannot target it.
    """
    with BINDING_WRITE_LOCK:
        existing = storage.get_node(name)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Node '{name}' not found")

        # Pre-mark node as DEAD so no concurrent requests can select it during rehoming
        storage.update_node(name, {"status": HealthStatus.DEAD.value, "circuit_state": HealthStatus.DEAD.value})

        rehome_result = None
        if rehome:
            rehome_result = router.rehome_leases_for_node(name)

        deleted = storage.delete_node(name, prune_bindings=True)
        return {
            "deleted": deleted,
            "node_name": name,
            "rehome_result": rehome_result
        }


@app.post("/v1/config/reload")
def reload_config():
    """
    Hot-reloads node topology from disk without requiring a systemd service restart.
    """
    try:
        count = load_initial_nodes()
        return {
            "reloaded": True,
            "config_file": CONFIG_FILE,
            "nodes_loaded": count
        }
    except Exception as e:
        # F6 同口径: 配置异常串可能含磁盘路径, 只落日志
        print(f"[-] reload config failed: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Failed to reload config (internal error, see api logs)")



def _lease_from_allocation(account_id: str, alloc: dict, st: Optional[Storage] = None) -> Optional[LeaseInfo]:
    target_storage = st or storage
    node = target_storage.get_node(alloc["node_name"])
    if not node:
        return None
    return LeaseInfo(
        account_id=account_id,
        node_name=node.name,
        proxy_url=node.proxy_url,
        exit_ip=node.exit_ip,
        region=node.region,
        egress_type=node.egress_type,
        assigned_at=alloc["allocated_at"],
        tier=node.tier
    )


@app.get("/v1/acquire", response_model=LeaseInfo)
def get_bound_lease(
    account_id: str = Query(..., description="Unique account ID / email"),
    service: Optional[str] = Query(None, description="Service namespace (defaults to 'default')")
):
    """
    Reads the account's current binding without creating one.

    Acquiring is a state change and lives on POST; a GET must stay safe so that
    prefetches, retries and link previews cannot silently bind accounts or consume
    capacity.
    """
    svc = service or "default"
    alloc = storage.find_allocation_by_account(account_id, service=svc)
    lease = _lease_from_allocation(account_id, alloc) if alloc else None
    if not lease:
        # 2026-11-19 review P3: legacy leases 表无 service 维度, 仅当该账号在
        # 任何 service 下都没有 allocation 时才可信 —— 否则会把 B 服务的绑定
        # 以 A 服务的名义泄露 (与 _resolve_ban_node 的守卫同口径)。
        if storage.find_allocation_by_account(account_id) is None:
            legacy = storage.get_lease(account_id)
            if legacy:
                lease = legacy
    if not lease:
        raise HTTPException(status_code=404, detail=f"No binding for account '{account_id}' in service '{svc}'")
    return lease


@app.post("/v1/acquire", response_model=LeaseInfo)
def acquire_node(
    account_id: str = Query(..., description="Unique account ID / email"),
    egress_type: Optional[str] = Query(None, description="Optional egress type constraint (warp_ipv6, native_ipv4, native_ipv6)"),
    region: Optional[str] = Query(None, description="Optional target region (tokyo, netherlands, singapore, india)"),
    tier: Optional[str] = Query(None, description="Optional quality tier constraint (premium or standard)"),
    tier_policy: Optional[str] = Query(None, description="Optional tier routing policy (standard_first, premium_first, premium_only, standard_only)"),
    force_rebind: bool = Query(False, description="Force re-allocating a new node"),
    # 2026-11-19 review P2: /v1/allocation/acquire 用 force_rebalance, 两个名字
    # 语义相同; 拼错的名字此前被 FastAPI 静默忽略 → 强制换绑 no-op 且 200。
    force_rebalance: bool = Query(False, description="Alias of force_rebind (accepted on both endpoints to kill the param drift)"),
    service: Optional[str] = Query(None, description="Service namespace (defaults to 'default')"),
    region_strict: bool = Query(False, description="Treat region as a hard constraint: never relax it to another region (empty candidate set instead)"),
    bind_persist: bool = Query(False, description="Persist the selected node even when the stored binding only failed this call's constraint filter (reason=constraint_rebind)")
):
    """
    Acquires (or returns) the account's sticky binding.

    `egress_type`/`region`/`tier` are optional constraints for this call: when the account
    already has a healthy binding that does not match them, the constrained node is
    returned without overwriting the stored binding. Use `force_rebind=true` to
    deliberately re-point the account.

    `region_strict=true` stops the ladder from relaxing `region`: with no candidate in
    the requested region the call returns 503 instead of a cross-region fallback.

    `bind_persist=true` persists the constrained selection (reason `constraint_rebind`)
    and stores the call's constraint set with the binding, so auto_rehome keeps the
    account inside its promised region.
    """
    lease = router.acquire_for_account(
        account_id=account_id,
        egress_type=egress_type,
        region=region,
        tier=tier,
        tier_policy=tier_policy,
        force_rebind=force_rebind or force_rebalance,
        service=service,
        region_strict=region_strict,
        bind_persist=bind_persist
    )
    if not lease:
        raise HTTPException(status_code=503, detail="No available proxy node matching criteria")
    return lease


@app.post("/v1/probe")
async def trigger_probe(
    background_tasks: BackgroundTasks,
    force: bool = Query(False, description="Force-probe DEAD nodes even inside circuit cooldown (fast recovery after bridge-core restart)")
):
    """Trigger a full pool probe.

    确定性语义 (2026-11-12 根修): 排队等待 `_probe_lock` 而不是撞锁静默丢弃——
    重启后的 force 探活必须必然执行 (此前与后台扫描循环共锁 + 双侧静默跳过,
    start.sh 启动尾的触发会被首轮全量扫描吞掉)。同窗口内的重复触发合并去重,
    防御未鉴权端点被刷成探活风暴。
    """
    global _last_probe_trigger_at, _probe_pending
    now = time.monotonic()
    was_locked = _probe_lock.locked()
    # 合并条件 = 「15s 窗口内已触发过」或「已排队/在跑的 sweep 尚未结束」。
    # 只看时间窗会漏掉排队时长超过窗口的 sweep (排队任务同样满足去重目的)。
    if _probe_pending or now - _last_probe_trigger_at < PROBE_TRIGGER_COALESCE_SECONDS:
        # clamp ≥0: pending=True 而时间窗已过时，差值为负 —— 报给调用方/日志的
        # 剩余窗口只能表达「还有多久可再触发」，负数无意义 (P3-3)。
        remaining = round(max(0.0, PROBE_TRIGGER_COALESCE_SECONDS - (now - _last_probe_trigger_at)), 1)
        print(f"[probe] trigger coalesced (force={force}, pending={_probe_pending}, "
              f"window remaining={remaining}s, queued_behind_sweep={was_locked})")
        return {
            "message": "Probe trigger coalesced (a recent trigger is already queued)",
            "force": force,
            "coalesced": True,
            "queued_behind_running_sweep": was_locked,
            "window_remaining_seconds": remaining,
        }
    _last_probe_trigger_at = now
    _probe_pending = True

    async def _run_probe():
        try:
            async with _probe_lock:
                print(f"[probe] sweep starting (force={force}, queued_behind_sweep={was_locked})")
                results = await prober.probe_all(concurrency=PROBE_CONCURRENCY, force=force)
                healthy = sum(1 for r in results if r.success)
                print(f"[probe] sweep finished: {healthy}/{len(results)} healthy (force={force})")
        finally:
            global _probe_pending  # noqa: F811 — 闭包内重新声明为模块全局
            _probe_pending = False

    background_tasks.add_task(_run_probe)
    return {
        "message": "Full pool probe queued in background",
        "force": force,
        "coalesced": False,
        "queued_behind_running_sweep": was_locked,
    }


@app.post("/v1/probe/host", response_model=List[ProbeResult])
async def probe_host(
    host: str = Query(..., description="Host IP (e.g. 10.144.144.81) to force-probe all its nodes"),
):
    """Host-level force probe (2026-11-19 warp 隧道僵死事故的运维抓手).

    场景: 宿主机 sing-box 重启/隧道层修复后, 单节点断路器退避使恢复最长等
    ~30min (MAX_COOLDOWN)。本端点对该 host 全部节点逐个 force=True 探活
    (绕过冷却), 同步返回每个节点的结果 —— 「重启 sing-box → 强制拉活」
    SOP 的一条命令通道。

    语义: 独立于全池 sweep 的 single-flight (自持小锁, 不占 _probe_lock),
    节点级 force probe 无冷却跳过, 失败也如实返回 —— 调用方 (运维/看门狗)
    拿到的就是隧道层的真实判决。
    """
    nodes = await asyncio.to_thread(storage.list_nodes)
    host_nodes = [n for n in nodes if n.host == host]
    if not host_nodes:
        raise HTTPException(status_code=404, detail=f"No nodes found for host {host}")

    async with _host_probe_lock:
        print(f"[probe] host sweep starting: {host} ({len(host_nodes)} nodes, force)")
        results = await prober.probe_all(concurrency=HOST_PROBE_CONCURRENCY,
                                         nodes=host_nodes, force=True)
        healthy = sum(1 for r in results if r.success)
        print(f"[probe] host sweep finished: {host} {healthy}/{len(results)} healthy")
    return results


@app.post("/v1/probe/test_node", response_model=ProbeResult)
async def test_single_node(node_name: str = Query(..., description="Node name to test")):
    """Immediately test a single node over EasyTier and return live egress result"""
    node = storage.get_node(node_name)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node {node_name} not found")
    res = await prober.probe_node(node, force=True)
    return res


@app.post("/v1/probe/target", response_model=ProbeResult)
async def test_target_service(
    node_name: str = Query(..., description="Node name to test"),
    target_url: str = Query(..., description="Target service URL to probe (e.g. https://x.com/i/flow/signup)")
):
    """Probe a specific business target URL through a proxy node to test anti-bot evasion"""
    node = storage.get_node(node_name)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node {node_name} not found")
    try:
        validate_probe_target(target_url)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    res = await prober.probe_target_service(node, target_url)
    return res


@app.get("/v1/bindings/epoch")
def get_bindings_epoch(service: str = Query("default", min_length=1)):
    """
    R5(b): current monotonic binding epoch for a service.

    Downstreams poll this cheaply; when `epoch` differs from the value they last
    reconciled they pull `GET /v1/export/grok2api?since_epoch=N` for the delta.
    This replaces "import once and trust it forever", the pattern that let 511
    grok2api accounts keep dialling mesh-nl after the host was gone.
    """
    info = storage.get_binding_epoch(service)
    if info is None:
        return {"service": service, "epoch": 0, "changed_at": None, "last_change_reason": None}
    return info


@app.get("/v1/export/grok2api")
def export_grok2api(
    since_epoch: Optional[int] = Query(
        None, ge=0,
        description="When set, return only the bindings that moved after this epoch"
    )
):
    """Export healthy nodes formatted for grok2api egress node injection.

    With `since_epoch=N` the response carries only accounts whose binding moved since epoch
    N (mapped to their *current* node, not the intermediate hop) plus the current epoch, so
    a downstream can reconcile incrementally instead of doing a one-time static import.
    """
    if since_epoch is not None:
        service = "grok2api"
        snapshot = storage.get_binding_delta_snapshot(
            service, since_epoch, _EXPORT_CHANGES_CAP
        )
        changes = snapshot["changes"]
        # 末条变更定归属 (2026-11-22 review P1): 同账号在本页内可同时出现 move 与
        # delete 行 (先 rebind 后配额回收), 若按"先到先得"去重, delete-after-move
        # 会让下游保留幽灵绑定。变更流按 epoch,id 升序, 每账号取末条决定归属。
        final_by_account: Dict[str, object] = {}
        for change in changes:
            final_by_account[change.account_id] = change
        assignments = {}
        deletions = []
        for acc, change in final_by_account.items():
            if change.to_node is None:
                deletions.append(acc)
                continue
            alloc = snapshot["allocations"].get(acc)
            node = snapshot["nodes"].get(alloc["node_name"]) if alloc else None
            if not node:
                deletions.append(acc)
                continue
            assignments[acc] = {
                "name": node.name,
                "url": f"socks5://{node.host}:{node.port}",
                "region": node.region,
                "egress_type": node.egress_type,
                "exit_ip": node.exit_ip or ""
            }
        nodes = []
        seen_nodes = set()
        for assignment in assignments.values():
            if assignment["name"] not in seen_nodes:
                seen_nodes.add(assignment["name"])
                nodes.append(assignment)
        return {
            "count": len(assignments),
            "truncated": snapshot["truncated"],
            "has_more": snapshot["has_more"],
            "nodes": nodes,
            "assignments": assignments,
            "deletions": deletions,
            "epoch": snapshot["epoch"],
            "since_epoch": since_epoch,
            "changed_at": snapshot["changed_at"],
            "last_change_reason": changes[-1].reason if changes else None,
            "reason_breakdown": _binding_reason_breakdown(changes),
        }

    nodes = storage.list_nodes(status="healthy")
    if not nodes:
        nodes = storage.list_nodes()  # fallback

    egress_nodes = []
    for n in nodes:
        egress_nodes.append({
            "name": n.name,
            "url": f"socks5://{n.host}:{n.port}",
            "region": n.region,
            "egress_type": n.egress_type,
            "exit_ip": n.exit_ip or ""
        })
    return {"count": len(egress_nodes), "nodes": egress_nodes}


def _binding_reason_breakdown(changes) -> Dict[str, int]:
    breakdown: Dict[str, int] = {}
    for change in changes:
        breakdown[change.reason] = breakdown.get(change.reason, 0) + 1
    return breakdown


@app.get("/metrics", response_class=PlainTextResponse)
def get_metrics():
    """Prometheus OpenMetrics exporter with low-cardinality labels"""
    return PlainTextResponse(
        content=metrics_collector.generate_metrics(),
        media_type="text/plain; version=0.0.4; charset=utf-8"
    )


@app.get("/v1/circuit/events", response_model=List[CircuitEvent])
def list_circuit_events(limit: int = Query(50, ge=1, le=200)):
    """Retrieve recent circuit breaker transition and self-healing events"""
    return storage.list_circuit_events(limit=limit)


@app.post("/v1/circuit/reset")
def reset_node_circuit(node_name: str = Query(..., description="Node name to reset")):
    """Operator manual reset of a node back to healthy state"""
    success = storage.reset_node_circuit(node_name)
    if not success:
        raise HTTPException(status_code=404, detail=f"Node {node_name} not found")
    return {"message": f"Node {node_name} circuit reset to healthy"}


@app.post("/v1/circuit/rehome")
def trigger_node_rehome(node_name: str = Query(..., description="Node name whose leases should be migrated")):
    """Trigger manual lease re-homing for a node"""
    result = router.rehome_leases_for_node(node_name)
    return result


# --- Registration Anti-Sybil Endpoints ---

# --- Registration Anti-Sybil Endpoints ---

@app.post("/v1/acquire_for_registration", response_model=RegistrationLease)
def acquire_for_registration(
    account_id: str = Query(..., description="Unique registration account ID / email"),
    target_service: str = Query(..., description="Target service identifier (e.g., grok_reg, openai_signup)"),
    cooldown: int = Query(1800, ge=0, le=86400, description="IP cooldown seconds after registration (0-86400)"),
    region: Optional[str] = Query(None, description="Optional target region preference"),
    egress_type: Optional[str] = Query(None, description="Optional preferred egress type (clash_proxy, native_ipv4, warp_ipv6)"),
    tier: Optional[str] = Query("premium", description="Optional quality tier preference (defaults to premium for registration protection)")
):
    """
    Dedicated anti-sybil egress lease assignment for batch registration bots.
    Ensures exit IP has not been used for target_service within the cooldown period.

    2026-11-15 review api#7: GET → POST —— 本端点消耗 IP cooldown 并持久化
    lease, 是状态变更; GET 会带 cookie 的链接预取/预加载静默烧 cooldown
    (与 GET /v1/acquire 的 safe-GET 不变式同一条)。
    """
    lease = router.acquire_for_registration(
        account_id=account_id,
        target_service=target_service,
        region=region,
        egress_type=egress_type,
        tier=tier,
        cooldown_seconds=cooldown
    )
    if not lease:
        raise HTTPException(status_code=503, detail="No clean proxy node available for registration (all candidates cooled)")
    return lease


@app.get("/v1/cooldowns", response_model=List[IpCooldownRecord])
def list_cooldowns(target_service: Optional[str] = Query(None, description="Filter by target service")):
    """List active IP cooldown locks for anti-sybil tracking"""
    return storage.get_active_cooldowns(target_service=target_service)


@app.post("/v1/release_cooldown")
def release_cooldown(
    exit_ip: str = Query(..., description="Exit IP to release"),
    target_service: str = Query(..., description="Target service")
):
    """Early unlock of an IP from cooldown (e.g. registration failed before form submission)"""
    success = storage.release_ip_cooldown(exit_ip=exit_ip, target_service=target_service)
    return {"success": success, "exit_ip": exit_ip, "target_service": target_service}


# --- Battery Lifecycle & Hot Swapping Endpoints ---

@app.post("/v1/battery/event")
def report_battery_event(report: BatteryEventReport):
    """
    Downstream battery consumers (e.g., grok2api, cpa-manager-plus) report 429/401/Turnstile failures.
    Triggers automatic battery swapping (lease re-homing to a fresh healthy node).
    """
    result = router.handle_battery_event(report)
    return result


@app.get("/v1/battery/events")
def list_battery_events(
    account_id: Optional[str] = Query(None, description="Filter by account ID"),
    limit: int = Query(50, ge=1, le=200)
):
    """List recent battery lifecycle events"""
    return storage.list_battery_events(account_id=account_id, limit=limit)


# --- Ban Feedback & Quarantine (ban-aware allocation, 2026-11-14 需求单) ---

BAN_RATE_LIMIT_PER_HOUR = int(os.environ.get("IP_POOL_BAN_RATE_LIMIT_PER_HOUR", "30"))
QUARANTINE_FLOOD_RATIO = 0.30  # 单 service 每日 quarantine > 池健康节点 30% -> warning (需求 §5.5)


def _resolve_ban_node(report: BanReportRequest) -> ProxyNode:
    """node_id 缺省时按 service+account 找当前绑定节点 (allocation -> legacy lease)。

    lease 兜底与 Router 1b 同守卫 (2026-11-14 review F2 根因修复): 该 account
    在全库存在任意 allocation 镜像时, lease 行的 service 归属不明 (leases 表无
    service 维度), 不得猜测后塞进本 service 的 quarantine —— 跨 service 泄漏
    比解析失败更糟。仅纯 legacy 账户 (无任何 allocation) 才信 lease。"""
    if report.node_id:
        node = storage.get_node(report.node_id)
        if not node:
            raise HTTPException(status_code=400, detail=f"Unknown node_id: {report.node_id}")
        return node
    alloc = storage.get_allocation(report.service, report.account)
    if alloc and alloc.get("node_name"):
        node = storage.get_node(alloc["node_name"])
        if node:
            return node
    if storage.find_allocation_by_account(report.account) is None:
        lease = storage.get_lease(report.account)
        if lease:
            node = storage.get_node(lease.node_name)
            if node:
                return node
    raise HTTPException(status_code=400, detail="No node_id given and no binding found for service+account")


@app.post("/v1/feedback/ban", response_model=BanReportResponse, status_code=202)
def report_ban(report: BanReportRequest):
    """
    封禁上报 (需求 §4.1): 边缘 HTML 405 = IP 级封禁权威信号 -> service 维度 quarantine。
    幂等: 同 node+reason 在隔离窗内重复上报返回同一 ban_id (不叠加时长)。
    判封门 (§5.1): unusual_activity_3012 且 evidence.body_kind != html -> 只记录不隔离。
    限流 (§8): 单 service 每小时 <= 30 次, 超出 429。
    """
    # scope 契约诚实 (review F8): P0 只实现 service 维度, 其他值显式拒绝而非静默
    # 当 service 处理 —— 消费方传 scope=global 期待全局隔离时必须得到明确失败。
    if report.scope and report.scope != "service":
        raise HTTPException(status_code=422, detail=f"scope '{report.scope}' is not implemented; this build only supports scope=service")
    # 限流: 计数含只记录的上报 (消费方 bug 打穿控制面同样要挡)
    if storage.count_bans_since(report.service, (datetime.now() - timedelta(hours=1)).isoformat()) >= BAN_RATE_LIMIT_PER_HOUR:
        raise HTTPException(status_code=429, detail=f"ban report rate limit exceeded ({BAN_RATE_LIMIT_PER_HOUR}/hour for service {report.service})")

    node = _resolve_ban_node(report)

    quarantine_hours = report.quarantine_hours
    if report.reason == BanReason.UNUSUAL_ACTIVITY_3012.value and (report.evidence or {}).get("body_kind") != "html":
        # 应用层 JSON 3012 在 zcode 语境多为「请求形状不对」(2.7.5 口径):
        # 默认不触发隔离, 由 evidence.body_kind 决定 —— 消费方确认是边缘 HTML
        # 才隔离, 否则只记录。
        quarantine_hours = 0

    record, idempotent = storage.record_ban(
        service=report.service,
        account=report.account,
        node_name=node.name,
        exit_ip=node.exit_ip,
        isp=node_isp(node) or None,
        reason=report.reason,
        evidence=report.evidence,
        quarantine_hours=quarantine_hours,
    )
    storage.incr_metric("ip_pool_ban_events_total", {"service": report.service, "reason": report.reason})
    return BanReportResponse(
        ban_id=record.ban_id,
        node_id=node.name,
        exit_ip=record.exit_ip,
        isp=record.isp,
        quarantine_until=record.quarantined_until,
        idempotent=idempotent,
    )


@app.get("/v1/feedback/bans", response_model=List[BanRecord])
def list_bans_history(
    service: Optional[str] = Query(None, description="Filter by service"),
    limit: int = Query(100, ge=1, le=1000, description="Max rows (newest first)")
):
    """封禁历史列表 (P1): 含已过期与已解除的记录, 供归因与审计。"""
    return storage.list_bans(service=service, limit=limit)


@app.post("/v1/feedback/ban/release")
def release_ban_quarantine(
    service: str = Query(..., description="Service scope of the quarantine"),
    node_id: str = Query(..., description="Node to release from quarantine")
):
    """人工解除隔离 (P1): 置 released_at, 该节点立即回到 avoid_banned 候选集。"""
    released = storage.release_quarantine(service, node_id)
    return {"success": released > 0, "service": service, "node_id": node_id, "released": released}


# --- Universal Account Allocation & Distribution Endpoints ---

@app.post("/v1/allocation/batch", response_model=BatchAllocationResponse)
def batch_allocate_accounts(request: BatchAllocationRequest):
    """
    Universal batch account allocation interface (e.g. grok2api 2,100+ accounts).
    Evenly distributes accounts across pool exits (e.g. 100 WARP IPv6 nodes) with minimal variance.
    Generates both direct SOCKS5 and transparent gateway URLs.
    """
    try:
        return allocator.batch_allocate(request)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # F6 (二轮 review): 异常串可能含内部路径/SQL, 只落日志, 响应体给类型摘要
        print(f"[-] batch_allocate failed: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="batch allocation failed (internal error, see api logs)")


@app.post("/v1/allocation/acquire", response_model=AccountAllocationItem)
def acquire_account_allocation(
    service: str = Query(..., description="Service identifier, e.g. grok2api"),
    account_id: str = Query(..., description="Unique account ID or email"),
    egress_preference: Optional[str] = Query("warp_ipv6", description="Preferred egress type"),
    region: Optional[str] = Query(None, description="Optional region preference"),
    region_strict: bool = Query(False, description="Treat region as a hard constraint: never relax it to another region (empty candidate set / 503 instead). 需求 §4.2 硬边界"),
    tier: Optional[str] = Query(None, description="Optional tier filter: premium or standard"),
    tier_policy: Optional[str] = Query(None, description="Optional tier policy: standard_first, premium_first, premium_only, standard_only"),
    force_rebalance: bool = Query(False, description="Force reallocating to a different node"),
    # 2026-11-19 review P2: 与 /v1/acquire 的 force_rebind 同语义, 两个端点互认别名
    force_rebind: bool = Query(False, description="Alias of force_rebalance (accepted on both endpoints to kill the param drift)"),
    avoid_banned: bool = Query(False, description="Exclude quarantined (banned) nodes and their /24 for this service"),
    avoid_cidr: Optional[str] = Query(None, description="Comma-separated CIDR list to exclude, e.g. 23.230.200.0/24"),
    avoid_same_isp: bool = Query(False, description="Cross-ISP selection: skip nodes whose isp=<name> tag matches a banned node's ISP"),
    exclusive: bool = Query(False, description="One-account-one-node hard mutex: skip nodes already allocated to another account of this service; exhausted pool answers 503 instead of sharing a node")
):
    """
    On-demand allocation for a single account with load-aware capacity distribution.
    Ban-aware (2026-11-14): avoid_banned/avoid_cidr/avoid_same_isp are opt-in hard
    filters — exhausted candidates answer 503, constraints are never relaxed.
    region_strict=true stops the ladder from relaxing `region` to another region
    (same semantics as POST /v1/acquire; an exhausted region answers 503).
    """
    # F7 (二轮 review): 非法 CIDR 原先在 build_ban_excluded_networks 里被静默跳过,
    # 消费方拼错网段时得到"过滤生效"的假象。入口先校验, 拼错 -> 422 明确失败。
    avoid_cidrs: Optional[List[str]] = None
    if avoid_cidr:
        import ipaddress as _ipaddress
        avoid_cidrs = []
        for c in avoid_cidr.split(","):
            c = c.strip()
            if not c:
                continue
            try:
                _ipaddress.ip_network(c, strict=False)
            except ValueError:
                raise HTTPException(status_code=422, detail=f"invalid CIDR in avoid_cidr: '{c}'")
            avoid_cidrs.append(c)

    try:
        result = allocator.acquire_single(
            service=service,
            account_id=account_id,
            egress_preference=egress_preference,
            region=region,
            tier=tier,
            tier_policy=tier_policy,
            force_rebalance=force_rebalance or force_rebind,
            avoid_banned=avoid_banned,
            avoid_cidrs=avoid_cidrs,
            avoid_same_isp=avoid_same_isp,
            exclusive=exclusive,
            region_strict=region_strict
        )
        # 防汛告警 (需求 §5.5): 该 service 的 active quarantine 打穿健康池 30% 时
        # 响应加 warning (语义照常返回, 告警走 metrics + 此字段)。
        # 仅 ban-aware 调用才付这两条查询的成本 (review F4) —— 无 avoid 参数的
        # 热路径不查。计数按节点去重 (同节点多 reason 的多行只算一个节点, review F7)。
        if avoid_banned or avoid_same_isp or avoid_cidr:
            quarantined_count = len({q.node_name for q in storage.get_active_quarantines(service=service)})
            healthy_count = storage.get_stats().healthy_nodes
            if healthy_count and quarantined_count > healthy_count * QUARANTINE_FLOOD_RATIO:
                result.warning = "quarantine_flood"
        return result
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # F6 (二轮 review): RuntimeError(str) 可能携带内部细节, 响应体给稳定摘要
        print(f"[-] allocation acquire failed: {type(e).__name__}: {e}")
        detail = "no available node for this constraint set (see api logs for cause)"
        if "exclusive exhausted" in str(e):
            detail = ("one-account-one-node mutex exhausted: all eligible nodes "
                      "are held by other accounts of this service")
        # 结构化错误码 (2026-11-21 iter3): detail 字符串匹配是隐式契约——hub 侧
        # 优先读 headers 的 X-Exhausted-Reason, 文案匹配保留为旧客户端兜底。
        headers = None
        if "exclusive exhausted" in str(e):
            headers = {"X-Exhausted-Reason": "mutex_exhausted"}
        raise HTTPException(status_code=503, detail=detail, headers=headers)


@app.get("/v1/allocation/status", response_model=AllocationOverviewResponse)
def get_allocation_overview():
    """
    Statistical overview of account distributions and node capacity utilization.
    """
    return storage.get_allocations_overview()


@app.get("/v1/allocation/status/account")
def get_account_allocation_status(
    service: str = Query(..., description="Service identifier"),
    account: str = Query(..., description="Account ID (X-Account-Id namespace)")
):
    """
    Per-account binding & quarantine status (需求 §4.3, 程序用):
    绑定节点 + exit_ip/isp + 该 service 的 active quarantine 清单 + 池健康快照。
    (既有池级 overview 保留在 /v1/allocation/status, 此路由为 per-account 载荷。)
    """
    alloc = storage.get_allocation(service, account)
    node = None
    bound_at = None
    if alloc and alloc.get("node_name"):
        node = storage.get_node(alloc["node_name"])
        bound_at = alloc.get("allocated_at")
    if node is None and storage.find_allocation_by_account(account) is None:
        # lease 兜底与 _resolve_ban_node 同守卫 (review F2): 该 account 在别的
        # service 有 allocation 时, lease 的 service 归属不明, 宁可 node_id=None
        # 也不跨 service 泄漏绑定。
        lease = storage.get_lease(account)
        if lease:
            node = storage.get_node(lease.node_name)
            bound_at = lease.assigned_at
    quarantined = [
        {
            "node_id": q.node_name,
            "exit_ip": q.exit_ip,
            "reason": q.reason,
            "until": q.quarantined_until,
        }
        for q in storage.get_active_quarantines(service=service)
    ]
    stats = storage.get_stats()
    return {
        "service": service,
        "account": account,
        "node_id": node.name if node else None,
        "exit_ip": node.exit_ip if node else None,
        "isp": (node_isp(node) or None) if node else None,
        "bound_at": bound_at,
        "quarantined": quarantined,
        "pool_health": {
            "healthy": stats.healthy_nodes,
            "quarantined": len(quarantined),
            "total": stats.total_nodes,
        },
    }


@app.get("/v1/allocation/accounts")
def list_service_allocations(
    service: str = Query(..., description="Service identifier"),
    # 2026-11-19 review P2: 大服务 (cpa 1564 / grok2api 2100+) 全量返回无界
    limit: int = Query(0, ge=0, le=20000, description="Cap the number of returned allocations (0 = no cap)")
):
    """
    List all active account allocations for a given service.
    """
    allocs = storage.list_allocations_by_service(service)
    return allocs[:limit] if limit else allocs


@app.post("/v1/allocation/release")
def release_account_allocation(
    service: str = Query(..., description="Service identifier"),
    account_id: str = Query(..., description="Account ID to release")
):
    """
    Release or unbind an account allocation.
    """
    success = storage.delete_allocation(service, account_id)
    return {"success": success, "service": service, "account_id": account_id}



