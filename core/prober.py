"""
core/prober.py - Asynchronous dual-stack health prober with Circuit Breaker Integration
"""

import asyncio
import datetime
import time
import httpx
from typing import List, Optional, Any
from core.models import ProxyNode, ProbeResult, EgressType, HealthStatus
from core.storage import Storage
from core.circuit import CircuitBreaker

DEFAULT_TIMEOUT = 12.0
PROBE_URL_V4 = "https://api.ipify.org"
PROBE_URL_CF_TRACE = "https://cloudflare.com/cdn-cgi/trace"


def parse_cf_trace(text: str) -> dict:
    data = {}
    for line in text.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            data[k.strip()] = v.strip()
    return data


def select_probe_target(node: ProxyNode) -> tuple[str, bool]:
    """Probe target ladder for a node. Returns (target_url, is_cf_trace).

    节点级 `probe_url` 覆盖优先 (2026-11-18 mesh 宿主机收编): CN 家宽/CN 机房
    native 出口下 GFW 会拦 Cloudflare 段 (api.ipify.org / cdn-cgi/trace 的
    TLS)，节点需自带可直达目标 (如 https://ipinfo.io/ip 纯 IP 文本)。
    格式按 URL 特征识别而非 egress_type —— CF trace 走 k=v 解析并附带 loc=
    geo 信号，其余按纯文本 IP 处理。空 probe_url 回落默认阶梯。
    """
    if node.probe_url:
        return node.probe_url, "cdn-cgi/trace" in node.probe_url
    if node.egress_type == EgressType.NATIVE_IPV4.value:
        return PROBE_URL_V4, False
    return PROBE_URL_CF_TRACE, True


def enforce_exit_contract(node: ProxyNode, exit_ip: str):
    """出口契约校验 (与探活目标格式正交): warp_ipv6 节点必须探得 IPv6。

    review 根修: 原 v6 校验只在 CF-trace 分支内, 节点级 probe_url 指向
    纯文本目标时会静默绕过, v4 出口的 warp 节点被误判 healthy。"""
    if node.egress_type == EgressType.WARP_IPV6.value and ":" not in (exit_ip or ""):
        raise ValueError(f"Expected IPv6 egress for warp node, got: {exit_ip}")


class Prober:
    def __init__(self, storage: Storage, router: Optional[Any] = None, timeout: float = DEFAULT_TIMEOUT):
        self.storage = storage
        self.router = router
        self.timeout = timeout

    async def probe_node(self, node: ProxyNode, retries: int = 1, force: bool = False) -> ProbeResult:
        # Admin-disabled nodes are never probed (force override: explicit
        # operator intent). Returns without touching storage — no fail_count
        # bump, no circuit event, no probe_history row.
        if not force and node.status == HealthStatus.DISABLED.value:
            return ProbeResult(
                node_name=node.name,
                success=False,
                latency_ms=0.0,
                error="admin_disabled_probe_skipped"
            )
        # Check circuit breaker cooldown if node is DEAD
        is_canary = (not force) and node.status == HealthStatus.HALF_OPEN.value
        if not force and node.status == HealthStatus.DEAD.value:
            if not CircuitBreaker.is_cooldown_expired(node):
                # Cooldown still active, skip probing to respect isolation
                return ProbeResult(
                    node_name=node.name,
                    success=False,
                    latency_ms=0.0,
                    error="circuit_open_in_cooldown"
                )
            else:
                # Cooldown expired, mark as HALF_OPEN for trial canary probe.
                # A canary is a single-trial probe by definition (the circuit
                # reopens on failure); burning a full retry (~timeout+0.5s
                # more per canary per sweep) adds no information.
                is_canary = True
                node.status = HealthStatus.HALF_OPEN.value
                self.storage.update_node_status(node.name, HealthStatus.HALF_OPEN.value)
                self.storage.record_circuit_event(
                    node_name=node.name,
                    from_state=HealthStatus.DEAD.value,
                    to_state=HealthStatus.HALF_OPEN.value,
                    reason="cooldown_expired_canary_probe"
                )
        elif not force and node.status == HealthStatus.DEGRADED.value \
                and node.next_probe_allowed_at:
            # mass-guard 拦截产物 (2026-11-17): DEGRADED + 未到期退避锚点 =
            # "本轮别再探" (guard 说这次别踢 DEAD, 但退避照走)。观察期 DEGRADED
            # 的 next_probe_allowed_at 为空, 不受影响, rise=2 语义不变。
            try:
                if datetime.datetime.now() < datetime.datetime.fromisoformat(node.next_probe_allowed_at):
                    return ProbeResult(
                        node_name=node.name,
                        success=False,
                        latency_ms=0.0,
                        error="degraded_backoff_in_cooldown"
                    )
            except ValueError:
                pass  # 锚点解析失败按无锚点处理, 照常探测

        proxy_url = f"socks5h://{node.host}:{node.port}"

        # Choose primary probe URL (node-level override -> egress-type ladder)
        target_url, is_cf_trace = select_probe_target(node)

        for attempt in range(retries + 1):
            start_t = time.perf_counter()
            try:
                async def _req():
                    async with httpx.AsyncClient(proxy=proxy_url, timeout=self.timeout) as client:
                        return await client.get(target_url)

                resp = await asyncio.wait_for(_req(), timeout=self.timeout + 1.0)
                latency = round((time.perf_counter() - start_t) * 1000, 1)

                if resp.status_code == 200:
                    text = resp.text.strip()
                    if is_cf_trace:
                        parsed = parse_cf_trace(text)
                        exit_ip = parsed.get("ip", "")
                        if not exit_ip:
                            raise ValueError(f"Expected valid IP in trace, got: {exit_ip}")
                    else:
                        exit_ip = text
                    # warp 出口契约与探活目标格式正交 (review 根修)
                    enforce_exit_contract(node, exit_ip)

                    res = ProbeResult(
                        node_name=node.name,
                        success=True,
                        exit_ip=exit_ip,
                        latency_ms=latency,
                        # CF trace 自带 loc= 出口国家码 (区域漂移纠偏的免费 geo 信号)
                        country_code=(parsed.get("loc") or None) if is_cf_trace else None
                    )
                    # sqlite 写移出事件环 (二轮 review #2): sweep 并发下每次同步
                    # 写库都是一次全事件环停顿。
                    _, event = await asyncio.to_thread(self.storage.update_probe_result, res)
                    return res
                else:
                    raise RuntimeError(f"HTTP status {resp.status_code}")

            except Exception as e:
                latency = round((time.perf_counter() - start_t) * 1000, 1)
                err_msg = str(e).strip()
                if not err_msg or isinstance(e, asyncio.TimeoutError):
                    err_msg = f"Timeout (>{self.timeout}s)"

                if attempt < retries and not is_canary:
                    await asyncio.sleep(0.5)
                    continue

                res = ProbeResult(
                    node_name=node.name,
                    success=False,
                    latency_ms=latency,
                    error=err_msg[:120]
                )
                _, event = await asyncio.to_thread(self.storage.update_probe_result, res)

                # Automatic Self-Healing: If circuit tripped OPEN (DEAD), rehome attached leases
                if event and event.to_state == HealthStatus.DEAD.value and self.router:
                    try:
                        # rehome 含 BINDING_WRITE_LOCK 多表事务, 同样移出事件环
                        rehome_res = await asyncio.to_thread(
                            self.router.rehome_leases_for_node, node.name
                        )
                        if rehome_res.get("rehomed_count", 0) > 0:
                            print(f"[Self-Healing] Node {node.name} tripped OPEN. Rehomed {rehome_res['rehomed_count']} leases.")
                    except Exception as rehome_err:
                        print(f"[-] Error rehoming leases for {node.name}: {rehome_err}")

                return res

    async def probe_all(self, concurrency: int = 25, force: bool = False,
                        nodes: Optional[List[ProxyNode]] = None) -> List[ProbeResult]:
        if nodes is None:
            # 全表读 (3600 行) 移出事件环 (2026-11-15 review #5: sqlite 连接的
            # lock 等待可达 10s, 不能阻塞调用方事件环)
            nodes = await asyncio.to_thread(self.storage.list_nodes)
            if not force:
                # admin-disabled 节点不进探活 (force 除外): 管理员意图优先,
                # 探活不碰 DISABLED, 不涨 fail_count, 不产生 circuit_event。
                nodes = [n for n in nodes if n.status != HealthStatus.DISABLED.value]
                # janitor 冻结层 (2026-11-15): 连续 DEAD >= 24h 已打 frozen tag,
                # 探活降频 —— 全量轮不再碰它们, 只由 janitor 周期性复核; 节点若
                # 真的恢复, 需人工 POST /v1/probe?force 或 revive, 防止 dead_since
                # 锚点被偶然探活抖动清掉、驱逐倒计时被打断。
                nodes = [n for n in nodes if "frozen" not in n.tags]
        sem = asyncio.Semaphore(concurrency)

        async def _worker(node: ProxyNode) -> ProbeResult:
            async with sem:
                try:
                    return await self.probe_node(node, retries=1, force=force)
                except Exception as exc:
                    return ProbeResult(
                        node_name=node.name,
                        success=False,
                        error=f"worker_exception: {exc}"[:120]
                    )

        tasks = [_worker(node) for node in nodes]
        results = await asyncio.gather(*tasks)
        return results

    async def probe_target_service(self, node: ProxyNode, target_url: str) -> ProbeResult:
        """
        Target-specific business probe (e.g. testing Cloudflare Turnstile, X signup, or OpenAI Auth).
        Differentiates between general network reachability and domain-level anti-bot blockages.
        """
        proxy_url = f"socks5h://{node.host}:{node.port}"
        start_t = time.perf_counter()
        try:
            async with httpx.AsyncClient(proxy=proxy_url, timeout=self.timeout, follow_redirects=True) as client:
                resp = await client.get(
                    target_url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
                    }
                )
                latency = round((time.perf_counter() - start_t) * 1000, 1)

                # Check for Cloudflare / Anti-Bot blockages
                if resp.status_code in (403, 503) and ("challenge-running" in resp.text or "Just a moment" in resp.text or "cf-mitigated" in resp.headers):
                    return ProbeResult(
                        node_name=node.name,
                        success=False,
                        latency_ms=latency,
                        error=f"Blocked by Cloudflare/WAF (HTTP {resp.status_code})"
                    )
                if resp.status_code >= 400 and resp.status_code not in (404, 401):
                    return ProbeResult(
                        node_name=node.name,
                        success=False,
                        latency_ms=latency,
                        error=f"Target returned HTTP {resp.status_code}"
                    )
                return ProbeResult(
                    node_name=node.name,
                    success=True,
                    exit_ip=node.exit_ip,
                    latency_ms=latency
                )
        except Exception as e:
            latency = round((time.perf_counter() - start_t) * 1000, 1)
            return ProbeResult(
                node_name=node.name,
                success=False,
                latency_ms=latency,
                error=str(e)[:120]
            )

