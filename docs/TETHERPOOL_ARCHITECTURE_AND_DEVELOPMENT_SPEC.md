# 🌐 TetherPool：面向注册机与接电池体系的企业级出站调度与中继桥接系统技术规范

> **版本**：v2.1-Enterprise  
> **状态**：规范落地 / 开发基准（Canonical Reference）  
> **核心定位**：服务于**自动化注册机（Batch Account Registration）**与**高并发接电池系统（Account Battery & Token Pools）**的企业级出站调度、防关联隔离与多级中继桥接基础设施。  
> **适用服务矩阵**：`grok2api`、`cpa-manager-plus`（Claude/OpenAI 代理池）、`gemini2api`、`CodeArt2api`、自动化注册脚本及所有依赖高纯净度、高粘性 IP 的 AI 逆向与爬虫流水线。

---

## 一、 系统定位与核心使命（Positioning & Mission）

### 1.1 与 3X-UI / 传统翻墙客户端的本质界限

TetherPool 的本质**绝不是**另一个 3X-UI，也不是给单人看网页、看 YouTube 使用的梯子客户端。

| 核心维度 | 3X-UI / 传统代理客户端 | **TetherPool 出站基础设施** |
| :--- | :--- | :--- |
| **服务对象** | 人工交互、桌面浏览器、看视频 | **自动化注册机（批量造号）、电池池（Session/Token 轮转）、无头爬虫** |
| **IP 使用模式** | 单人长期复用一个节点，追求极限单流带宽 | **高频轮替、一号一密、严格防关联（Anti-Sybil）、目标注册冷却期** |
| **会话持久性** | 人为手动切节点，或客户端被动分流 | **严格粘性（Sticky）：电池放电期间绝对禁止漂移，防止触发异地风控** |
| **网络可达性假定** | 假定客户端网络正常，直连目标代理即可 | **不可信网络环境：大量沙盒/容器无 IPv6、跨境直连丢包，必须多级桥接** |
| **故障耐受模式** | 节点掉线人工报修或重连 | **高损耗常态化：四态断路器秒级隔离、同域租约自愈、静默换电** |
| **衡量核心指标** | YouTube 4K 测速、Ping 延迟 | **注册成功率（%）、CF Turnstile 突破率、电池账号存活周期** |

### 1.2 系统核心使命
为注册机与接电池项目提供**「打得通（高可达桥接）、分得清（防关联冷冻）、粘得住（会话强保活）、换得快（故障毫秒自愈）」**的弹药级出站调度底座。

---

## 二、 核心应用场景与微观机制反推（Reverse Architecture）

```
                     ┌─────────────────────────────────────────────────────────┐
                     │            上游业务系统：造电池与用电池                   │
                     └────────────┬───────────────────────────────┬────────────┘
                                  │                               │
                [造电池] 注册机并发造号                           │ [用电池] 账号池放电轮换
                                  ▼                               ▼
       +──────────────────────────────────────+      +──────────────────────────────────────+
       │ 场景 A：注册机 (Registration Bots)   │      │ 场景 B：接电池 (Battery Account Pool)│
       │ - 一号一 IP 独占租约                  │      │ - 确定性 CRC32 账号哈希粘性           │
       │ - 目标站点 30~60min 注册冷却隔离     │      │ - 电池放电生命周期内 IP 绝对锁定      │
       │ - 业务纯净度分级 (WARP / 原生 / 家宽) │      │ - 节点熔断同域同 ASN 毫秒级重签      │
       +──────────────────┬───────────────────+      +──────────────────┬───────────────────+
                          │                                             │
                          └──────────────────────┬──────────────────────┘
                                                 ▼
                          +─────────────────────────────────────────────+
                          │    TetherPool 统一智能正向网关 (:9101)      │
                          │    - Header 驱动路由 (X-Account / X-Region) │
                          │    - 目标冷却感知 (Anti-Sybil Cooldown)     │
                          │    - 直连/中继自适应桥接级联决策             │
                          +──────────────────────┬──────────────────────+
                                                 │
                                                 ▼
                                     [真实网络中继与出口拓扑]
```

---

### 2.1 场景 A：注册机（批量造号、自动化养号）

**典型业务**：`grok2api` 批量造号脚本、Claude/OpenAI 注册机、邮箱与临时手机接收器。

#### 业务微观痛点与反推机制：
1. **反关联连坐（Anti-Sybil Risk）**：
   - 目标站点风控（Cloudflare Turnstile、Arkose Labs、DataDome）对同一 IP 短时间内的重复注册行为极其敏感。如果同一个 IP 在 10 分钟内发起多次注册，不仅会导致当前注册失败，甚至会导致该 IP 之前注册的一整批新号被“连坐全灭”。
   - **反向架构推导**：
     - **目标站点冷却锁（Target-Specific IP Cooldown）**：网关记录 `(ip, target_domain, registered_at)`。某 IP 一旦注册成功，自动对该目标域名进入至少 30~60 分钟冷却期，在此期间对同类注册任务隐藏，仅允许分配给非注册查询任务。
     - **一号一租约独占**：注册任务执行期间，该出口处于独占锁定状态，防止并发注册任务命中同一 IP。
2. **纯净度出口分级调度**：
   - 低风控任务（收取验证邮件、拉取静态资源）：调度低成本 **WARP IPv6**；
   - 苛刻风控（提交注册表单、过人机验证）：调度 **机房原生 IPv4 / 51x 优质自建出口**；
   - 区域限制风控（如限制大陆家宽）：调度 **🏠家宽住宅出口**。

---

### 2.2 场景 B：接电池项目（账号池轮转消耗、Token 换电站）

**典型业务**：`grok2api`（2,100+ 账号轮换）、`cpa-manager-plus`（Claude/OpenAI 代理池）、`gemini2api`。

#### 业务微观痛点与反推机制：
1. **什么是“接电池”与“换电”？**
   - 逆向 API 体系中，每个账号（Session Cookie、Refresh Token）就是一块**“电池”**。
   - 注册机负责**“造电池”**；中继 API 负责**“接电池并放电”**；当账号被限流（429）、额度耗尽或会话过期时，系统自动**“换下旧电池，换上新电池”**。
2. **异地登录封杀（Session IP Pinning）**：
   - 平台（如 X/Grok、OpenAI）对已登录的 Session 维持强烈的网络环境信任度。如果同一个会话在放电期间，底层 IP 在美国、日本、香港乱跳，平台风控会瞬间触发异地安全拦截并强制登出，整块电池直接作废。
   - **反向架构推导**：
     - **确定性 CRC32 强粘性路由**：只要 Account ID 不变，路由算法永远算出唯一的出口节点，终生锁定。**2026-11-12 升级为 weight 感知区间选择**：节点按 `weight`（nodes.yaml / PATCH API 可调，1–1000）瓜分 CRC32 哈希空间，同（账号, 候选集, 权重）三元组恒定映射同一节点；等权重下均匀分布（实测 20 万账号逐节点偏差 ±6%）。存量粘性由 allocation 表保证，与哈希公式解耦——调整权重只影响新绑定。适用全部 5 个选点（账号粘性 / 单节点 rehome / 域 rehome / 电池换绑 / 注册反女巫）。`resolve_relay_hop` 对 `direct_only` 节点零查询早退（省每连接 4 次 hub 扫描）。 同批根修：**residential opt-in 语义**——`residential` 标签的计量型 vendor 家宽只服务显式 `type=clash_proxy` 请求，无 egress_type 请求一律排除（健康/降级阶梯均生效；全池兜底保留，availability first）；**`POST /v1/probe?force=true`** 强制探活（跳过冷却 canary），start.sh 启动尾自动触发，重启后熔断债 46→2 节点 ~25s 清零；Mac 桥核由 `scripts/bridge_keeper.sh` 守护（崩溃 2s 重生，击杀测试通过；根因=脚本漏 +x），teleapi 139 节点独立故障域 `mac-bridge-core`。双端部署验证：Mac pytest 180 / 云端 165 全绿；云端实测 typed 粘性 ✓、匿名零家宽 ✓、30 账号 20 独立 WARP v6 出口 ≈ 均匀期望 21.5。 **二轮 review 根修（同日）**：`POST /v1/probe` 确定性化——排队等待 `_probe_lock`（不再撞锁静默丢弃）+ 15s 窗口合并去重（防未鉴权刷探活风暴）+ `queued_behind_running_sweep` 观测字段；start.sh 确认式重试（拿到 200 才算排队）。**`scripts/import_standalone.py` 入仓**（standalone 两阶段导入器：import 写 manifest/nodes.yaml/桥核配置并 mihomo -t 校验、realize 探活回写 realized_map/import_audit，fault_domain 自动传播），消除 /tmp 一次性脚本复现性缺口。keeper 指数退避（2s→60s 封顶，长稳自动重置）；`_weighted_deterministic_pick` 空列表显式 ValueError；L3 O(n²) 修正。二轮验证：Mac pytest 185 / 云端 168 全绿；日志实证 queued→executed(force=True) 全链路；合并/退避/击杀实测通过。
     - **同域自愈重签（Same-Region Rehoming）**：若绑定的底层节点物理宕机，四态断路器必须在**同区域、同机房网段**内优雅漂移到替补节点，严禁跨大区漂移。
3. **电池生命周期回传协同**：
   - 下游接电池服务（如 CPA / Grok2API）如果在调用中收到 401（封号）或 429（限流），应具备一键反向通知 TetherPool 的机制，标记当前出口的业务健康状态。

---

## 三、 架构支柱：不可信网络下的「多级 IP 桥接体系（Bridge & Relay Cascade）」

在真实生产中，注册机与接电池 Worker 常运行于：
- 没有公网 IPv6 的容器环境（如 Render、CloudStudio、K8s Pod）；
- 跨国骨干网抖动严重、UDP/WireGuard 握手被阻断的大陆机房或办公机；
- NAT444 隔离无法直连 EasyTier P2P 的机器。

**假定“直连能通”是系统不可靠的根本根源。TetherPool 必须建立完备的桥接级联架构。**

### 3.1 三级链路级联模型

```
[Level 1: 业务接入端]
  注册机 Worker / 接电池实例
       │
       ▼ (本地或同内网 HTTP / SOCKS5 握手)
[Level 2: TetherPool 网关 & 中继跳板层]
  TetherPool 本地网关 (:9101)  <───>  香港/东京 Azure/AWS 高速中继跳板
       │
       ├─────────────────────────────────────────┐
       ▼ (直连可达)                               ▼ (直连不可达 / 跨境恶劣)
[Level 3A: 直连落地]                      [Level 3B: 桥接落地]
  - 100x WARP IPv6 (EasyTier)             - 经由香港/日本 SOCKS5 链式代理中继
  - 8x 原生机房 IP                          - 经由 Clash Verge SOCKS5 动态桥接
  - 51x 优质自建出口                        - 跨协议栈转换 (IPv4 In -> IPv6 Out)
```

### 3.2 智能级联回退状态机（Relay Fallback State Machine）

网关在接收到连接请求时，执行如下级联自适应决策：

```
                    发起请求 (CONNECT host:port)
                                │
                                ▼
                   探测目标出口首选连通方式
                                │
         ┌──────────────────────┴──────────────────────┐
         ▼                                             ▼
   [尝试 1: 直连通路]                            [尝试 2: 中继通路]
Direct SOCKS5 / Mesh P2P                    通过指定的香港/东京跳板中继
         │                                             │
         ├────────── 成功 ─────────┐                   ├────────── 成功 ─────────┐
         ▼                         │                   ▼                         │
   建立直接双向管道                 │             建立中继级联双向管道            │
         │                         │                   │                         │
         ▼ 失败 (超时/网络不可达)   │                   ▼ 失败 (跳板超时)          │
         │                         │                   │                         │
         ▼                         │                   ▼                         │
   自动降级触发中继 ───────────────┘             四态断路器将该链路标记为 DEAD     │
                                                       │                         │
                                                       ▼                         │
                                                 触发同域租约自愈重签 ────────────┘
```

---

## 四、 核心数据模型与契约扩展规范

### 4.1 节点扩展：支持中继与桥接配置 (`core/models.py`)

```python
class RelayMode(str, Enum):
    DIRECT_ONLY = "direct_only"       # 仅允许直连
    RELAY_PREFERRED = "relay_pref"    # 优先中继（适合跨境恶劣节点）
    RELAY_FALLBACK = "relay_fallback" # 直连优先，失败自动跳板中继 (默认)

class ProxyNode(BaseModel):
    name: str
    host: str
    port: int
    protocol: str = "socks5h"
    region: str
    egress_type: str                   # warp_ipv6, native_ipv4, clash_proxy
    tags: List[str] = Field(default_factory=list)
    weight: int = 100
    
    # 桥接与中继扩展
    relay_mode: str = RelayMode.RELAY_FALLBACK.value
    relay_hop_node: Optional[str] = None  # 指定跳板节点名，如 "clash-004-vless-azure-reality"
    
    # 状态与断路器
    status: str = "unprobed"
    latency_ms: Optional[float] = None
    exit_ip: Optional[str] = None
    circuit_state: str = "unprobed"
    consecutive_failures: int = 0
```

### 4.2 注册机专属冷却契约

```python
class RegistrationLease(BaseModel):
    account_id: str
    target_service: str                # 例如 "grok_reg", "openai_signup"
    node_name: str
    proxy_url: str
    exit_ip: Optional[str] = None
    region: str
    cooldown_seconds: int = 1800       # 注册成功后锁定冷却 30 分钟
    leased_at: str
    expires_at: str
```

---

## 五、 四阶段实施路线图与可依规程（Implementation Steps）

```
+─────────────────────────────────────────────────────────────────────────────+
|                                实施路线图                                    |
+─────────────────────────────────────────────────────────────────────────────+
| [阶段一] 中继桥接与链路级联引擎 (Relay Bridge Cascade)                        |
|   1.1 扩展 ProxyNode 支持 relay_mode 与 relay_hop_node                      |
|   1.2 在 gateway/server.py 实现 SOCKS5 链式代理与直连失败毫秒级回退          |
|   1.3 编写 test_relay_cascade.py 单元测试                                   |
+─────────────────────────────────────────────────────────────────────────────+
| [阶段二] 注册机防关联与 IP 冷却调度引擎 (Anti-Sybil Cooldown Engine)         |
|   2.1 SQLite 新增 ip_cooldowns 记录表 (带过期索引)                          |
|   2.2 实现 Router.acquire_for_registration() 冷却过滤调度算法               |
|   2.3 暴露 GET /v1/acquire_for_registration 与 POST /v1/release_cooldown   |
+─────────────────────────────────────────────────────────────────────────────+
| [阶段三] 针对性靶向业务探活 (Target-Specific Probing)                        |
|   3.1 prober.py 支持靶向服务校验 (CF Turnstile / X / OpenAI Auth)            |
|   3.2 区分「网络通」与「业务被封」软失效状态                                |
+─────────────────────────────────────────────────────────────────────────────+
| [阶段四] 接电池生命周期与状态回传闭环 (Battery Lifecycle Hooks)              |
|   4.1 实现 POST /v1/battery/event (限流/封号上报与主动换电)                 |
|   4.2 完善 grok2api 与 cpa-manager-plus 适配 SDK                             |
+─────────────────────────────────────────────────────────────────────────────+
```

---

### 阶段一：中继桥接与链路级联引擎（Relay Bridge Cascade）

#### 1.1 目标
彻底解决“直连连不到”问题。当注册机 Worker 无法直连 EasyTier 或海外节点时，网关透明经由高速中继节点（如香港 Azure、东京 AWS）进行 SOCKS5 链式中转，外部业务调用无感。

#### 1.2 落地规范步骤：
1. **模型层**：在 `core/models.py` 补充 `RelayMode` 枚举与 `relay_hop_node` 字段。
2. **网关核心转发层 (`gateway/server.py`)**：
   - 改造 `ForwardGateway.tunnel_connect()`：
     - 若节点 `relay_mode == "relay_fallback"`，首选直连尝试握手；
     - 若直连在 1.5 秒内超时或抛出 `ConnectionRefused/OSError`，捕获异常并不直接向客户端报错；
     - 立即提取备用跳板中继节点（如 `127.0.0.1:11004` 香港 Azure 或 `127.0.0.1:11002` 东京 AWS），建立两跳 SOCKS5 级联隧道：`Client -> Gateway -> Relay Node -> Target Exit -> Internet`。
3. **自动化测试覆盖**：
   - 在 `tests/test_gateway.py` 中增加中继级联模拟测试，模拟直连超时自动回退到 Relay 成功的用例。

---

### 阶段二：注册机防关联与 IP 冷却调度引擎（Anti-Sybil Cooldown Engine）

#### 2.1 目标
确保批量注册机不会把同一个 IP 在短时间内重复分给相同任务，从根源规避 Cloudflare / Arkose 批量封号。

#### 2.2 落地规范步骤：
1. **持久层 (`core/storage.py`)**：
   - 新建 `ip_cooldowns` 表：
     ```sql
     CREATE TABLE IF NOT EXISTS ip_cooldowns (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         exit_ip TEXT NOT NULL,
         target_service TEXT NOT NULL,
         node_name TEXT NOT NULL,
         cooled_until TEXT NOT NULL,
         created_at TEXT NOT NULL
     );
     CREATE INDEX IF NOT EXISTS idx_cooldown_lookup ON ip_cooldowns(exit_ip, target_service, cooled_until);
     ```
2. **路由调度层 (`core/router.py`)**：
   - 增加 `acquire_for_registration(account_id, target_service, region, cooldown_seconds)`：
     - 查询当前所有健康节点；
     - 剔除其 `exit_ip` 存在于 `ip_cooldowns` 且 `cooled_until > NOW()` 的节点；
     - 在候选纯净池中基于哈希或轮询分配，并在分配成功后插入冷却锁定记录。
3. **控制面 API (`api/server.py`)**：
   - 暴露 `GET /v1/acquire_for_registration`：注册机取号专享；
   - 暴露 `POST /v1/release_cooldown`：注册失败时提前解冻。

---

### 阶段三：针对性靶向业务探活（Target-Specific Probing）

#### 3.1 目标
传统探活只能检测“节点能不能 Ping 通外网”。但注册机业务经常遭遇“IP 能连外网，但已经被 X.com 或 OpenAI Cloudflare 拉黑（403 Forbidden / 503 Challenge Loop）”。系统必须具备靶向探活能力。

#### 3.2 落地规范步骤：
1. **靶向探测器 (`core/prober.py`)**：
   - 新增针对性探测目标列表：
     - `GENERAL`: `https://cloudflare.com/cdn-cgi/trace`
     - `GROK_SIGNUP`: `https://x.com/i/flow/signup`（检测是否出现 Cloudflare WAF 阻断）
     - `OPENAI_AUTH`: `https://auth.openai.com`
2. **断路器协同**：
   - 若通用探测成功，但业务探测返回 403/Blocked，节点标记为 `DEGRADED (Business Blocked)`，不影响通用爬虫，但从注册候选池中熔断隔离。

---

### 阶段四：接电池生命周期与状态协同（Battery Lifecycle Hooks）

#### 4.1 目标
打通与下游 `grok2api`、`cpa-manager-plus` 的双向协同，下游发现账号限流封号时回传事件，驱动 TetherPool 自动评估节点质量与自愈换电。

#### 4.2 落地规范步骤：
1. **事件上报端点 (`api/server.py`)**：
   - `POST /v1/battery/event`：
     - 参数：`account_id`, `node_name`, `event_type` (`rate_limited_429`, `banned_401`, `turnstile_failed`)
   - 存储层记录异常指标，连续发生异常时，TetherPool 主动重置该账号的 Lease，自动无缝重签分配新节点（换电完成）。

---

## 六、 接口规范与数据契约详述（API Specifications）

### 6.1 注册机专用出口租约分配
- **端点**：`GET /v1/acquire_for_registration`
- **请求参数**：
  | 参数 | 类型 | 必填 | 默认值 | 说明 |
  |---|---|---|---|---|
  | `account_id` | string | 是 | - | 待注册账号唯一标识（如邮箱） |
  | `target_service` | string | 是 | - | 目标业务标识（如 `grok_reg`, `openai_reg`） |
  | `cooldown` | int | 否 | `1800` | 注册成功后 IP 冷却锁定秒数（默认 30 分钟） |
  | `egress_type` | string | 否 | `clash_proxy` | 倾向出口类型（`clash_proxy`, `native_ipv4`） |
  | `region` | string | 否 | null | 目标地域偏好（如 `tokyo`, `us-west`） |
- **响应示例**：
  ```json
  {
    "account_id": "new_grok_user_001@mango.com",
    "target_service": "grok_reg",
    "node_name": "clash-002-vless-india-reality",
    "proxy_url": "socks5h://127.0.0.1:11002",
    "exit_ip": "20.198.2.112",
    "region": "india",
    "cooldown_seconds": 1800,
    "cooled_until": "2026-11-04T02:35:00",
    "relay_mode": "relay_fallback",
    "relay_hop": "127.0.0.1:11004"
  }
  ```

### 6.2 注册失败 / 提前解除冷却
- **端点**：`POST /v1/release_cooldown`
- **请求 Payload**：
  ```json
  {
    "exit_ip": "20.198.2.112",
    "target_service": "grok_reg",
    "reason": "registration_failed_before_submit"
  }
  ```

### 6.3 接电池异常与换电上报
- **端点**：`POST /v1/battery/event`
- **请求 Payload**：
  ```json
  {
    "account_id": "demo_user@mango.com",
    "node_name": "clash-002-vless-india-reality",
    "event_type": "rate_limited_429",
    "force_rebind": true
  }
  ```
- **响应**：直接返回新重签换电成功的 `LeaseInfo`。

---

## 七、 运维部署与验证 SOP

1. **查看服务双模运行状态**：
   ```bash
   ./start.sh status
   ```
2. **端到端测试直连与中继连通性**：
   ```bash
   # 测试直连香港
   curl -x http://127.0.0.1:9101 --proxy-header "X-Region: hongkong" https://cloudflare.com/cdn-cgi/trace
   # 测试注册机冷却模拟
   curl -s "http://127.0.0.1:9100/v1/acquire_for_registration?account_id=test@m.com&target_service=grok_reg" | jq .
   ```
3. **单元测试回归检验**：
   ```bash
   .venv/bin/pytest -v
   ```

---

> **结语**：本技术规范为 TetherPool 作为注册机与电池基础设施的长期权威指南，所有后续架构演进与代码变更均需严格依此规范执行。
