# 全局双栈 IP 池系统（`ip-pool`）企业级演进规范与能力补齐开发文档
> **版本**：v2.0.0-RFC  
> **状态**：已定稿（实施中）  
> **适用范围**：`/Users/mango/project/ip-pool`  
> **作者**：Antigravity Agent  
> **日期**：2026-11-03

---

## 1. 系统愿景与架构定位

### 1.1 核心愿景
`ip-pool` 是面向大规模 AI 逆向反代（Grok Web 2,100+ 账号、Gemini Web、Claude / OpenAI CPA）、高性能分布式爬虫集群（Playwright）构建的**私有骨干网格级超级代理中枢（Mesh-backed Super-Proxy Engine）**。
系统统一纳管 **108 个独立全球公网出口**（100 个 Cloudflare WARP IPv6 + 8 个机房原生公网 IP），利用 **EasyTier P2P 加密虚拟局域网**（`10.144.144.0/24`）建立零公网暴露的高安全链路，对外提供**“智能调度控制面 + 统一正向网关数据面”双模能力**。

### 1.2 架构模式双模定义
根据架构决策，系统同时支持两种上游接入模式：
1. **控制面模式（Control Plane Mode，默认）**：
   - 监听端口：`http://127.0.0.1:9100`（FastAPI）
   - 交互协议：HTTP REST API (`/v1/acquire`, `/v1/stats`, `/v1/nodes` 等)
   - 适用场景：下游服务（如 `grok2api` 批量账号同步、自动化运维脚本）本地具备代理客户端能力，主动向 IP 池请求租约，并自行直连分发的 EasyTier 节点。
2. **统一正向网关模式（Data Plane Gateway Mode，新补齐能力）**：
   - 监听端口：`http://127.0.0.1:9101`（异步高性能正向代理网关）
   - 交互协议：标准 HTTP/HTTPS (CONNECT) 与 SOCKS5
   - 特性能力：下游客户端（如爬虫、无头浏览器、无法调用 REST 的通用组件）无需理解 108 个底层节点，只需统一配置代理 `http://127.0.0.1:9101`，并通过请求头（如 `X-Account-Id: user@example.com`、`X-Region: tokyo`、`X-Egress-Type: warp_ipv6`）或代理认证用户名，由网关在本地内存中自动查验/分配粘性租约并透明分流出站。

---

## 2. 现状与企业级生产标准差距分析（Gap Analysis）

通过参考业内成熟项目（`scrapoxy`、`glider`、`rota`、`jhao104/proxy_pool`），梳理出当前系统的差距与补齐清单：

| 模块能力 | 当前版本 (v1.0) 现状 | 目标版本 (v2.0) 规范标准 | 补齐优先级 |
| :--- | :--- | :--- | :--- |
| **断路器状态机** | 仅基于 SQLite 的简单 `fail_count >= 2 -> dead`，无冷却期概念。 | **工业级四态断路器（Closed / Degraded / Open / Half-Open）**，带指数退避 + Jitter 冷却窗口，半开试探机制。 | **P0 (关键可靠性)** |
| **租约自动自愈** | 节点熔断时，已有租约不会主动迁移，需等待业务下次主动请求重签。 | **租约后台平滑主动自愈（Lease Re-homing）**。看门狗熔断节点时，自动将受影响租约平滑重签至同区域最佳备用节点，下游无感。 | **P0 (关键可用性)** |
| **统一正向代理** | 无统一数据面，必须调 REST API 分配节点后自行拨号。 | **本地统一正向网关服务（:9101）**，支持 HTTP CONNECT / SOCKS5，通过 Header/Auth 实现透明粘性路由。 | **P0 (核心架构升级)** |
| **Web 可视化控制台** | 仅提供 Swagger `/docs`，无业务视图和交互面板。 | **内嵌现代化轻量 Dashboard（:9100/dashboard）**。全球 6 大区域 108 节点状态卡片、实时 RTT 柱状图、出口 IP 拓扑、一键批量并发探活与搜索过滤。 | **P1 (核心可操作性)** |
| **Prometheus 监控** | 无监控暴露端点。 | **低基数规范 `/metrics` 端点**，导出节点状态分布、各区域探测延迟直方图、断路器迁移计数、活跃租约量。 | **P1 (核心可观测性)** |
| **上游适配器矩阵** | 仅有 `grok2api_adapter.py`，且缺少增量回退机制。 | 补全 **`cpa_adapter.py`**（CPA 多凭证导出）、**`gemini_adapter.py`**（Gemini Web 专用轮换分片）、**`playwright_pool.py`**（无头爬虫客户端）。 | **P1 (生态扩展)** |

---

## 3. 分层系统架构设计

```
                            +-------------------------------------------------------------+
                            |                       下游应用层                              |
                            |  - grok2api (2,100+ 账号)   - Gemini Web 逆向                 |
                            |  - CPA (Claude/OpenAI Proxy) - Playwright 爬虫集群          |
                            +------------------------------+------------------------------+
                                                           |
                          +--------------------------------+--------------------------------+
                          | [模式 A] 控制面 REST API (:9100) | [模式 B] 统一正向网关 (:9101)  |
                          |   - /v1/acquire (租约获取)       |   - HTTP CONNECT / SOCKS5 监听  |
                          |   - /dashboard (可视化控制台)    |   - Header: X-Account-Id 路由   |
                          |   - /metrics (Prometheus 指标)   |   - 自动在网关内部转发出站       |
                          +--------------------------------+--------------------------------+
                                                           |
                                                           v
                            +-------------------------------------------------------------+
                            |                 核心调度与高可用引擎 (Core Engine)            |
                            |   - 四态断路器状态机 (Closed / Degraded / Open / Half-Open)  |
                            |   - 指数退避冷却与抖动算法 (Backoff + Jitter)                 |
                            |   - 区域感知租约自愈调度器 (Region-Aware Lease Re-homer)     |
                            |   - CRC32 / 一致性哈希约束路由器 (Router)                   |
                            |   - 异步并发双栈探活看门狗 (Prober Watchdog)                 |
                            |   - SQLite WAL 原子持久化与备份 (Storage)                   |
                            +------------------------------+------------------------------+
                                                           |
                                                           | SOCKS5h over EasyTier
                                                           v
                            +-------------------------------------------------------------+
                            |               EasyTier 加密 P2P 网格 (10.144.144.0/24)       |
                            |   (所有端口 10801~10905 仅监听在内网网格 IP，外部零暴露)     |
                            +-------+---------------+---------------+-------------+-------+
                                    |               |               |             |
                                    v               v               v             v
                             [ aws-tokyo ]   [ huawei-sg ]     [ mesh-nl ]  [ mesh-india ]
                              10.144.144.3   10.144.144.13   10.144.144.12   10.144.144.5
                             (25 WARP IPv6)  (25 WARP IPv6)  (25 WARP IPv6) (25 WARP IPv6)
                             (1 Native IPv4) (1 Native IPv4) (1 Native IPv4)(1 Native IPv4)
                             (1 Native IPv6)                 (1 Native IPv6)
                                    |               |               |             |
                                    +---------------+---------------+-------------+
                                                    | (WireGuard 隧道)
                                                    v
                                    +-------------------------------+
                                    |   Cloudflare 全球边缘网络     |
                                    | (loc=JP, SG, NL, IN 等独立出口)|
                                    +-------------------------------+
```

---

## 4. 核心功能与技术设计细则

### 4.1 四态断路器与自愈引擎（Circuit Breaker）

#### 4.1.1 节点状态机定义
- **CLOSED (Healthy)**：节点最近探测全部正常，全权参与生产流量和租约分配。
- **DEGRADED (亚健康)**：单次探测超时或偶发错误（`fail_count = 1`），降低路由权重，不分配新账号租约，保留现有租约。
- **OPEN (熔断/Dead)**：连续失败 $\ge 2$ 次，立即从候选池中剔除，进入隔离观察。
  - **冷却机制**：计算冷却解冻时间：
    $$T_{\text{cooldown}} = \min(60 \times 2^{\text{fail\_count}-2}, 1800) + \text{jitter}(0, 15) \text{ 秒}$$
  - **租约自愈触发**：立即启动异步自愈任务，将绑定在该节点上的所有活跃租约，自动迁移至同区域中健康、负载最低的备用节点。
- **HALF-OPEN (半开探测)**：冷却时间耗尽后，允许探活看门狗进行一次单发探测。若成功，重置 `fail_count = 0` 并转为 `CLOSED` 重入候选池；若失败，重新转为 `OPEN` 并翻倍退避时间。

#### 4.1.2 租约自愈算法（Lease Re-homing）
1. 监听断路器事件：当某节点从非 OPEN 变为 OPEN 时，触发 `rehome_leases_for_node(broken_node_name)`。
2. 查找该节点名下的全部账号租约集合 $L$。
3. 针对每个租约：
   - 优先在**同区域（Same Region）**且**同出站协议（Same Egress Type）**的 CLOSED 节点中，使用 CRC32 一致性哈希选择新的目标节点；
   - 若同区域无健康节点，降级为跨区域全局健康节点；
   - 原子更新数据库 `leases` 表，并向 `circuit_events` 表写入迁移审计记录：`{account_id, from_node, to_node, reason="circuit_breaker_opened"}`。

---

### 4.2 本地统一正向代理网关（Unified Forward Proxy Gateway `:9101`）

#### 4.2.1 协议规范与透明解析
- 采用高性能异步网络框架（`asyncio` 结合 `tornado` 或轻量原生 Stream Relay 实现）：
  - 监听地址：`127.0.0.1:9101`
  - 支持 **HTTP/1.1 CONNECT 隧道**（用于 HTTPS）及普通 HTTP GET/POST 转发。
- **动态路由解析逻辑**：
  1. 拦截客户端 HTTP 请求头：
     - `X-Account-Id`（或 `Proxy-Authorization: Basic <base64(account_id:password)>`）
     - `X-Region`（可选偏好：`tokyo`、`singapore`、`netherlands`、`india`、`hongkong`、`us-west`）
     - `X-Egress-Type`（可选偏好：`warp_ipv6`、`native_ipv4`、`native_ipv6`）
  2. 调用路由模块获取或分配该 `account_id` 的当前有效健康租约，得到目标 EasyTier 内网节点（例如 `10.144.144.3:10885`）。
  3. 网关与该内网节点建立 SOCKS5h 握手，随后透明桥接客户端数据流与远端代理出站数据流。
  4. 剥离内部路由专有请求头（`X-Account-Id`, `X-Region`, `X-Egress-Type`），避免信息泄露给目标网站。

---

### 4.3 内嵌现代化 Web 可视化控制台（Dashboard UI）

#### 4.3.1 页面技术栈
- 架构：单文件内嵌轻量 SPA，由 FastAPI 静态路由托管在 `/dashboard`。
- 前端技术：HTML5 + TailwindCSS (CDN) + Alpine.js / 原生 ES6 + Lucide Icons。
- 零依赖：无额外 Node.js、Webpack、Vite 构建步骤，修改即生效。

#### 4.3.2 界面功能规划
1. **顶部状态看板（Header KPI Bar）**：
   - 节点总量卡片（Total / 108+）、实时健康率卡片（Healthy %）、平均探测 RTT、当前活跃账号租约总数。
   - 控制按钮组：“⚡ 全池并发批量探活（带进度动画与防抖）”、“🔄 刷新数据”、“📥 导出数据”。
2. **多维筛选与搜索栏（Filter & Search Tool）**：
   - 关键字搜索（节点名、Host、真实出口 IP）；
   - 区域单选/多选过滤（Tokyo, Singapore, Netherlands, India, HK, US-West）；
   - 协议类型过滤（WARP IPv6, Native IPv4, Native IPv6）；
   - 状态过滤（全部、健康、降级、熔断）。
3. **全球区域节点拓扑矩阵（Regional Node Matrix）**：
   - 6 大区域分组折叠/展开；
   - 节点卡片：
     - 状态指示灯（🟢 Healthy / 🟡 Degraded / 🔴 Dead）；
     - 节点名称、内网地址端口；
     - 探测 RTT 延迟彩色指示条（<1000ms 绿，1000~3000ms 黄，>3000ms 橙）；
     - 真实公网出口 IP（脱敏展示）与 Cloudflare Edge 标志（`loc=JP` 等）；
     - 绑定租约数量。
4. **在线测试与交互抽屉（Interactive Drawer）**：
   - “账号租约模拟器”：输入 `account_id` 实时计算匹配节点，并一键测试代理出站连通性；
   - “节点操作”：手动触发单节点即时探测、手动熔断/解熔断开关。

---

### 4.4 云原生 Prometheus 监控指标规范（`/metrics`）

为防止时序数据库基数（Cardinality）爆炸，严格禁止使用节点 IP 作为 Label。指标设计如下：

| 指标名称 | 类型 | 标签 (Labels) | 含义与用途 |
| :--- | :--- | :--- | :--- |
| `ip_pool_node_count` | Gauge | `region`, `egress_type`, `status` | 各区域不同状态的节点数量分布 |
| `ip_pool_probe_duration_seconds` | Histogram | `region`, `egress_type` | 探测 RTT 延迟直方图（分桶：0.5, 1, 2, 5, 10, 15s） |
| `ip_pool_probes_total` | Counter | `region`, `status` (`success`/`failure`) | 探活请求总次数计数 |
| `ip_pool_leases_active` | Gauge | `region`, `egress_type` | 当前数据库中生效的粘性租约总数 |
| `ip_pool_circuit_transitions_total` | Counter | `region`, `from_state`, `to_state` | 断路器状态变迁计数（熔断告警核心指标） |
| `ip_pool_gateway_requests_total` | Counter | `region`, `status_code` | 统一正向网关 (:9101) 转发的请求总数 |

---

### 4.5 上游适配器矩阵（Adapters Matrix）

1. **`adapters/grok2api_adapter.py`（增强版）**：
   - 增强：在当前均分离散 2,103 个账号至 100 个 WARP IPv6 出口的基础上，增加增量模式（只同步新增账号，保留已有映射）。
   - 保持安全底座：更新 SQLite 业务库前自动执行 `VACUUM INTO '<db>.bak'` 原生原子备份。
2. **`adapters/cpa_adapter.py`（新编写）**：
   - 功能：读取当前健康的原生与 WARP 节点，生成 Claude Proxy / OpenAI CPA 专属的多凭据 `proxies.yaml` 配置，支持基于 credential 维度的固定分配。
3. **`adapters/gemini_adapter.py`（新编写）**：
   - 功能：面向 Gemini Web 逆向反代，提供特定国家偏好（如 US-West、Tokyo、Singapore）的出口分配策略与会话保活辅助。
4. **`adapters/playwright_pool.py`（新编写）**：
   - 功能：提供纯 Python 异步 SDK，一行代码为 Playwright / httpx / aiohttp 注入动态轮换或粘性代理配置。

---

## 5. 实施里程碑计划与文件变更规范

| 阶段 | 任务目标 | 核心变更文件 | 验收标准 |
| :--- | :--- | :--- | :--- |
| **Phase 1** | **四态断路器与租约平滑自愈引擎** | `core/models.py`<br>`core/storage.py`<br>`core/prober.py`<br>`core/router.py` | 模拟单节点失败触发 Open 状态，断路器自动将租约重签至同区域备用节点，恢复后半开试探重入。单测覆盖率 100%。 |
| **Phase 2** | **内嵌现代化 Web 可视化控制台** | `api/server.py`<br>`web/static/...`<br>`web/templates/index.html` | 浏览器访问 `http://127.0.0.1:9100/dashboard` 正常加载 108 个节点矩阵，支持多维过滤与一键批量并发探活。 |
| **Phase 3** | **Prometheus 监控指标导出器** | `api/server.py`<br>`core/metrics.py` | `curl -s http://127.0.0.1:9100/metrics` 输出合法云原生 OpenMetrics 格式，涵盖节点、延迟与断路器事件。 |
| **Phase 4** | **本地统一正向代理网关 (:9101)** | `gateway/relay.py`<br>`start.sh` | 启动网关并在 `:9101` 监听，`curl -x http://127.0.0.1:9101 -H "X-Account-Id: test1" https://cloudflare.com/cdn-cgi/trace` 成功透明出站。 |
| **Phase 5** | **补全上游反代适配器生态** | `adapters/cpa_adapter.py`<br>`adapters/gemini_adapter.py`<br>`adapters/playwright_pool.py` | 运行脚本生成合规配置与客户端调用测试通过。 |
| **Phase 6** | **系统集成验收与运维文档更新** | `demo.py`<br>`tests/`<br>`start.sh`<br>Obsidian 笔记库同步 | 全套单测绿色通过；`demo.py` 演示新功能；更新知识库手册。 |
