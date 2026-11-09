<div align="center">

# TetherPool

**跨国多节点双栈出口池、工业级四态断路器与本地统一正向代理网关**

[![License](https://img.shields.io/badge/license-MIT-green.svg?style=flat-square)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB.svg?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![EasyTier](https://img.shields.io/badge/Mesh-EasyTier%20P2P-FF6600.svg?style=flat-square)](https://github.com/EasyTier/EasyTier)
[![Prometheus](https://img.shields.io/badge/Metrics-Prometheus-E6522C.svg?style=flat-square&logo=prometheus&logoColor=white)](https://prometheus.io/)
[![SQLite](https://img.shields.io/badge/Database-SQLite%20WAL-003B57.svg?style=flat-square&logo=sqlite&logoColor=white)](https://sqlite.org/)
[![Tests](https://img.shields.io/badge/tests-106%20passed-brightgreen.svg?style=flat-square)]()

[English Documentation (README.md)](README.md) · [简体中文](./README.zh-CN.md)

</div>

---

## 双模架构全景

```
                      +-------------------------------------------------+
                      |              下游 AI 反代与爬虫业务              |
                      |   (grok2api, Gemini Web, CPA, Playwright, curl) |
                      +-------------------+-----------------------------+
                                          |
                +-------------------------+-------------------------+
                | 标准 HTTP / CONNECT 代理                           | REST 控制面 API
                v                                                   v
  +---------------------------+                       +---------------------------+
  | 统一正向网关 (:9101)      |                       | 控制面中枢 (:9100)        |
  | - HTTP CONNECT 隧道       |                       | - 现代化内嵌 Web 控制台   |
  | - 原生 HTTP 透传          |                       | - 云原生 /metrics 导出器  |
  | - Header 驱动动态路由     |                       | - 四态断路器审计与复位    |
  |   (X-Account-Id, Region)  |                       | - 异步并发探活引擎        |
  +-------------+-------------+                       +-------------+-------------+
                |                                                   |
                +-------------------------+-------------------------+
                                          |
                                          v
                      +---------------------------------------+
                      |            核心调度与存储引擎         |
                      | - SQLite WAL 模式与运行时状态保护     |
                      | - 四态断路器 (Closed/Degraded/        |
                      |   Dead/Half-Open) + 租约平滑自愈      |
                      | - 确定性 CRC32 账号粘性路由器         |
                      +-------------------+-------------------+
                                          |
                      EasyTier P2P 加密网格 (10.144.144.0/24)
            +--------------------+--------------------+--------------------+
            |                    |                    |                    |
            v                    v                    v                    v
    [ 日本东京 AWS ]      [ 华为云新加坡 ]      [ 荷兰 LeaseWeb ]     [ 印度 Azure ]
     10.144.144.3        10.144.144.13        10.144.144.12        10.144.144.5
    +--------------+    +--------------+    +--------------+    +--------------+
    | 25 WARP IPv6 |    | 25 WARP IPv6 |    | 25 WARP IPv6 |    | 25 WARP IPv6 |
    | 1 原生 IPv4  |    | 1 原生 IPv4  |    | 1 原生 IPv4  |    | 1 原生 IPv4  |
    | 1 原生 IPv6  |    |              |    | 1 原生 IPv6  |    |              |
    +--------------+    +--------------+    +--------------+    +--------------+
```

### 核心亮点
1. **108 个独立公网出口矩阵**：
   - **100 个 Cloudflare WARP IPv6 出口**：东京 (loc=JP)、新加坡 (loc=SG)、荷兰 (loc=NL) 和印度 (loc=IN) 4 大边缘节点各 25 个独立出口。
   - **8 个机房原生公网出口**：6 个原生 IPv4 与 2 个原生 IPv6，覆盖东京、新加坡、荷兰、印度、香港与美西。
2. **优质/劣质代理双分池架构 (Quality Tiering)**：
   - **优质代理池 (`premium`)**：准入标准高，专纳自建原生机房出口、低延迟 (<350ms)、纯净独享 IP、零熔断的优质出口。
   - **劣质/标准代理池 (`standard`)**：容纳第三方聚合代理、WARP 大批量出口、高延迟或通用出口。
   - **默认普通优先 (`standard_first`) 策略**：默认请求优先消耗普通/劣质池，保护高价值自建优质 IP；账号注册或高权重调用传入 `tier=premium` 享受优质通道。
   - **多因子质量评估引擎 (`core/quality.py`)**：结合节点来源 (自建+40/WARP+25/第三方+15)、RTT 延迟 (最高+30)、健康度与公网 Egress 认证，预留 Scamalytics/IPQS 风控情报扣分。
3. **零公网端口暴露（Mesh 纵深防御）**：
   - 所有代理端口（`:10801~10905`）100% 仅监听在 EasyTier 网格内网 IP（`10.144.144.x`），外网零端口暴露。
4. **控制面与统一正向网关双模**：
   - **控制面 (`:9100`)**：提供 REST 租约调度、现代 Web 控制台 (`/dashboard`)、Prometheus 指标 (`/metrics`) 与断路器治理。
   - **统一正向代理网关 (`:9101`)**：基于 `asyncio` 构建的本地透明代理，支持通过 HTTP 请求头（`X-Account-Id`、`X-Region`、`X-Egress-Type`、`Proxy-Authorization`）无感路由出站。
5. **工业级四态断路器与自愈重签**：
   - 节点经历 `HEALTHY`（闭合）-> `DEGRADED`（单次探测抖动）-> `DEAD`（连续熔断并进入指数退避冷却）-> `HALF_OPEN`（试探金丝雀探活）。
   - 节点熔断时，其名下绑定的账号租约自动秒级平滑重签（Re-home）至同区域健康的备用节点。
6. **云原生低基数可观测性**：
   - 标准 Prometheus OpenMetrics 导出器，严格按区域、协议、状态聚合，拒绝在指标 Label 中暴露动态 IP 或 URL。

---

## 快速上手

### 1. 环境准备
```bash
git clone https://github.com/dengyie/tether-pool.git
cd tether-pool

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 运行交互式验证 Demo
```bash
.venv/bin/python3 demo.py
```
10 秒内完整跑通全池拓扑校验、双栈实时探活、断路器熔断自愈演练与 `:9101` 正向网关转发验证。

### 3. 执行自动化测试 (106 项全绿)
```bash
.venv/bin/pytest -v
```

### 4. 启动后台守护服务
```bash
./start.sh start
# 查看运行状态
./start.sh status
```
- 控制台界面：`http://127.0.0.1:9100/dashboard`
- Prometheus 指标：`http://127.0.0.1:9100/metrics`

> **网络暴露防护：** 两个监听器默认仅绑定 `127.0.0.1`。设置 `IP_POOL_API_TOKEN=<密钥>` 后，
> **除 `/health` 外的所有端点**都要求携带密钥（`Authorization: Bearer <密钥>`、`?token=<密钥>`，
> 或 dashboard 握手写入的 HttpOnly Cookie）。浏览器只需访问一次 `/dashboard?token=<密钥>`，
> 密钥即被换成 Cookie，页面后续同源请求自动携带；`/metrics` 同样受保护（Prometheus 抓取请用
> `Authorization: Bearer <密钥>`）。
>
> **节点清单对齐：** 启动时，数据库中已存在但 `config/nodes.yaml` 已移除的节点会被标记为 `dead`
> （保留账号绑定，交由自愈重签迁移）。**破坏性删除需显式开启**：`IP_POOL_PRUNE_MISSING_NODES=1`，
> 且配置文件至少含 10 个节点（避免配置被截断时误清空注册表）。
- 统一正向网关：`http://127.0.0.1:9101`

---

## 正向网关调用示例 (`:9101`)

### 1. HTTPS CONNECT 隧道 (指定东京出口)
```bash
curl -x http://127.0.0.1:9101 \
  -H "X-Account-Id: alice@example.com" \
  -H "X-Region: tokyo" \
  https://cloudflare.com/cdn-cgi/trace
```

### 2. 机房原生 IPv4 出口 (Basic 认证透传)
```bash
curl -x http://bob:secret@127.0.0.1:9101 \
  -H "X-Egress-Type: native_ipv4" \
  https://api.ipify.org
```

---

## REST API 接口定义 (`:9100`)

| 动词 | 端点 | 功能说明 |
|---|---|---|
| `GET` | `/health` | 服务健康检查 |
| `GET` | `/dashboard` | 现代毛玻璃 UI 运维监控控制台 |
| `GET` | `/metrics` | 云原生 Prometheus OpenMetrics 导出端点 |
| `GET` | `/v1/stats` | 节点总数、健康度分类与区域分布统计 |
| `GET` | `/v1/nodes` | 查询节点列表（支持 `region`, `egress_type`, `status` 过滤） |
| `GET` | `/v1/acquire` | 只读查询已有的粘性绑定（无绑定时返回 `404`，不会创建） |
| `POST` | `/v1/acquire` | 创建/返回粘性绑定（`account_id`, `region`, `egress_type`, `service`） |
| `POST` | `/v1/probe` | 触发全池异步并发健康探测 |
| `GET` | `/v1/circuit/events` | 查询断路器状态变迁与自愈重签审计日志 |
| `POST` | `/v1/circuit/reset` | 运维人员手动复位节点断路器至 `healthy` |
| `POST` | `/v1/circuit/rehome` | 针对特定故障节点手动触发租约重签迁移 |
| `GET` | `/v1/export/cpa` | 导出 CPA 格式代理列表 |
| `GET` | `/v1/export/grok2api` | 导出 grok2api 所需健康节点格式 |

---

## 项目目录结构

```
ip-pool/
├── config/
│   └── nodes.yaml              # 出站节点拓扑配置
├── core/
│   ├── circuit.py              # 四态断路器状态机与退避冷却算法
│   ├── clash_importer.py       # Clash / 3X-UI 节点导入与 Mihomo SOCKS5 桥接
│   ├── metrics.py              # 低基数 Prometheus 监控导出器
│   ├── models.py               # Pydantic 领域模型与数据契约
│   ├── allocator.py            # 容量感知的批量/单账号分配引擎
│   ├── prober.py               # 异步双栈探活器
│   ├── router.py               # 确定性 CRC32 粘性路由与自愈重签
│   └── storage.py              # SQLite 持久化引擎 (WAL 模式)
├── gateway/
│   └── server.py               # 本地统一正向代理网关 (:9101)
├── api/
│   └── server.py               # FastAPI 控制面服务 (:9100)
├── web/
│   └── templates/
│       └── dashboard.html      # 现代化内嵌单文件 Web 控制台
├── adapters/
│   ├── cpa_adapter.py          # Claude/OpenAI Proxy (CPA) 导出适配器
│   ├── gemini_adapter.py       # Gemini Web 逆向出站适配器
│   ├── playwright_pool.py      # Python 爬虫与 Playwright 代理池 SDK
│   └── tetherpool_client.py    # 控制面零依赖 HTTP 客户端
├── tests/                      # 隔离沙箱自动化单元测试 (99 项全绿)
├── demo.py                     # 端到端交互式全功能演练脚本
├── requirements.txt            # Python 依赖清单
└── start.sh                    # 双进程后台守护启停脚本
```

---

## 开源许可证
MIT License.
