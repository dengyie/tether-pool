# 🌐 TetherPool (`tetherpool`)

> **跨国多节点双栈出口池、工业级四态断路器与本地统一正向代理网关**  
> 统一编排纳管 **108 个独立公网出口**（100 个 Cloudflare WARP IPv6 + 8 个机房原生公网 IPv4/IPv6），基于 EasyTier 全网虚拟网格内网安全互通。采用「控制面 (`:9100`) + 统一正向网关 (`:9101`)」双模架构，具备四态断路器故障转移与租约平滑自愈、Prometheus 云原生低基数监控及现代嵌入式可视化控制台。

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg)](https://fastapi.tiangolo.com/)
[![Tests](https://img.shields.io/badge/tests-84%20passed-brightgreen.svg)]()
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

[English Documentation (README.md)](README.md)

---

## 📌 双模架构全景

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
2. **零公网端口暴露（Mesh 纵深防御）**：
   - 所有代理端口（`:10801~10905`）100% 仅监听在 EasyTier 网格内网 IP（`10.144.144.x`），外网零端口暴露。
3. **控制面与统一正向网关双模**：
   - **控制面 (`:9100`)**：提供 REST 租约调度、现代 Web 控制台 (`/dashboard`)、Prometheus 指标 (`/metrics`) 与断路器治理。
   - **统一正向代理网关 (`:9101`)**：基于 `asyncio` 构建的本地透明代理，支持通过 HTTP 请求头（`X-Account-Id`、`X-Region`、`X-Egress-Type`、`Proxy-Authorization`）无感路由出站。
4. **工业级四态断路器与自愈重签**：
   - 节点经历 `HEALTHY`（闭合）-> `DEGRADED`（单次探测抖动）-> `DEAD`（连续熔断并进入指数退避冷却）-> `HALF_OPEN`（试探金丝雀探活）。
   - 节点熔断时，其名下绑定的账号租约自动秒级平滑重签（Re-home）至同区域健康的备用节点。
5. **云原生低基数可观测性**：
   - 标准 Prometheus OpenMetrics 导出器，严格按区域、协议、状态聚合，拒绝在指标 Label 中暴露动态 IP 或 URL。

---

## 🚀 快速上手

### 1. 环境准备
```bash
git clone https://github.com/dengyie/tetherpool.git
cd tetherpool

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 运行交互式验证 Demo
```bash
.venv/bin/python3 demo.py
```
10 秒内完整跑通全池拓扑校验、双栈实时探活、断路器熔断自愈演练与 `:9101` 正向网关转发验证。

### 3. 执行自动化测试 (84 项全绿)
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

> **网络暴露防护：** 两个监听器默认仅绑定 `127.0.0.1`，且控制面本身不含用户鉴权。
> 如需对外暴露，必须同时设置 `HOST=0.0.0.0` 与 `IP_POOL_API_TOKEN=<密钥>`；
> 设置后所有 `/v1/*` 接口都要求携带 `Authorization: Bearer <密钥>`。
- 统一正向网关：`http://127.0.0.1:9101`

---

## 🧭 正向网关调用示例 (`:9101`)

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

## 📡 REST API 接口定义 (`:9100`)

| 动词 | 端点 | 功能说明 |
|---|---|---|
| `GET` | `/health` | 服务健康检查 |
| `GET` | `/dashboard` | 现代毛玻璃 UI 运维监控控制台 |
| `GET` | `/metrics` | 云原生 Prometheus OpenMetrics 导出端点 |
| `GET` | `/v1/stats` | 节点总数、健康度分类与区域分布统计 |
| `GET` | `/v1/nodes` | 查询节点列表（支持 `region`, `egress_type`, `status` 过滤） |
| `GET` | `/v1/acquire` | 获取账号绑定的粘性节点租约 |
| `POST` | `/v1/probe` | 触发全池异步并发健康探测 |
| `GET` | `/v1/circuit/events` | 查询断路器状态变迁与自愈重签审计日志 |
| `POST` | `/v1/circuit/reset` | 运维人员手动复位节点断路器至 `healthy` |
| `POST` | `/v1/circuit/rehome` | 针对特定故障节点手动触发租约重签迁移 |
| `GET` | `/v1/export/cpa` | 导出 CPA 格式代理列表 |
| `GET` | `/v1/export/grok2api` | 导出 grok2api 所需健康节点格式 |

---

## 📂 项目目录结构

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
├── tests/                      # 隔离沙箱自动化单元测试 (84 项全绿)
├── demo.py                     # 端到端交互式全功能演练脚本
├── requirements.txt            # Python 依赖清单
└── start.sh                    # 双进程后台守护启停脚本
```

---

## 📄 开源许可证
MIT License.
