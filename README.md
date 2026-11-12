<div align="center">

> ⚠️ **本仓库已归档 (ARCHIVED)** — 该项目已迁移并继续开发于 **[dengyie/tether-pool](https://github.com/dengyie/tether-pool)**（生产部署与最新代码均以彼处为准；生产机历史快照见其 `prod-legacy` 分支）。本仓库保留仅为历史参考, 请勿在此提交 PR/Issue。
>
> ---
# TetherPool

**Unified Global Dual-Stack IP Pool, Circuit Breaker & Forward Proxy Gateway**

[![License](https://img.shields.io/badge/license-MIT-green.svg?style=flat-square)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB.svg?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![EasyTier](https://img.shields.io/badge/Mesh-EasyTier%20P2P-FF6600.svg?style=flat-square)](https://github.com/EasyTier/EasyTier)
[![Prometheus](https://img.shields.io/badge/Metrics-Prometheus-E6522C.svg?style=flat-square&logo=prometheus&logoColor=white)](https://prometheus.io/)
[![SQLite](https://img.shields.io/badge/Database-SQLite%20WAL-003B57.svg?style=flat-square&logo=sqlite&logoColor=white)](https://sqlite.org/)
[![Tests](https://img.shields.io/badge/tests-106%20passed-brightgreen.svg?style=flat-square)]()

[English](./README.md) · [简体中文](./README.zh-CN.md)

</div>

---

## Dual-Mode Architecture Overview

```
                      +-------------------------------------------------+
                      |              Upstream AI & Scrapers             |
                      |   (grok2api, Gemini Web, CPA, Playwright, curl) |
                      +-------------------+-----------------------------+
                                          |
                +-------------------------+-------------------------+
                | Standard HTTP / CONNECT                           | REST Control API
                v                                                   v
  +---------------------------+                       +---------------------------+
  | Forward Gateway (:9101)   |                       | Control Plane (:9100)     |
  | - HTTP CONNECT Tunneling  |                       | - Modern Web Dashboard    |
  | - Native HTTP Forwarding  |                       | - Cloud-Native /metrics   |
  | - Header-Driven Routing   |                       | - Circuit Breaker REST    |
  |   (X-Account-Id, Region)  |                       | - Asynchronous Prober     |
  +-------------+-------------+                       +-------------+-------------+
                |                                                   |
                +-------------------------+-------------------------+
                                          |
                                          v
                      +---------------------------------------+
                      |        Core Engine & Storage          |
                      | - SQLite WAL Mode & State Preserved   |
                      | - 4-State Breaker (Closed/Degraded/   |
                      |   Dead/Half-Open) + Self-Healing      |
                      | - Deterministic CRC32 Sticky Router   |
                      +-------------------+-------------------+
                                          |
                      EasyTier P2P Encrypted Mesh (10.144.144.0/24)
            +--------------------+--------------------+--------------------+
            |                    |                    |                    |
            v                    v                    v                    v
    [ aws-tokyo ]        [ huawei-sg ]        [ mesh-nl ]        [ mesh-india ]
     10.144.144.3        10.144.144.13        10.144.144.12        10.144.144.5
    +--------------+    +--------------+    +--------------+    +--------------+
    | 25 WARP IPv6 |    | 25 WARP IPv6 |    | 25 WARP IPv6 |    | 25 WARP IPv6 |
    | 1 Native IPv4|    | 1 Native IPv4|    | 1 Native IPv4|    | 1 Native IPv4|
    | 1 Native IPv6|    |              |    | 1 Native IPv6|    |              |
    +--------------+    +--------------+    +--------------+    +--------------+
```

### Key Highlights
- **108 Independent Exits**:
  - **100 Cloudflare WARP IPv6 Exits**: Distributed in batches of 25 across Tokyo, Singapore, Netherlands, and India, with distinct IPv6 addresses and Cloudflare edge locations (`loc=JP`, `loc=SG`, `loc=NL`, `loc=IN`).
  - **8 Native Datacenter Exits**: 6 IPv4 + 2 IPv6 spanning Tokyo (AWS), Singapore (Huawei), Netherlands (LeaseWeb), India (Azure), Hong Kong (Azure), and US-West (GCP).
- **Zero Public Port Exposure**: All proxy inbounds listen strictly on private EasyTier mesh IPs (`10.144.144.x:10801~10905`), leaving zero public listening ports open on VPS nodes.
- **Dual-Mode Serving**:
  - **Control Plane (`:9100`)**: REST API for programmatic leasing, visual dashboard (`/dashboard`), Prometheus OpenMetrics (`/metrics`), and circuit breaker administration.
  - **Unified Forward Proxy Gateway (`:9101`)**: High-performance local forward proxy supporting HTTP CONNECT (HTTPS) and standard HTTP GET/POST, with header-directed routing via `X-Account-Id`, `X-Region`, `X-Egress-Type`, and `Proxy-Authorization`.
- **Quality Pool Separation (优质/劣质代理分池)**:
  - **优质代理池 (`premium`)**: 准入限制高，专纳自建原生节点、低延迟 (<350ms)、纯净独享 IP、零熔断的优质出口。
  - **普通代理池 (`standard`)**: 容纳第三方聚合代理、WARP 大批量出口、高延迟或通用出口。
  - **默认策略为「普通优先 (`standard_first`)」**：普通无标记请求优先消耗标准/劣质池，保护高成本自建优质 IP；敏感业务（注册、高权重 AI 账号）显式传入 `tier=premium` 享受优质通道。
  - **轻量内置质量评分模型 (`core/quality.py`)**：结合节点来源 (Provenance)、实时 RTT 延迟、断路器历史与公网真实 Egress，支持外部风控情报 API (Scamalytics / IPQS) 扣分集成。
- **4-State Circuit Breaker & Automatic Self-Healing**:
  - Nodes transit through `HEALTHY` (Closed) -> `DEGRADED` (1 failure) -> `DEAD` (2+ failures, Exponential backoff cooldown) -> `HALF_OPEN` (Trial canary probe).
  - When a node trips to `DEAD`, all attached active account leases automatically migrate to healthy backup nodes within the same region and egress protocol.
- **Strict Low-Cardinality Observability**: Cloud-native Prometheus exporter (`/metrics`) aggregating by region, egress type, and health status without label bloat.

---

## Quick Start

### 1. Installation
```bash
git clone https://github.com/dengyie/tether-pool.git
cd tether-pool

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the Interactive Showcase Demo
```bash
.venv/bin/python3 demo.py
```
Verifies pool topology, multi-region probing, circuit breaker self-healing, forward proxy gateway tunneling, and adapter exports in ~10 seconds.

### 3. Run Unit Tests (106 Tests Passing)
```bash
.venv/bin/pytest -v
```

### 4. Start the Production Daemons
```bash
./start.sh start
# Check status
./start.sh status
```
- Web Dashboard: `http://127.0.0.1:9100/dashboard`
- Prometheus Metrics: `http://127.0.0.1:9100/metrics`

> **Network exposure:** both listeners bind to `127.0.0.1` by default. When
> `IP_POOL_API_TOKEN=<secret>` is set, **every endpoint except `/health`** requires the token
> (`Authorization: Bearer <secret>`, `?token=<secret>`, or the HttpOnly cookie set by the
> dashboard handshake). Browsers navigate to `/dashboard?token=<secret>` once; the token is
> exchanged for a cookie so the page's same-origin fetches keep working. `/metrics` is
> protected too (use `Authorization: Bearer <secret>` for Prometheus scraping).
>
> **Node inventory reconciliation:** on startup, nodes present in the database but missing
> from `config/nodes.yaml` are marked `dead` by default (bindings are preserved so the
> self-healing re-home path can move them). Destructive deletion requires explicit opt-in:
> `IP_POOL_PRUNE_MISSING_NODES=1` (and at least 10 configured nodes, so a truncated config
> cannot wipe the registry).
- Forward Proxy Gateway: `http://127.0.0.1:9101`

---

## Gateway Usage Examples (`:9101`)

### 1. HTTPS CONNECT Tunneling via Tokyo WARP
```bash
curl -x http://127.0.0.1:9101 \
  -H "X-Account-Id: alice@example.com" \
  -H "X-Region: tokyo" \
  https://cloudflare.com/cdn-cgi/trace
```

### 2. Native IPv4 Egress with Basic Auth
```bash
curl -x http://bob:secret@127.0.0.1:9101 \
  -H "X-Egress-Type: native_ipv4" \
  https://api.ipify.org
```

---

## REST API Reference (`:9100`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Service health status |
| `GET` | `/dashboard` | Modern glassmorphism web console |
| `GET` | `/metrics` | Cloud-native Prometheus OpenMetrics |
| `GET` | `/v1/stats` | Real-time node count, health breakdown, and distribution |
| `GET` | `/v1/nodes` | List all nodes with filters (`region`, `egress_type`, `status`) |
| `GET` | `/v1/acquire` | Read the existing sticky binding (`404` when none — never mutates state) |
| `POST` | `/v1/acquire` | Create/return the sticky binding (`account_id`, `region`, `egress_type`, `service`) |
| `POST` | `/v1/probe` | Trigger concurrent background probe across all nodes |
| `GET` | `/v1/circuit/events` | Audit log of circuit transitions and lease migrations |
| `POST` | `/v1/circuit/reset` | Operator manual reset of a node to `healthy` |
| `POST` | `/v1/circuit/rehome` | Manually re-home leases for a degraded/dead node |
| `GET` | `/v1/export/cpa` | Export proxies in CPA format |
| `GET` | `/v1/export/grok2api` | Export healthy nodes for `grok2api` |

---

## Repository Structure

```
ip-pool/
├── config/
│   └── nodes.yaml              # Static inventory of pool proxy endpoints
├── core/
│   ├── circuit.py              # 4-State Circuit Breaker & cooldown algorithms
│   ├── clash_importer.py       # Clash / 3X-UI proxy importer & Mihomo SOCKS5 bridge
│   ├── metrics.py              # Low-cardinality Prometheus exporter
│   ├── models.py               # Pydantic data schemas
│   ├── allocator.py            # Capacity-aware batch/single account allocation engine
│   ├── prober.py               # Asynchronous dual-stack health prober
│   ├── router.py               # Sticky hash & self-healing lease re-homer
│   └── storage.py              # SQLite persistence engine (WAL mode)
├── gateway/
│   └── server.py               # High-performance forward proxy gateway (:9101)
├── api/
│   └── server.py               # FastAPI REST control plane (:9100)
├── web/
│   └── templates/
│       └── dashboard.html      # Responsive web monitoring & operation console
├── adapters/
│   ├── cpa_adapter.py          # Claude/OpenAI Proxy configuration exporter
│   ├── gemini_adapter.py       # Gemini Web reverse routing adapter
│   ├── playwright_pool.py      # Python crawler & Playwright pool SDK
│   └── tetherpool_client.py    # Zero-dependency HTTP client for the control plane
├── tests/                      # Pytest isolated unit test suite (99 passing tests)
├── demo.py                     # Interactive end-to-end verification showcase
├── requirements.txt            # Python dependencies
└── start.sh                    # Dual-process background daemon manager
```

---

## License
MIT License.
