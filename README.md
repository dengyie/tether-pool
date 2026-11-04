# 🌐 TetherPool (`tetherpool`)

> **Unified Global Dual-Stack IP Pool, Circuit Breaker & Forward Proxy Gateway**  
> Enterprise-grade proxy pool orchestrating **108 independent exits** (100 Cloudflare WARP IPv6 + 8 Datacenter Native IPv4/IPv6) across 6 global regions over an encrypted EasyTier P2P mesh network. Featuring dual-mode operation (REST Control Plane on `:9100` + Unified Forward Proxy Gateway on `:9101`), 4-state circuit breaker with self-healing lease re-homing, and low-cardinality Prometheus observability.

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg)](https://fastapi.tiangolo.com/)
[![Tests](https://img.shields.io/badge/tests-83%20passed-brightgreen.svg)]()
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

[中文文档 (README.zh-CN.md)](README.zh-CN.md)

---

## 📌 Dual-Mode Architecture Overview

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
- **4-State Circuit Breaker & Automatic Self-Healing**:
  - Nodes transit through `HEALTHY` (Closed) -> `DEGRADED` (1 failure) -> `DEAD` (2+ failures, Exponential backoff cooldown) -> `HALF_OPEN` (Trial canary probe).
  - When a node trips to `DEAD`, all attached active account leases automatically migrate to healthy backup nodes within the same region and egress protocol.
- **Strict Low-Cardinality Observability**: Cloud-native Prometheus exporter (`/metrics`) aggregating by region, egress type, and health status without label bloat.

---

## 🚀 Quick Start

### 1. Installation
```bash
git clone https://github.com/dengyie/tetherpool.git
cd tetherpool

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the Interactive Showcase Demo
```bash
.venv/bin/python3 demo.py
```
Verifies pool topology, multi-region probing, circuit breaker self-healing, forward proxy gateway tunneling, and adapter exports in ~10 seconds.

### 3. Run Unit Tests (83 Tests Passing)
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

> **Network exposure:** both listeners bind to `127.0.0.1` by default and the control plane
> has no built-in user auth. To expose it beyond loopback, set `HOST=0.0.0.0` **and**
> `IP_POOL_API_TOKEN=<secret>` — every `/v1/*` endpoint then requires
> `Authorization: Bearer <secret>`.
- Forward Proxy Gateway: `http://127.0.0.1:9101`

---

## 🧭 Gateway Usage Examples (`:9101`)

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

## 📡 REST API Reference (`:9100`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Service health status |
| `GET` | `/dashboard` | Modern glassmorphism web console |
| `GET` | `/metrics` | Cloud-native Prometheus OpenMetrics |
| `GET` | `/v1/stats` | Real-time node count, health breakdown, and distribution |
| `GET` | `/v1/nodes` | List all nodes with filters (`region`, `egress_type`, `status`) |
| `GET` | `/v1/acquire` | Acquire sticky proxy lease (`account_id`, `region`, `egress_type`) |
| `POST` | `/v1/probe` | Trigger concurrent background probe across all nodes |
| `GET` | `/v1/circuit/events` | Audit log of circuit transitions and lease migrations |
| `POST` | `/v1/circuit/reset` | Operator manual reset of a node to `healthy` |
| `POST` | `/v1/circuit/rehome` | Manually re-home leases for a degraded/dead node |
| `GET` | `/v1/export/cpa` | Export proxies in CPA format |
| `GET` | `/v1/export/grok2api` | Export healthy nodes for `grok2api` |

---

## 📂 Repository Structure

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
├── tests/                      # Pytest isolated unit test suite (83 passing tests)
├── demo.py                     # Interactive end-to-end verification showcase
├── requirements.txt            # Python dependencies
└── start.sh                    # Dual-process background daemon manager
```

---

## 📄 License
MIT License.
