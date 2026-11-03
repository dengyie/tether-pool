# 🌐 IP Pool System (`ip-pool`)

> **Unified Global Dual-Stack IP Pool & Sticky Routing Engine**  
> Enterprise-grade proxy pool orchestrating **108 independent exits** (100 Cloudflare WARP IPv6 + 8 Datacenter Native IPv4/IPv6) across 6 global regions over an encrypted EasyTier P2P mesh network.

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg)](https://fastapi.tiangolo.com/)
[![Tests](https://img.shields.io/badge/tests-14%20passed-brightgreen.svg)]()
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

[中文文档 (README.zh-CN.md)](README.zh-CN.md)

---

## 📌 Architecture Overview

```
                      +---------------------------------------+
                      |         Upstream AI Gateways          |
                      | (grok2api, Gemini, CPA, Web Scrapers) |
                      +-------------------+-------------------+
                                          |
                        HTTP REST / CRC32 Sticky Lease
                                          v
                      +---------------------------------------+
                      |       IP Pool System (:9100)          |
                      |   - SQLite State & Lease Storage      |
                      |   - Dual-Stack Concurrent Prober      |
                      |   - Sticky Hash & Regional Router     |
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
- **Zero Public Exposure**: All proxy inbounds listen strictly on private EasyTier mesh IPs (`10.144.144.x:10801~10905`), leaving zero public listening ports open on VPS nodes.
- **Deterministic Sticky Hash**: Deterministic `CRC32(account_id) % healthy_node_count` ensures sticky session pinning for each account while automatically shedding load if a node fails.
- **Account Sharding**: Successfully sharded **2,103 Grok Web accounts** across 100 WARP exits at an even ratio of **21~22 accounts per IP**, drastically mitigating anti-bot risk.

---

## 🚀 Quick Start

### 1. Prerequisites & Installation
```bash
git clone <repo-url> /Users/mango/project/ip-pool
cd /Users/mango/project/ip-pool

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the Interactive Showcase Demo
```bash
.venv/bin/python3 demo.py
```
This demo runs in ~10 seconds and verifies:
1. Pool topology (108 active nodes).
2. Live multi-region dual-stack health probing.
3. Deterministic account routing.
4. Real outbound HTTP requests via the acquired proxy.

### 3. Run Unit Tests
```bash
.venv/bin/pytest
```

### 4. Start the Production API Service
```bash
./start.sh
```
- API Endpoint: `http://127.0.0.1:9100`
- Interactive API Docs: `http://127.0.0.1:9100/docs`
- Health check: `http://127.0.0.1:9100/health`

---

## 📡 REST API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Service health status |
| `GET` | `/v1/stats` | Real-time node count, health breakdown, and regional distribution |
| `GET` | `/v1/nodes` | List all nodes with filters (`region`, `egress_type`, `status`) |
| `GET` | `/v1/acquire` | Acquire a sticky proxy lease for a given account (`account_id`, `region`, `egress_type`) |
| `POST` | `/v1/probe` | Trigger an asynchronous background probe across all nodes |
| `GET` | `/v1/export/grok2api` | Export healthy nodes formatted for `grok2api` integration |
| `GET` | `/v1/export/cpa` | Export proxies formatted for `CPA` configuration |

### Example: Acquiring a Sticky Proxy
```bash
curl -s "http://127.0.0.1:9100/v1/acquire?account_id=user123@example.com&region=tokyo" | jq .
```
Response:
```json
{
  "account_id": "user123@example.com",
  "node_name": "aws-tokyo-warp-05",
  "proxy_url": "socks5h://10.144.144.3:10885",
  "exit_ip": "2a09:bac5:4304:dc::16:150",
  "region": "tokyo",
  "egress_type": "warp_ipv6",
  "assigned_at": "2026-11-03T20:30:15.123456"
}
```

---

## 📂 Repository Structure

```
ip-pool/
├── config/
│   └── nodes.yaml              # Static inventory of 108 proxy endpoints
├── core/
│   ├── models.py               # Pydantic data schemas
│   ├── prober.py               # Asynchronous dual-stack health prober
│   ├── router.py               # Sticky hash & constraint-based router
│   └── storage.py              # SQLite persistence engine
├── api/
│   └── server.py               # FastAPI REST service (:9100)
├── adapters/
│   ├── grok2api_adapter.py     # Database synchronizer & account sharder
│   └── cpa_adapter.py          # CPA config exporter
├── scripts/
│   ├── deploy_remote_node.py   # Remote sing-box + WARP provisioning SOP
│   └── probe_all.py            # CLI probe summary runner
├── tests/                      # Pytest unit test suite (14 passing tests)
├── demo.py                     # Self-contained interactive verification demo
├── requirements.txt            # Python dependencies
└── start.sh                    # Background daemon launcher
```

---

## 🔒 Security & Operational Rules

1. **Private Mesh Isolation**: Endpoints are strictly accessible via EasyTier (`10.144.144.0/24`). Never bind proxy inbounds to `0.0.0.0` on VPS hosts.
2. **SQLite Atomic Backups**: Always execute `VACUUM INTO '<file>.bak'` before performing updates on production relational databases.
3. **LeaseWeb Transit Optimization**: Always ensure `disable_ipv6 = true` in LeaseWeb Netherlands' EasyTier configuration to prevent cross-border IPv6 routing congestion.
