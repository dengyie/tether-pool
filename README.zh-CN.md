# 🌐 全局双栈 IP 池系统 (`ip-pool`)

> **跨国多节点双栈出口池与账号粘性哈希路由调度中枢**  
> 统一编排纳管 **108 个独立公网出口**（100 个 Cloudflare WARP IPv6 + 8 个机房原生公网 IPv4/IPv6），基于 EasyTier 全网虚拟网格内网安全互通，具备双栈亚秒级并发探活、CRC32 粘性会话分配与下游应用自动化洗牌能力。

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg)](https://fastapi.tiangolo.com/)
[![Tests](https://img.shields.io/badge/tests-14%20passed-brightgreen.svg)]()
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

[English Documentation (README.md)](README.md)

---

## 📌 架构全景

```
                      +---------------------------------------+
                      |         下游 AI 反代与业务应用         |
                      | (grok2api, Gemini Web, CPA, 爬虫集群)  |
                      +-------------------+-------------------+
                                          |
                          HTTP REST / CRC32 粘性租约
                                          v
                      +---------------------------------------+
                      |        全局 IP 池中枢 (:9100)          |
                      |   - SQLite 状态与租约持久化 (WAL)      |
                      |   - 双栈分层并发健康探测器 (Prober)     |
                      |   - 粘性哈希与区域标签路由器 (Router)   |
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
   - **100 个 Cloudflare WARP IPv6 出口**：每个海外主机部署 25 个独立 WireGuard 实例，真实出口分别落入东京 (loc=JP)、新加坡 (loc=SG)、荷兰 (loc=NL) 和印度 (loc=IN) 4 大边缘节点。
   - **8 个机房原生公网出口**：包含 6 个原生 IPv4 与 2 个原生 IPv6，覆盖东京、新加坡、荷兰、印度、香港与美西。
2. **零公网端口暴露（Mesh 纵深防御）**：
   - 所有代理端口（`:10801~10905`）100% 仅监听在 EasyTier 网格内网 IP（`10.144.144.x`），海外 VPS 外部防火墙零开放，彻底免疫全网端口扫描与探测。
3. **CRC32 粘性哈希会话保持**：
   - 基于 `CRC32(account_id) % healthy_nodes` 实现账号/任务级别的长期静态出口绑定；节点异常时自动剔除并平滑漂移至相邻健康节点。
4. **超大规模账号池均匀离散**：
   - 已无缝对接 `grok2api`，将 2,103 个 Grok Web 账号平均分摊到 100 个 WARP 出口，**单 IP 仅承载 21~22 个账号**（相较单节点 ~131 账号降幅达 84%），极大降低批量风控封号关联性。

---

## 🚀 快速上手

### 1. 环境准备与依赖安装
```bash
git clone <repo-url> /Users/mango/project/ip-pool
cd /Users/mango/project/ip-pool

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 运行交互式终端彩色验证 Demo
```bash
.venv/bin/python3 demo.py
```
演示脚本会在 10 秒内执行：
1. 校验 108 个节点的拓扑与存储加载；
2. 实时对 5 个不同区域的采样节点执行并发双栈探活，提取真实出口 IP；
3. 执行 4 组账号的粘性哈希映射与约束过滤；
4. 验证同一账号多次请求的 100% 粘性不变性；
5. 经由分配的代理节点发起真实公网 HTTP 请求，验证真实的 WARP 激活状态与国家归属。

### 3. 运行完整单元测试
```bash
.venv/bin/pytest
```

### 4. 启动后台守护服务
```bash
./start.sh
```
- 服务地址：`http://127.0.0.1:9100`
- Swagger 文档：`http://127.0.0.1:9100/docs`
- 健康探活：`http://127.0.0.1:9100/health`

---

## 📡 REST API 核心接口

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/health` | 服务探针 |
| `GET` | `/v1/stats` | 实时节点总量、健康/降级分布及各区域节点占比 |
| `GET` | `/v1/nodes` | 查询全量出口节点列表，支持 `region`、`egress_type`、`status` 筛选 |
| `GET` | `/v1/acquire` | 为指定账号申请粘性绑定的代理端点（支持区域与协议类型偏好） |
| `POST` | `/v1/probe` | 触发全池后台异步并发健康探测 |
| `GET` | `/v1/export/grok2api` | 导出供 `grok2api` `egress_nodes` 注入的格式 |
| `GET` | `/v1/export/cpa` | 导出供 CPA（Claude/OpenAI Proxy）使用的配置文件 |

### 请求示例：为账号分配代理
```bash
curl -s "http://127.0.0.1:9100/v1/acquire?account_id=user123@example.com&region=tokyo" | jq .
```
返回响应：
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

## 📂 项目结构规范

```
ip-pool/
├── config/
│   └── nodes.yaml              # 108 个底层出口节点的静态定义（IP、端口、区域、权重）
├── core/
│   ├── models.py               # Pydantic 领域模型
│   ├── prober.py               # 异步并发双栈探活器（Cloudflare Trace 亚秒级解析）
│   ├── router.py               # CRC32 粘性哈希与约束路由调度引擎
│   └── storage.py              # SQLite (data/ip_pool.db) 状态与历史持久化
├── api/
│   └── server.py               # FastAPI 服务端 (:9100)
├── adapters/
│   ├── grok2api_adapter.py     # grok2api 数据库同步与 2,103 账号均衡洗牌
│   └── cpa_adapter.py          # CPA per-credential 静态配置导出
├── scripts/
│   ├── deploy_remote_node.py   # 远端海外 VM 自动化批量部署 WireGuard 与 sing-box
│   └── probe_all.py            # CLI 命令行全量探活彩表输出
├── tests/                      # 14 项自动化单测套件 (pytest)
├── demo.py                     # 开箱即用的一键演示脚本
├── pytest.ini                  # 测试配置
├── requirements.txt            # Python 核心依赖定义
└── start.sh                    # 进程启停守护脚本
```

---

## 🔒 运维规范与安全红线

1. **严格网格隔离**：所有节点的代理监听只允许绑定在 `10.144.144.x`。任何情况下严禁在海外 VM 上将代理端点公开至 `0.0.0.0`。
2. **原子备份第一**：在执行 `adapters/grok2api_adapter.py` 等涉及修改业务数据库的操作时，必须通过 SQLite 原生 `VACUUM INTO` 创建不可变快照备份。
3. **LeaseWeb 跨境隧道调优**：LeaseWeb 荷兰主机的 EasyTier 配置中必须保持 `disable_ipv6 = true`，强制通过稳定可靠的 IPv4 直连隧道承载流量，规避公共国际 IPv6 链路丢包导致的 SOCKS5 握手超时。
