# TetherPool 通用抽象层接口与统一接入开发规范 (Universal Integration & Abstraction Spec)

> **版本**：v2.1  
> **更新日期**：2026-11-04  
> **适用业务**：`grok2api` (2,100+ 账号池)、`cpa-manager-plus`、`gemini2api`、批量造号注册机、高并发爬虫与代理中间件。

---

## 一、 为什么需要抽象层接口？

TetherPool 纳管了 **100 个 WARP IPv6 节点、8 个多机房原生公网节点以及 53+ 个 Clash 核心隧道节点**。
以往外部系统（如 `grok2api`）接入时，通过本地脚本直接侵入其私有 SQLite 数据库进行加密注入；CPA 采用静态 YAML 导出；Gemini 采用代码写死。

为了让任何外部系统能够**零摩擦、标准化、多范式**地接入 TetherPool 庞大的出口资源（尤其是充分利用 100+ WARP IPv6 出口，避免海量账号扎堆封禁），TetherPool 提供了**三层通用抽象接口体系**：

```
                    外部接入项目 (grok2api / cpa-manager-plus / 爬虫)
                                       │
        ┌──────────────────────────────┴─────────────────────────────┐
        ▼                                                            ▼
 【范式 A: 控制面 REST API & SDK 调度】               【范式 B: 数据面零配置网关透传】
  - POST /v1/allocation/batch                        - 统一监听端口：:9101
  - POST /v1/allocation/acquire                      - RFC 标准带参鉴权 (Parametric Auth):
  - GET  /v1/allocation/status                         http://{acc_id}:tetherpool@:9101
  - Python SDK (TetherPoolClient)                      http://{acc}__type_warp_ipv6@:9101
        │                                                            │
        └──────────────────────────────┬─────────────────────────────┘
                                       ▼
                     TetherPool 核心调度与分配引擎
           [容量交错均摊 Balanced Round-Robin / 极差 <= 1]
           [虚拟节点一致性哈希 Consistent Hash Ring]
           [两跳级联中继 Chained SOCKS5 Cascade]
           [电池生命周期 429 自动换电自愈]
                                       ▼
                          全局 161+ 出口节点资源
```

---

## 二、 接入范式一：控制面 REST API（批量账号分配引擎）

适用于外部系统在数据库中管理每个账号的代理配置（如 `grok2api` 的 `provider_accounts` 绑定 `egress_nodes`）。

### 1. 批量账号分配接口：`POST /v1/allocation/batch`
- **请求体（JSON）**：
  ```json
  {
    "service": "grok2api",
    "accounts": [
      "acc_0001@mango.com",
      "acc_0002@mango.com",
      "..."
    ],
    "strategy": "balanced_round_robin",
    "egress_preference": "warp_ipv6",
    "region": null,
    "max_accounts_per_ip": 30,
    "force_rebalance": false
  }
  ```
- **核心算法保证**：
  - **容量均摊交错平衡（Balanced Round-Robin）**：将 2,100 个账号按区域（东京、荷兰、新加坡、印度）交错打散到 100 个 IPv6 节点上，每个节点的账号分配极差 $\le 1$（例如 100 个节点，每个恰好承载 21 个账号，无热点）。
  - **增量幂等保护（`force_rebalance: false`）**：已有健康绑定的账号保持原 IP 不变，仅对新加入账号进行均摊填补。
- **响应体（JSON）**：
  ```json
  {
    "service": "grok2api",
    "total_accounts": 2100,
    "allocated_nodes_count": 100,
    "min_accounts_per_node": 21,
    "max_accounts_per_node": 21,
    "avg_accounts_per_node": 21.0,
    "allocations": {
      "acc_0001@mango.com": {
        "account_id": "acc_0001@mango.com",
        "node_name": "aws-tokyo-warp-01",
        "proxy_url": "socks5://10.144.144.3:10881",
        "gateway_proxy_url": "http://acc_0001@mango.com__type_warp_ipv6__region_tokyo:tetherpool@127.0.0.1:9101",
        "exit_ip": "2a09:bac5:3b20:10::16:101",
        "region": "tokyo",
        "egress_type": "warp_ipv6",
        "service": "grok2api",
        "allocated_at": "2026-11-04T02:30:00"
      }
    }
  }
  ```

### 2. 单账号按需分配：`POST /v1/allocation/acquire`
- **参数**：`service=grok2api&account_id=new_user@corp.com&egress_preference=warp_ipv6`
- **特性**：优先复用已有健康分配；若无分配，自动选择当前负载最低的健康节点。

### 3. 大盘分布监控：`GET /v1/allocation/status`
- 输出全局各节点承载的账号总数与健康状态，供监控大盘与运维告警。

---

## 三、 接入范式二：数据面零配置网关（Parametric Proxy Auth）

适用于**不想在本地编写任何分配逻辑**，只想把代理配置填给第三方库（`requests`, `httpx`, `curl`, `Playwright`）的场景。

TetherPool 网关（`:9101`）支持将路由参数直接嵌入到标准 HTTP 代理的用户名中：
```text
http://{account_id}[__type_{egress}][__region_{region}]:tetherpool@127.0.0.1:9101
```

### 1. 语法格式支持
| 格式风格 | 代理用户名示例 | 说明 |
| :--- | :--- | :--- |
| **基础格式** | `user_001:tetherpool` | 使用 `user_001` 走默认 `warp_ipv6` 确定性粘性路由 |
| **双下划线标签 (推荐)** | `user_001__type_warp_ipv6__region_tokyo:tetherpool` | 强制锁定东京区域与 IPv6 出口 |
| **URL 查询串格式** | `user_001?type=warp_ipv6&region=singapore:tetherpool` | 标准 Query-String 风格 |
| **键值对横杠格式** | `account=user_001-type=warp_ipv6-region=us-west:tetherpool` | 兼容 BrightData / Oxylabs 商业标准 |

### 2. 命令行与代码接入实操
```bash
# curl 直接验证
curl -x "http://test_bot__type_warp_ipv6__region_tokyo:tetherpool@127.0.0.1:9101" https://cloudflare.com/cdn-cgi/trace
```

Python `requests` / `httpx`：
```python
import httpx

proxies = "http://account_001__type_warp_ipv6:tetherpool@127.0.0.1:9101"
with httpx.Client(proxy=proxies) as client:
    resp = client.get("https://api.ipify.org")
    print(resp.text)
```

Playwright：
```python
browser = await playwright.chromium.launch()
context = await browser.new_context(
    proxy={
        "server": "http://127.0.0.1:9101",
        "username": "bot_01__type_warp_ipv6__region_tokyo",
        "password": "tetherpool"
    }
)
```

---

## 四、 官方 Python SDK 快速接入指南

在任何下游项目中直接引入 `adapters/tetherpool_client.py`（零第三方依赖）：

```python
from adapters.tetherpool_client import TetherPoolClient

client = TetherPoolClient(api_base="http://127.0.0.1:9100", gateway_base="http://127.0.0.1:9101")

# 1. grok2api 一键同步 2,100 个账号，均摊到 100 个 WARP IPv6 节点
accounts = [f"grok_acc_{i:04d}" for i in range(1, 2101)]
result = client.batch_allocate(
    accounts=accounts,
    service="grok2api",
    egress_preference="warp_ipv6",
    max_accounts_per_ip=30
)
print(f"分配完成: 均摊节点数 {result['allocated_nodes_count']}, 单节点账号: {result['min_accounts_per_node']}~{result['max_accounts_per_node']}")

# 2. 单账号取代理（支持返回原生 SOCKS5 与网关透传 URL）
proxy_info = client.get_proxy_for_account("grok_acc_0001", service="grok2api")
print(proxy_info["proxy_url"])          # socks5://10.144.144.3:10881
print(proxy_info["gateway_proxy_url"])  # http://grok_acc_0001__type_warp_ipv6__region_tokyo:tetherpool@127.0.0.1:9101

# 3. 电池上报异常自愈（遇 429 自动在同区域无感换电）
swap_res = client.report_battery_error(
    account_id="grok_acc_0001",
    error_type="rate_limited_429",
    service="grok2api"
)
print("换电状态:", swap_res["status"])
```

---

## 五、 总结与最佳实践

1. **大批量持久账号（如 `grok2api`）**：推荐调用 `client.batch_allocate()`，既能将直连 `socks5://` 写入自身数据库，又能获得极差严格受控（$\le 1$）的安全均摊。
2. **轻量自动化 / 爬虫（如 Playwright / requests）**：推荐直接使用 `:9101` 网关的带参鉴权 `http://{acc_id}__type_warp_ipv6:tetherpool@127.0.0.1:9101`，省去所有中间状态维护。
3. **注册机批造新号**：调用 `/v1/acquire_for_registration`，触发防女巫 30~60 分钟冷却锁定，保障造号高通过率。
