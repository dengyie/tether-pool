# TetherPool 多业务平台接入与集成全指南
*(Universal Downstream Integration Guide for TetherPool)*

本文档详细说明第三方系统、反代服务、爬虫集群及自动化脚本如何接入与使用 **TetherPool IP 代理池与智能路由系统**。

---

## 一、系统服务拓扑与网络说明

TetherPool 对外提供两个核心服务端口（部署于中心节点，如 EasyTier 局域网 `10.144.144.9`）：

| 模块名称 | 监听端口 | 协议类型 | 核心职能 |
| :--- | :--- | :--- | :--- |
| **Unified Forward Gateway (正向网关)** | `:9101` | HTTP / HTTPS (CONNECT) | **数据面代理出口**：支持任意标准 HTTP/HTTPS 代理客户端，支持基于认证凭据的动态零 API 路由 |
| **Control Plane API (管控控制面)** | `:9100` | HTTP (REST / JSON) | **控制面管理中心**：节点 CRUD 直接管控、粘性分配、批处理、探活、指标、熔断自愈 |

---

## 二、三种主流接入模式对比

下游业务平台接入 TetherPool 支持以下三种模式：

```
+-----------------------------------------------------------------------------------+
|                                 业务系统 / 客户端                                  |
+-----------------------------------------------------------------------------------+
       |                                   |                                   |
  [Mode A: 零改造网关]               [Mode B: REST API 调度]            [Mode C: 官方 SDK]
       |                                   |                                   |
       v                                   v                                   v
+-----------------------+        +-----------------------+        +-----------------------+
|  Gateway (:9101)      |        | Control Plane (:9100) |        | TetherPoolClient SDK  |
| - 参数化用户名路由      |        | - POST /v1/acquire    |        | - 自动重试 & 异常上报  |
| - 零 API 交互直连      |        | - POST /allocation/*  |        | - 粘性生命周期管理    |
+-----------------------+        +-----------------------+        +-----------------------+
       \                                   /                                   /
        -----------------------------------------------------------------------
                                           |
                                           v
                              +-------------------------+
                              |   479+ 节点动态出口池   |
                              | - 420 WARP IPv6 (普通池)|
                              | - 8 自建原生 (优质池)   |
                              +-------------------------+
```

### 模式 A：零代码改造·统一正向网关模式 (推荐)
- **适用场景**：已有的反向代理服务（如 `grok2api`）、浏览器自动化框架（Playwright / Puppeteer）、现有爬虫工具、CURL 脚本等。
- **机制**：通过网关 Basic Auth 用户名字段编码路由指令，**无需发起任何前置 API 调用**，网关在处理代理握手时自动解析并完成粘性绑定与转发。
- **网关地址**：`http://10.144.144.9:9101`
- **代理凭据语法**：
  ```
  http://<account_id>[__param1_value][__param2_value]:<password>@10.144.144.9:9101
  ```
  *(默认密码固定为 `tetherpool`)*

#### 支持的参数化指令表：
| 参数标识 | 可选值 | 说明 | 示例 |
| :--- | :--- | :--- | :--- |
| `type_<egress>` | `warp_ipv6`, `native_ipv4`, `native_ipv6`, `clash_proxy` | 指定出网节点底层协议类型 | `__type_warp_ipv6` |
| `region_<loc>` | `tokyo`, `singapore`, `netherlands`, `india` | 指定出网国家/地区 | `__region_tokyo` |
| `tier_<level>` | `premium`, `standard` | 指定质量分层（优质池/普通池） | `__tier_premium` |
| `policy_<pol>` | `standard_first`, `premium_first`, `premium_only`, `standard_only` | 跨池分级降级策略 | `__policy_premium_first` |
| `rebind_true` | (标志位) | 强制换绑该账号的新出口节点 | `__rebind_true` |
| `region_strict_true` | (标志位) | 区域硬约束：候选集只在 `region_<loc>` 内筛选，不跨区降级；无可用节点时返回 503 `pool_exhausted`。默认关闭 | `__region_us-west__region_strict_true` |
| `bind_persist_true` | (标志位) | 持久化换绑：当本次调用的约束与现有绑定节点冲突时，将新绑定写回数据库（`constraint_rebind`）；不带此标志的换绑请求保持临时性、不落库。默认关闭 | `__region_singapore__bind_persist_true` |

> **约束语义（2026-11-12）**：凭据携带的约束集（region / region_strict / egress / tier / policy）在绑定时持久化（`account_allocations.bound_*` 列）。节点故障后的 auto_rehome 只会在约束集内迁移；约束区域内无健康候选时保持原绑定不动并计入 `ip_pool_rehome_blocked_total` 指标（`rehome_blocked`），绝不迁移到约束区域外。

> **示例**：
> - 账号 `bot_user_01` 绑定东京节点：`bot_user_01__region_tokyo:tetherpool@10.144.144.9:9101`
> - 核心账号 `vip_99` 强制走新加坡优质原生池：`vip_99__region_singapore__tier_premium__policy_premium_only:tetherpool@10.144.144.9:9101`

---

### 模式 B：控制面 REST API 显式调度模式
- **适用场景**：多租户调度中心、统一资产管理系统、账号注册机器人（防女巫机制）。
- **机制**：在执行请求前先调用 `:9100` 获取当前分配的专用节点信息，或进行全局批处理分发。
- **优势**：可精确获得节点的实时公网 IP、延迟、可用性评分、冷却剩余时间等元数据。

---

### 模式 C：官方 Python SDK 模式 (`adapters/tetherpool_client.py`)
- **适用场景**：Python 语言开发的后端服务与自动化工作流。
- **机制**：封装了连接池、自适应重试、动态降级与风控状态上报（例如 429 自动换绑自愈）。

---

## 三、常用业务平台无缝接入实战

### 1. `grok2api` (Grok 逆向/API 兼容代理)
`grok2api` 负责管理大量 xAI/Grok 账号，需要高并发、低关联度的出网代理。

#### 配置方式（环境变量）：
在 `grok2api` 的 `.env` 或 `docker-compose.yml` 中：
```bash
# 全局统一通过 TetherPool 正向网关出网
PROXY_URL=http://10.144.144.9:9101

# 或者针对特定账号使用动态参数化代理
# 格式：http://grok_user_{id}__tier_standard:tetherpool@10.144.144.9:9101
```

#### 进阶风控联动（429 自动换绑）：
当 Grok 账号遭遇 Cloudflare 429 或账号风控时，调用 TetherPool 的 Battery Event 接口自动触发节点换绑：
```bash
curl -X POST http://10.144.144.9:9100/v1/battery/event \
  -H "Content-Type: application/json" \
  -d '{
    "account_id": "grok_user_01",
    "event_type": "rate_limited_429",
    "target_service": "grok2api",
    "force_rebind": true
  }'
```
系统会瞬间将 `grok_user_01` 隔离重编排到另一个健康的独立出网节点，下次该账号通过网关发请求时即无缝走新出口。

#### 死节点故障自愈契约（重要）
- **推荐零配置**：继续使用上面的网关地址即可。上游节点死亡时网关会在每次请求内最多重试 `GATEWAY_MAX_ATTEMPTS`（默认 3）个不同出口，**重试耗尽后自动迁移该账号的持久绑定**；下游只需处理错误头：
  - `502` + `X-TetherPool-Error: binding_rehomed, retry=1` → 绑定已迁移，立即重放一次请求即可成功。
  - `503` + `X-TetherPool-Error: pool_exhausted` → 全池暂无可用节点，退避重试。
  - `503` + `X-TetherPool-Error: no_healthy_target` → 该账号无可用替代节点，需要扩容。
- **若下游必须自行维护"账号→节点"映射**：**禁止一次性静态导入后长期信任**（2026-11-07 `mesh-nl` 事故的根因）。应改为轮询绑定纪元：
  ```bash
  # 1) 轻量轮询纪元（建议 30s）
  curl -s 'http://10.144.144.9:9100/v1/bindings/epoch?service=grok2api'
  # {"service":"grok2api","epoch":43,"changed_at":"...","last_change_reason":"domain_rehome"}

  # 2) 纪元变化时只拉增量，并覆盖到本地映射
  curl -s 'http://10.144.144.9:9100/v1/export/grok2api?since_epoch=42'
  # {"assignments":{"grok_user_01":{"name":"sg-huawei-warp-05","url":"socks5://..."}},"epoch":43,...}
  ```
  首次冷启动可以不带 `since_epoch` 做一次全量对齐。详见 [API.md](./API.md) 的「绑定增量对账契约」与「死节点自愈部署参数」两节。

---

### 2. CPA Manager Plus (自动化运营矩阵)
TetherPool 原生支持 CPA 格式的批量导出端点：

```bash
# 导出当前全部健康节点为 CPA 代理列表
curl -s http://10.144.144.9:9100/v1/export/cpa
```
返回输出格式标准：
```text
http://10.144.144.9:9101:cpa_task_01:tetherpool
http://10.144.144.9:9101:cpa_task_02:tetherpool
...
```
在 CPA 后台导入该 URL 或批量粘贴，即可实现每个任务固定独立出口。

---

### 3. Gemini Web 逆向与爬虫业务
Gemini Web 逆向对 IP 质量和 TLS 特征极其敏感。
- **核心操作**：为 Gemini 账号分配 `tier=premium`（自建原生机房 IPv4/IPv6 出口，质量分 90+，纯净度高）。
- **注册/新号场景**：调用 `/v1/acquire_for_registration` 激活 **Anti-Sybil 防女巫 IP 冷却锁**，确保 30 分钟内同一公网出口不被第二个注册请求复用。

```bash
# 获取注册防女巫节点
curl -s "http://10.144.144.9:9100/v1/acquire_for_registration?account_id=gemini_reg_101&target_service=gemini_web&cooldown_seconds=1800"
```

---

### 4. Playwright / Puppeteer 自动化集群

```javascript
// Node.js + Playwright 接入示例
const { chromium } = require('playwright');

(async () => {
  const accountId = 'browser_bot_01';
  
  // 启动浏览器，指向统一网关
  const browser = await chromium.launch({
    proxy: {
      server: 'http://10.144.144.9:9101',
      // 用户名携带参数：指定走日本节点、优质池、自动粘性
      username: `${accountId}__region_tokyo__tier_premium__policy_premium_first`,
      password: 'tetherpool'
    }
  });

  const page = await browser.newPage();
  await page.goto('https://api.ipify.org?format=json');
  console.log(await page.textContent('body'));
  
  await browser.close();
})();
```

---

## 四、多语言请求接入代码示例

### Python (httpx / requests)

```python
import httpx

# 1. 直接通过网关出网 (零 API 调用，自动按 account_id 粘性)
account_id = "user_883"
proxy_url = f"http://{account_id}__region_tokyo__tier_standard:tetherpool@10.144.144.9:9101"

with httpx.Client(proxies=proxy_url, timeout=15.0) as client:
    resp = client.get("https://api64.ipify.org?format=json")
    print("Outbound IP:", resp.json()["ip"])

# 2. 或者使用官方 SDK
from adapters.tetherpool_client import TetherPoolClient

client = TetherPoolClient(api_base="http://10.144.144.9:9100", gateway_base="http://10.144.144.9:9101")
lease = client.get_proxy_for_account(account_id="user_883", tier="premium")
print(f"Assigned node: {lease.node_name}, Proxy: {lease.proxy_url}")
```

### Go (net/http)

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
)

func main() {
	// 网关代理地址，带账号与路由参数
	proxyRaw := "http://user_golang_01__region_singapore:tetherpool@10.144.144.9:9101"
	proxyURL, _ := url.Parse(proxyRaw)

	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
		},
	}

	resp, err := client.Get("https://api64.ipify.org?format=json")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Println("Response:", string(body))
}
```

### cURL 快速测试与排错

```bash
# 测试 1：常规默认出网（轮询 WARP IPv6 出口）
curl -x http://test_curl:tetherpool@10.144.144.9:9101 https://api64.ipify.org?format=json

# 测试 2：强制指定使用日本原生/优质池
curl -x http://test_curl__region_tokyo__tier_premium:tetherpool@10.144.144.9:9101 https://api.ipify.org?format=json

# 测试 3：强制重置换绑节点 (rebind_true)
curl -x http://test_curl__rebind_true:tetherpool@10.144.144.9:9101 https://api64.ipify.org?format=json
```
