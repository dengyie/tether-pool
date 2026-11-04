# TetherPool 全量 REST API 参考与直接管控手册
*(Complete REST API Reference & Management Manual)*

TetherPool 控制面提供高可用、异步非阻塞的 RESTful API，默认监听于 `:9100`。
本文档涵盖全量 API 端点规范、请求参数、响应体结构以及直接管控节点与热重载的完整用例。

---

## 一、鉴权机制 (Authentication)

当服务器环境变量配置了 `IP_POOL_API_TOKEN` 时，除 `/health` 之外的所有端点均受到 Token 保护。

支持以下三种凭据传递方式：
1. **HTTP 请求头 (推荐)**：
   ```http
   Authorization: Bearer <your_token>
   ```
2. **URL Query 参数**：
   ```http
   GET http://10.144.144.9:9100/v1/stats?token=<your_token>
   ```
3. **Cookie 凭据**（Web 控制台登录时自动下发）：
   ```http
   Cookie: ip_pool_token=<your_token>
   ```

---

## 二、节点直接管控 API (Node Direct Management)

该组端点支持下游平台直接通过 API 纳管、调整、删除代理节点以及动态热重载，**无需登录 VPS 操作终端**。

### 1. 创建或更新节点 (Create / Upsert Node)
- **方法**：`POST /v1/nodes`
- **说明**：向代理池动态注册一个新节点。系统将自动执行首轮质量评分并在数据库中持久化。
- **请求体 (JSON)**：
  ```json
  {
    "name": "sg-custom-01",
    "host": "10.144.144.13",
    "port": 10881,
    "protocol": "socks5h",
    "region": "singapore",
    "egress_type": "native_ipv4",
    "tags": ["self_hosted", "datacenter"],
    "weight": 100,
    "tier": "premium",
    "configured_tier": "premium",
    "relay_mode": "relay_fallback"
  }
  ```
  *(注：`tier` 可选 `premium` 或 `standard`；`relay_mode` 可选 `direct_only`, `relay_pref`, `relay_fallback`)*
- **成功响应**：`200 OK` (返回完整的 `ProxyNode` 对象)

---

### 2. 获取单节点详情 (Get Node)
- **方法**：`GET /v1/nodes/{name}`
- **说明**：实时查询节点的健康状态、公网出口 IP、最近探测延迟、连续成功/失败计数、熔断器状态及质量分。
- **成功响应**：`200 OK`
  ```json
  {
    "name": "sg-custom-01",
    "host": "10.144.144.13",
    "port": 10881,
    "protocol": "socks5h",
    "region": "singapore",
    "egress_type": "native_ipv4",
    "tags": ["self_hosted", "datacenter"],
    "weight": 100,
    "exit_ip": "119.8.x.x",
    "latency_ms": 48.2,
    "status": "healthy",
    "circuit_state": "healthy",
    "consecutive_failures": 0,
    "consecutive_successes": 52,
    "next_probe_allowed_at": null,
    "relay_mode": "relay_fallback",
    "tier": "premium",
    "configured_tier": "premium",
    "quality_score": 95
  }
  ```
- **错误响应**：`404 Not Found` (当节点名称不存在时)

---

### 3. 热更新节点属性 (Patch Node)
- **方法**：`PATCH /v1/nodes/{name}`
- **说明**：增量修改节点的运行参数（如权重、分级、标签、中继跳板、或手动置为 dead 触发自愈）。
- **请求体 (JSON)**：
  ```json
  {
    "weight": 200,
    "tier": "premium",
    "status": "dead"
  }
  ```
  *(注：若将 `status` 更新为 `"dead"`，系统将自动触发绑定驱逐与重编排机制，将该节点上的账号安全迁移至备用节点)*
- **成功响应**：`200 OK` (返回更新后的 `ProxyNode` 对象)

---

### 4. 下线并删除节点 (Delete Node)
- **方法**：`DELETE /v1/nodes/{name}`
- **Query 参数**：
  - `rehome` (bool, 可选, 默认 `true`): 是否在删除前将绑定该节点的下游账号自动重编排至其他健康节点。
- **调用示例**：
  ```bash
  curl -X DELETE "http://10.144.144.9:9100/v1/nodes/sg-custom-01?rehome=true"
  ```
- **成功响应**：`200 OK`
  ```json
  {
    "deleted": true,
    "node_name": "sg-custom-01",
    "rehome_result": {
      "rehomed_count": 0,
      "allocations_rehomed_count": 3,
      "details": [
        {"account_id": "user_01", "new_node": "aws-tokyo-01"}
      ]
    }
  }
  ```

---

### 5. 免重启热重载全量拓扑 (Hot Reload Config)
- **方法**：`POST /v1/config/reload`
- **说明**：通知控制面重新读取本地 `nodes.yaml` 配置文件，增量同步节点、继承已有探测分值，并清理无效旧节点。**无需执行 `systemctl restart`**。
- **调用示例**：
  ```bash
  curl -X POST http://10.144.144.9:9100/v1/config/reload
  ```
- **成功响应**：`200 OK`
  ```json
  {
    "reloaded": true,
    "config_file": "/opt/ip-pool/config/nodes.yaml",
    "nodes_loaded": 479
  }
  ```

---

## 三、代理路由与分配 API (Routing & Allocation)

### 1. 单账号粘性分配 / 获取 (Acquire Node)
- **方法**：`POST /v1/acquire`
- **Query 参数**：
  - `account_id` (string, 必需): 账号唯一标识（如邮箱、UUID、用户名）
  - `egress_type` (string, 可选): 出口类型 (`warp_ipv6`, `native_ipv4`, `native_ipv6`)
  - `region` (string, 可选): 地区偏好 (`tokyo`, `singapore`, `netherlands`, `india`)
  - `tier` (string, 可选): 质量分层 (`premium`, `standard`)
  - `tier_policy` (string, 可选): 分级策略 (`standard_first`, `premium_first`, `premium_only`, `standard_only`)
  - `force_rebind` (bool, 可选, 默认 `false`): 强制换绑新节点
  - `service` (string, 可选, 默认 `"default"`): 业务命名空间
- **成功响应**：`200 OK`
  ```json
  {
    "account_id": "user_1001",
    "node_name": "aws-tokyo-warp-01",
    "proxy_url": "socks5h://10.144.144.3:10881",
    "exit_ip": "2a09:bac5:4307:dc::16:16f",
    "region": "tokyo",
    "egress_type": "warp_ipv6",
    "assigned_at": "2026-11-05T02:45:06.505024",
    "tier": "standard"
  }
  ```

---

### 2. 幂等只读查询当前绑定 (Get Bound Lease)
- **方法**：`GET /v1/acquire`
- **说明**：纯只读安全查询，不消耗容量、不发生状态变更，适合做前置健康检查或展示。
- **Query 参数**：`account_id` (必需), `service` (可选)
- **错误响应**：`404 Not Found` (未分配时)

---

### 3. 多业务通用分配接口 (Universal Allocation Acquire)
- **方法**：`POST /v1/allocation/acquire`
- **说明**：面向多微服务、多业务线设计的通用分配引擎，支持指定业务命名空间与容量保护。
- **Query 参数**：
  - `service` (string, 可选, 默认 `"default"`): 如 `"grok2api"`, `"cpa_manager"`
  - `account_id` (string, 必需)
  - `egress_preference` (string, 可选)
  - `region` (string, 可选)
  - `tier` (string, 可选)
  - `tier_policy` (string, 可选)
  - `force_rebind` (bool, 可选)
- **成功响应**：`200 OK`
  ```json
  {
    "account_id": "grok_acc_101",
    "node_name": "sg-huawei-warp-05",
    "proxy_url": "socks5h://10.144.144.13:10885",
    "gateway_proxy_url": "http://grok_acc_101:tetherpool@10.144.144.9:9101",
    "exit_ip": "2a09:bac5:3120:e8::1f:2b",
    "region": "singapore",
    "egress_type": "warp_ipv6",
    "service": "grok2api",
    "allocated_at": "2026-11-05T02:46:12.112000",
    "tier": "standard"
  }
  ```

---

### 4. 批量账号分配调度 (Batch Allocation)
- **方法**：`POST /v1/allocation/batch`
- **请求体 (JSON)**：
  ```json
  {
    "service": "cpa_campaign_a",
    "accounts": ["bot_1", "bot_2", "bot_3", "bot_4"],
    "strategy": "balanced_round_robin",
    "region": "tokyo",
    "tier": "standard",
    "max_accounts_per_ip": 10,
    "force_rebalance": false
  }
  ```
  *(注：`strategy` 支持 `balanced_round_robin` 平衡轮询或 `consistent_hash` 一致性哈希)*
- **成功响应**：`200 OK` (返回各账号的分配映射及统计分布指标)

---

### 5. 账号解绑与释放 (Release Allocation)
- **方法**：`POST /v1/allocation/release`
- **Query 参数**：`account_id` (必需), `service` (必需)
- **成功响应**：`{"status": "released", "account_id": "...", "service": "..."}`

---

## 四、账号风控与自愈管控 API (Battery Events & Circuit)

### 1. 业务异常反馈与自动换绑 (Battery Event Report)
- **方法**：`POST /v1/battery/event`
- **说明**：下游业务触发限流、封号或验证码失效时上报该事件，系统自动执行隔离和账号迁移。
- **请求体 (JSON)**：
  ```json
  {
    "account_id": "user_alpha",
    "event_type": "rate_limited_429",
    "target_service": "grok2api",
    "force_rebind": true
  }
  ```
  *(可选 `event_type`: `rate_limited_429`, `banned_401`, `turnstile_failed`, `timeout`, `success`)*
- **成功响应**：`200 OK`

---

### 2. 防女巫注册防关联租约 (Anti-Sybil Registration)
- **方法**：`GET /v1/acquire_for_registration`
- **说明**：专为新账号注册打造。分配成功后将对分配到的出口 IP 加锁（默认 1800 秒），在冷却期内绝不分配给同业务的第二个账号注册。
- **Query 参数**：
  - `account_id` (必需)
  - `target_service` (必需)
  - `cooldown_seconds` (可选, 默认 `1800`)
- **成功响应**：`200 OK` (含 `cooled_until` 字段)

---

### 3. 重置熔断器 (Circuit Reset)
- **方法**：`POST /v1/circuit/reset`
- **Query 参数**：`node_name` (可选，不传则全池重置)
- **说明**：手动将 `dead` 状态节点重置为 `unprobed` 并清除失败计数。

---

### 4. 手动重编排迁移 (Circuit Rehome)
- **方法**：`POST /v1/circuit/rehome`
- **Query 参数**：`node_name` (必需)
- **说明**：立即将指定节点上的所有活跃租约和账号迁移至同区域、同质量等级的健康节点。

---

## 五、健康探活与网络质量 API (Probing)

### 1. 触发全量并发探测 (Trigger Probe Sweep)
- **方法**：`POST /v1/probe`
- **Query 参数**：`concurrency` (int, 可选, 默认 `25`, 建议最高 `40`)
- **说明**：异步非阻塞并发探活全池节点（内置并发防重入锁保护）。

---

### 2. 即时单节点连通性测试 (Test Single Node)
- **方法**：`POST /v1/probe/test_node`
- **Query 参数**：`node_name` (必需)
- **成功响应**：`ProbeResult` (包含 `exit_ip`, `latency_ms`, `success`, `error`)

---

### 3. 指定目标域名可达性探活 (Probe Target Domain)
- **方法**：`POST /v1/probe/target`
- **Query 参数**：`node_name` (必需), `target_url` (必需，如 `https://api.x.ai/health`)
- **说明**：通过指定节点的 socks5 出口去探测下游具体业务域名的网络状态与响应延迟。

---

## 六、监控指标与格式导出 API (Monitoring & Exports)

### 1. Prometheus 监控指标 (OpenMetrics)
- **方法**：`GET /metrics`
- **说明**：标准 Prometheus 抓取端点，暴露各区域、协议、分级的节点与绑定计数，包含 `tier` 维度标签。

### 2. 统计摘要 (Pool Stats)
- **方法**：`GET /v1/stats`
- **说明**：返回全池总节点数、存活数、劣质数、熔断数、平均质量分及地区/分层分布。

### 3. Grok2api 格式导出
- **方法**：`GET /v1/export/grok2api`

### 4. CPA 格式导出
- **方法**：`GET /v1/export/cpa`
