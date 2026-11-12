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
    "relay_mode": "relay_fallback",
    "fault_domain": "",
    "provider_region": ""
  }
  ```
  *(注：`tier` 可选 `premium` 或 `standard`；`relay_mode` 可选 `direct_only`, `relay_pref`, `relay_fallback`。`fault_domain` 为**故障域 (fault domain)** 标识，同一宿主机上的所有监听端口应留空（自动回退为 `host`）或显式填同一个值，一个宿主机故障时它们会被一次性整体隔离；`provider_region` 为云厂商可用区标识（如 `leaseweb-nl-ams`），仅作观测用途。API 创建的节点系统会自动打上 `api_managed` 标签，即使执行 `POST /v1/config/reload` 重新读取静态 YAML，动态注册的节点也绝不会被错误标记为 dead 或被修剪删除)*
- **`config/nodes.yaml` 中的对应写法**（可选，缺省时 `fault_domain` 回退为 `host`）：
  ```yaml
  - name: nl-warp-01
    host: 10.144.144.12
    port: 10881
    fault_domain: mesh-nl      # 显式声明；省略则等于 host
    provider_region: leaseweb-nl-ams
    tier: standard
    egress_type: warp_ipv6
  ```
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
  - `region_strict` (bool, 可选, 默认 `false`): 区域硬约束 —— 候选集只在 `region` 内筛选，不跨区降级；无可用节点时返回 `503` + `X-TetherPool-Error: pool_exhausted`
  - `bind_persist` (bool, 可选, 默认 `false`): 当换绑请求与现有绑定冲突时持久化新绑定（`constraint_rebind`）；不带此标志的冲突换绑保持临时性、不落库
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
  - `region_strict` (bool, 可选, 默认 `false`): 区域硬约束，见 `/v1/acquire`
  - `bind_persist` (bool, 可选, 默认 `false`): 冲突换绑持久化，见 `/v1/acquire`
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

## 三·补充、绑定增量对账契约 (Binding Reconciliation — R5)

> **背景**：2026-11-07 `mesh-nl` 整机掉线事故中，grok2api 通过"一次性静态导入"拿到了 511 个账号的节点映射，此后主机消失它也无从知晓，于是一直对着死节点重试。**静态导入是错误用法**，下游必须按下面的契约做增量对账。

下游有两种合法接法，任选其一（推荐 1）：

### 方案 1：零配置网关路由（推荐）
下游不再关心"账号 → 哪个节点"，而是把代理地址固定指向网关，用账号名做认证前缀：
```
http://<account_id>:<gateway_password>@<gateway_host>:9101
```
节点绑定完全由 TetherPool 服务端维护；上游节点死亡时网关自动重试 3 次（`GATEWAY_MAX_ATTEMPTS`）并在重试耗尽后把该账号的持久绑定迁移到健康节点，下游只需一次重试即可，不需要感知节点列表。故障恢复期间网关返回：
- `502 Bad Gateway` + `X-TetherPool-Error: binding_rehomed, retry=1` — 绑定已迁移，**立即重放一次**该请求即可成功。
- `503 Service Unavailable` + `X-TetherPool-Error: pool_exhausted` — 池内已无可分配节点，属于全站故障，退避后再试。
- `503 Service Unavailable` + `X-TetherPool-Error: no_healthy_target` — 该账号在目标地区/出口类型下没有可用替代节点，需人工扩容。

### 方案 2：绑定纪元 (binding epoch) 增量拉取
仍需要在自己进程内维护"账号→节点"映射的下游，按以下两步轮询，**禁止**一次性拉全量后长期信任：

#### 2.0.1 查询当前绑定纪元
- **方法**：`GET /v1/bindings/epoch?service=grok2api`
- **说明**：极轻量端点，只返回一个单调递增整数。任何账号的绑定发生迁移（重编排、故障域隔离、手动改绑）都会让该业务的 epoch +1。调用间隔建议 30s，比拉全量导出便宜两个数量级。
- **成功响应**：`200 OK`
  ```json
  {
    "service": "grok2api",
    "epoch": 42,
    "changed_at": "2026-11-07T03:14:05.120000",
    "last_change_reason": "domain_rehome"
  }
  ```
  *(epoch 为 `0` 表示该业务尚无任何绑定变更记录，此时可安全地做首次全量对齐)*

#### 2.0.2 拉取增量绑定
- **方法**：`GET /v1/export/grok2api?since_epoch=42`
- **说明**：**只**返回 `epoch > since_epoch` 的变更，且每个账号只出现一次——映射到它的**当前**节点，而不是中间跳转过的节点。下游把这些条目覆盖到自己缓存的映射上，随后把本地记录的 epoch 提升到响应里的 `epoch` 即可。响应里没有任何账号回滚/删除条目，因此天然幂等，可以安全重放。
- **成功响应**：`200 OK`
  ```json
  {
    "count": 511,
    "nodes": [ { "name": "nl-warp-01", "url": "socks5://10.144.144.12:10881", "region": "netherlands", "egress_type": "warp_ipv6", "exit_ip": "" } ],
    "assignments": { "grok_acc_101": { "name": "sg-huawei-warp-05", "url": "socks5://10.144.144.13:10885", "region": "singapore", "egress_type": "warp_ipv6", "exit_ip": "" } },
    "epoch": 43,
    "since_epoch": 42,
    "changed_at": "2026-11-07T03:14:41.882000",
    "last_change_reason": "domain_rehome",
    "reason_breakdown": { "domain_rehome": 511 }
  }
  ```
  - `assignments`：本次需要应用的增量映射（账号 → 当前节点），以它为权威。
  - `nodes`：`assignments` 中涉及节点的去重列表，便于下游一次性预热连接池。
  - `epoch` / `since_epoch`：对账后的新纪元与本次请求的起始纪元。
  - `reason_breakdown`：各迁移原因的条目数（`auto_rehome`, `domain_rehome`, `retry_exhausted` 等），便于下游区分数以千计的"整机迁移"与零星换绑。
- **错误响应**：`422 Unprocessable Entity`（`since_epoch` 为负或非整数）
- **不带 `since_epoch`**：保持历史全量导出语义（健康节点列表），仅用于**首次冷启动对齐**。

**t = 0 → t + 5s 的故障恢复时序**：EasyTier peer 消失（≈1s 内）→ 整机 25 个监听合并为**一次**域事件、511 个账号在**单个**事务里批量迁移（epoch 42→43）→ 下游下一次 `GET /v1/bindings/epoch` 看到 43，执行一次 `?since_epoch=42` 拉取 511 条增量，全部恢复。第一轮成功率 > 95%，后续 100%。


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
- **Query 参数**：`since_epoch` (可选, ≥0)
- **说明**：
  - 不带 `since_epoch`：全量导出 `healthy` 节点的 `socks5://` 地址，仅适用于下游**首次冷启动对齐**。
  - 带 `since_epoch=N`：增量对账，仅返回 epoch > N 期间绑定发生迁移的账号及其**当前**节点（详见「三·补充、绑定增量对账契约」）。长期运行的下游必须走增量模式。

### 3.1 绑定纪元端点
- **方法**：`GET /v1/bindings/epoch?service=grok2api`
- **说明**：返回当前绑定纪元 `{service, epoch, changed_at, last_change_reason}`。

### 4. CPA 格式导出
- **方法**：`GET /v1/export/cpa`

---

## 七、故障域与死节点自愈指标 (R3 / R7)

以下指标由网关进程写入共享的 `runtime_metrics` 表，由控制面 `/metrics` 端点统一渲染，因此**只需抓取控制面一个端点**。

| 指标名 | 类型 | 标签 | 含义 |
|---|---|---|---|
| `ip_pool_gateway_attempts_per_request` | histogram | `le` | 每次客户端请求实际尝试了多少个出口（1/2/3/4/+Inf） |
| `ip_pool_retry_exhausted_total` | counter | `service`, `reason` | 网关重试预算耗尽的次数 |
| `ip_pool_rehome_total` | counter | `service`, `trigger` | 账号持久绑定被迁移的次数（`trigger`: `circuit_dead`, `retry_exhausted`, …） |
| `ip_pool_domain_trip_total` | counter | `domain`, `policy` | 整个故障域被隔离的次数（**一个主机掉线只应 +1**） |
| `ip_pool_host_unreachable_seconds` | gauge | `host` | 该故障域当前已持续不可达的秒数，恢复时置 0 |

关键告警建议：
- `rate(ip_pool_retry_exhausted_total[5m]) > 0` 持续出现 → 有节点在"半死"状态（能建连但转发失败）。
- `ip_pool_domain_trip_total` 单分钟突增 → 整机级故障，立即查看对应 `domain`。
- `ip_pool_host_unreachable_seconds > 30` → 该主机上的账号迁移可能不充分，需要人工介入。

---

## 八、死节点自愈部署参数 (Deployment Parameters)

### 网关进程 (`gateway/server.py`)

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `GATEWAY_HOST` | `127.0.0.1` | 网关监听地址 |
| `GATEWAY_PORT` | `9101` | 网关监听端口 |
| `GATEWAY_MAX_ATTEMPTS` | `3` | 单次客户端请求最多的出口尝试次数；请求一旦开始转发字节就锁定，绝不重放 |
| `GATEWAY_RETRY_BACKOFF_MS` | `0` | 两次尝试之间的退避毫秒数 |
| `GATEWAY_ATTEMPT_TIMEOUT` | `2.0` | 单次隧道建连（含 SOCKS5 握手）超时秒数；调小可加快失败切换 |

**预算**：最坏耗时 ≈ `GATEWAY_MAX_ATTEMPTS × GATEWAY_ATTEMPT_TIMEOUT`。默认 `3 × 2.0 = 6s`，满足"单请求 < 6s"验收线。若网络 RTT 高，可下调 `GATEWAY_ATTEMPT_TIMEOUT` 到 `1.5`。

### 故障域隔离器 (`core/failover.py`)

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `FAILURE_DOMAIN_TRIP` | `any` | 触发整域隔离所需的"不可达监听"比例策略：`any`（任一节点确认 DEAD 即整域隔离）、`quorum`（≥ 比例阈值）、`all`（全部不可达才隔离） |
| `FAILURE_DOMAIN_QUORUM_RATIO` | `0.5` | `quorum` 策略下的比例阈值（0~1） |
| `FAILURE_DOMAIN_COOLDOWN_SEC` | `60` | 同一故障域两次隔离事件之间的去抖窗口，避免抖动期重复批量迁移 |

### EasyTier 秒级掉线检测

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `EASYTIER_MONITOR_ENABLED` | `1` | 是否启用 EasyTier peer 监视器（`0/false/no` 关闭） |
| `EASYTIER_SCAN_INTERVAL_SEC` | `1.0` | peer 表扫描间隔秒数 |
| `EASYTIER_PEER_MISS_SCRAPES` | `2` | 连续多少次扫描缺席才判定主机死亡（≈ `2 × 1s`，满足 < 5s 目标） |
| `EASYTIER_CLI` | `easytier-cli` | `easytier-cli` 可执行文件路径 |
| `EASYTIER_HOST_MAP` | — | 显式 mesh IP → 主机名映射，逗号分隔，如 `10.144.144.12=mesh-nl,10.144.144.13=mesh-sg`。只有**显式声明**或**曾在 peer 表出现过**的主机才会被判定掉线，避免把公网 IP / `127.0.0.1` 监听误判为 mesh 掉线；生产建议显式配置，以便网关重启后仍能在首个扫描周期发现已失联主机。右侧主机名应与节点的 `fault_domain`（未配置时即节点 `host`）一致；不一致时监视器会自动回退到该 IP 对应的实际故障域，不会漏判 |

**行为说明**：EasyTier 是全互联 P2P 网格，peer 直连消失即代表整机失联，监视器会在 1~2 个扫描周期内把该 host 的所有监听一次性判定为 DEAD，**并只产生一个域事件、一次批量重编排**。空的 peer 表（CLI 故障/尚未同步）不会触发任何隔离，避免误杀全池。


