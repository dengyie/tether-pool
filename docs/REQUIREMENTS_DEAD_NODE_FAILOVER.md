# 需求：死节点有界重试与自动换节点（Dead-Node Failover & Rehome）

> **状态**：待 TetherPool 项目迭代实现
> **提出方**：grok2api 运维（2026-11-07 线上事故复盘）
> **优先级**：P0（当前线上持续产生失败请求）
> **范围**：`gateway/server.py` 请求链路 + `core/router.py` 分配/重编排 + 下游绑定契约

---

## 一、触发事故（可作为验收回归用例）

**2026-11-07 04:19 / 05:07，grok2api 生产环境视频任务批量失败。**

| 项目 | 事实 |
| :--- | :--- |
| 故障节点 | `mesh-nl`（LeaseWeb 荷兰，EasyTier `10.144.144.12`）**整机离线** |
| 影响面 | 该机器上的 **25 个 WARP IPv6 出口**（`10.144.144.12:10881~10905`）**同时全部不可达** |
| 下游绑定 | grok2api 有 **511 个账号**的 `egress_node_id` 静态指向这 25 个节点（id 92–116，每节点 21 个） |
| EasyTier 事实 | `easytier-cli peer` 中 `10.144.144.12` **已完全消失**，其余 20+ 节点（aws-tokyo/huawei-sg/india-vps/huawei-vps…）全部 p2p 0% 丢包 |
| 请求表现 | 每次请求先尝试 mesh SOCKS（10s 超时）+ 回落直连（10s 超时）= **单次 20.6s**，随后失败；`attempt_count=1` 直接终态 |
| 池内健康度 | 同期 aws-tokyo 25/25、huawei-sg 25/25、india 25/25 均 healthy，**池里有大量可用出口，但没有一个请求被换过去** |

**关键结论**：这不是"池里没有可用节点"，而是**死节点的失败没有被传递到分配层**——下游拿到的绑定永不失效，只能一直撞死节点。

---

## 二、现状能力盘点（避免重复实现）

现有实现已具备以下能力，**本需求不要求重做**：

| 能力 | 位置 | 现状 |
| :--- | :--- | :--- |
| 四态断路器（HEALTHY/DEGRADED/DEAD/HALF_OPEN） | `core/circuit.py` | 已实现；**连续 2 次失败** → DEAD |
| 指数退避 + 抖动 | `core/circuit.py:compute_cooldown` | 60s 起步，封顶 1800s，含 0–10s 抖动 |
| 请求期连接失败反馈到断路器 | `gateway/server.py:_open_upstream` | 已实现：失败即 `update_probe_result(success=False)` |
| 熔断到 DEAD 后自动重编排租约 | `gateway/server.py:_open_upstream` → `router.rehome_leases_for_node` | **已实现**（仅在该节点自身状态跃迁到 DEAD 时触发） |
| 换出口重试 | `gateway/server.py:_open_upstream` | **已有，但硬编码 `for attempt in range(2)`，即总尝试次数固定为 2** |
| 手动重编排 / 熔断重置 / 业务事件上报 | `POST /v1/circuit/rehome`、`/v1/circuit/reset`、`/v1/battery/event` | 已实现 |
| 批量分配 | `POST /v1/allocation/batch` | 已实现 |

**缺口（本需求要解决的）**：

1. 重试次数写死为 2，不可配置；耗尽后只抛通用 502。
2. 重试排除只作用于"本次请求"（`exclude_node`），**账号级绑定不变**，下一次请求继续撞同一个死节点。
3. **没有故障域概念**：同宿主 25 个出口被当作 25 个独立节点，各自独立熔断、各自触发一次 rehome，无法识别"整台机器没了"。
4. **被动检测太慢**：整机掉线后仍要走 HTTP 探测超时（配置 15s）才可能累计失败；EasyTier peer 消失这一"秒级可知"的信号没有被使用。
5. **与下游绑定脱节**：TetherPool 只重编排自己的租约表，grok2api 侧是静态导入的 `egress_nodes` 外键（一次性 `GET /v1/export/grok2api` 落地），**没有任何机制通知它"你的绑定已失效"**。

---

## 三、需求清单

### R1（P0）请求级：可配置的最大重试次数

**行为**：`gateway/server.py:_open_upstream` 的尝试次数改为可配置，替换现有硬编码 `range(2)`。

| 参数 | 类型 | 默认 | 说明 |
| :--- | :--- | :--- | :--- |
| `GATEWAY_MAX_ATTEMPTS` | int | 3 | 单个客户端请求在网关内部最多尝试的出口数 |
| `GATEWAY_RETRY_BACKOFF_MS` | int | 0 | 换节点重试前的短暂退避（默认不等待，避免叠加 EDT） |

**约束**：
- 每次尝试**必须**使用不同节点（已有 `exclude_node` 机制，需扩展为累积排除集合，而不是只排除上一个）。
- 已 DEAD / 冷却中的节点不得进入尝试序列。
- 若本次客户端请求是 CONNECT 建立阶段失败（尚未向目标发送任何字节），换节点是安全的；若已开始双向转发，**不得**重试（避免重复提交）——沿用现有"upstream socket 归属"边界。

**验收**：把 `GATEWAY_MAX_ATTEMPTS` 设为 1/2/3，构造一个死节点 + 两个健康节点，断言实际尝试次数等于配置值且节点互不重复。

---

### R2（P0）耗尽后：把账号绑定迁到健康节点（不只是本次请求）

**行为**：重试预算耗尽后，除了返回错误，必须**主动重编排该账号的持久绑定**，使后续请求直接落到健康节点。

**要点**：
- 复用 `router.rehome_leases_for_node` 的语义，但触发条件要扩展：**请求期重试耗尽**也视为"该节点对账号不可用"，不能只在断路器跃迁 DEAD 时触发。
- 重编排需保持粘性约束：优先同区域、同 `egress_type`、同 `tier`；找不到同域时允许跨域，但要在响应/审计中标注 `cross_region_rebind=true`。
- 重编排必须幂等，且**按账号去重**（同一账号并发 5 个请求同时耗尽，只允许一次迁移）。

**验收**：模拟 mesh-nl 场景（25 个出口全死），对绑定的账号发请求，断言首次请求返回失败后，第二次请求的出口 IP 已在 aws-tokyo/huawei-sg/india 中，且 `egress_type` 仍为 `warp_ipv6`。

---

### R3（P0）故障域：同宿主/同 ASN 批量隔离

**行为**：节点模型增加**故障域标识**（至少 `host` / `provider_region` 两级），一次宿主级故障要把其下所有出口一次性隔离、一次性批量 rehome。

**理由（本事故的核心）**：`mesh-nl` 的 25 个出口端口是同一台机器上的 25 个 sing-box 实例。现有实现会：
- 25 个节点各自累计 2 次失败 → 25 次独立 DEAD 跃迁；
- 每跃迁一次触发一次 `rehome_leases_for_node`；
- 期间 511 个账号仍在反复请求 → 每次 20.6s。

**要求**：
- 同一 `host` 下的任一节点出现"连接层不可达"（非目标站点返回的业务错误），达到阈值后，**同一故障域内全部节点立即进入 DEAD**（可配置 `failure_domain_trip=any|quorum|all`）。
- 每个故障域一次扫描只做**一次**批量 rehome，而不是每节点一次。
- 故障域恢复时按 `half_open` 逐个试探，不要一次全放回（避免抖动放大）。

**验收**：给 25 个节点标注同一 `host=mesh-nl`，杀死该 host 的全部端口，断言：DEAD 跃迁只产生 1 条故障域级事件，rehome 只执行 1 次，且 511 个账号全部迁出。

---

### R4（P1）整机离线快速判定（不等 15s HTTP 探测）

**行为**：引入"网络层不可达"的快速信号，优先于业务探测超时。

**要求**：
- `mesh-nl` 这类 EasyTier 成员，应通过 `easytier-cli peer`（或 RPC `rpc_portal`）判断成员是否还在线；成员消失即视为该 host 全部出口不可用，**直接 DEAD，不再逐个走 SOCKS 握手超时**。
- 对非 mesh 节点，使用 TCP 连接超时（建议 2–3s）而不是完整 HTTP 探测，作为一级快速判定；HTTP 业务探测作为二级。

**验收**：模拟 peer 掉线，断言从掉线到该 host 全部出口被标记不可用的时间 **< 5s**（现状为 15s×探测周期）。

---

### R5（P0）下游绑定契约：让 grok2api 能感知"绑定已失效"

**行为**：给出一个明确的、低成本的失联通知机制。三选一（建议 a+b 组合）：

**方案 a（推荐，零改造优先）：网关零配置路由**
- 下游只配置一个固定入口：`http://<account>:tetherpool@10.144.144.9:9101`
- 账号 → 出口的映射**每次请求由网关实时解析**，绑定漂移对下游完全透明。
- 这是 `docs/INTEGRATION_GUIDE.md` 最初设计的模式，**需要保证网关侧解析失败时不会返回静态死 IP**。

**方案 b：绑定版本号 + 轻量对账接口**
- 新增 `GET /v1/bindings/epoch?service=grok2api`，返回全局单调递增的 `epoch` 及变更摘要。
- 下游按 epoch 变化拉取 `GET /v1/export/grok2api?since_epoch=N` 做增量对账；无需 webhook。

**方案 c：webhook 推送**
- `POST /v1/hooks/register` 注册回调，重编排发生时推送 `{account_id, old_node, new_node, reason, at}`。

**要求**：至少实现 a 或 b 之一；c 作为可选增强。**明确禁止**"一次性静态导入后不再对账"作为受支持的集成方式——这正是本次事故的根因。

**验收**：kill 一个 host，下游不做任何主动轮询，其后续请求在 R2 生效后 1 个请求周期内落到健康出口。

---

### R6（P1）耗尽后的错误语义

**行为**：重试耗尽返回可被下游区分的错误码，而不是笼统 502。

| 场景 | 建议 | 下游动作 |
| :--- | :--- | :--- |
| 池内所有候选节点均不可用 | `503` + `X-TetherPool-Error: pool_exhausted` | 下游整体降载/推迟 |
| 该账号绑定节点已失效、重编排成功但本次请求仍失败 | `502` + `X-TetherPool-Error: binding_rehomed, retry=1` | 下游**立刻重试一次**（新绑定已生效） |
| 账号绑定节点已失效、无健康节点可迁 | `503` + `X-TetherPool-Error: no_healthy_target` | 下游停止该账号，告警 |

**验收**：三种场景分别构造，断言响应码与 header 与表一致。

---

### R7（P2）可观测性

新增指标（Prometheus，沿用现有 `/metrics`）：

| 指标 | 类型 | 说明 |
| :--- | :--- | :--- |
| `ip_pool_gateway_attempts_per_request` | histogram | 每请求实际尝试出口数 |
| `ip_pool_retry_exhausted_total{service,reason}` | counter | 重试耗尽次数 |
| `ip_pool_rehome_total{service,trigger}` | counter | 换绑次数，`trigger` ∈ `circuit_dead`,`retry_exhausted`,`manual` |
| `ip_pool_domain_trip_total{domain}` | counter | 故障域级隔离次数 |
| `ip_pool_host_unreachable_seconds{host}` | gauge | 整机不可达持续时长 |

告警建议：单一 `host` 下 >50% 出口 DEAD 持续 60s → 告警。

---

## 四、数据模型变更建议

`ProxyNode` 增加：

```python
host: str = ""            # 故障域一级标识，如 "mesh-nl"、"aws-tokyo"
provider_region: str = "" # 故障域二级标识，如 "leaseweb-nl"
self_reported_at: Optional[datetime]  # 节点侧最近一次存活上报（如 EasyTier peer 心跳）
```

`Lease` / allocation 增加：

```python
rebind_count: int = 0     # 该账号累计被重编排次数，用于识别"烫手账号"
last_rebind_reason: str = ""
epoch: int                # 绑定版本，配合 R5(b) 增量对账
```

存储层需保证 `rehome_leases_for_node` 与 `epoch` 自增在同一事务内，避免下游读到半迁移状态。

---

## 五、非目标（Out of Scope）

- 不要求 TetherPool 负责账号侧风控判断（429/401 语义仍由下游 `grok2api` 判定，通过 `/v1/battery/event` 上报）。
- 不要求 TetherPool 做请求内容级重试（HTTP 幂等性由业务系统负责）。
- 不改变现有质量分级（premium/standard）与注册防冷却语义。
- 本次不做跨供应商（不同 ASN）自动扩容采购。

---

## 六、整体验收（以本次事故为回归基准）

复现 2026-11-07 mesh-nl 场景：

1. 池内 25 个出口标注 `host=mesh-nl`，全部端口关闭；其余 75 个出口健康。
2. 511 个账号按现有比例绑定到 mesh-nl 的 25 个出口。
3. 以 20 QPS 发起请求，持续 5 分钟。

**通过标准**：

| 指标 | 现状（事故时） | 目标 |
| :--- | :--- | :--- |
| 单请求最大耗时 | 20.6s（两次 10s 拨号超时） | **< 6s**（3 次尝试 × ≤2s 快速判定） |
| mesh-nl 全部出口被判定不可用耗时 | >15s 且逐节点 | **< 5s，1 次故障域事件** |
| 首轮请求成功率 | 绑定组 0% | **> 95%**（重试 + 重编排后） |
| 后续请求成功率 | 0%（绑定不变，持续撞死节点） | **100%**（已迁至健康出口） |
| 下游需人工介入 | 是（需手动重绑 511 账号） | **否** |

---

## 七、交接说明

- 该需求由 **TetherPool 项目**实现；grok2api 侧只负责"请求失败后按错误语义重试一次"，不再承担出口健康管理。
- grok2api 当前缺陷（拨号期超时被误判为"已提交"从而拒绝换账号）由 grok2api 自行修复，**与本需求解耦**，但两者叠加才能覆盖今天的事故。
- 事故证据：`request_audit_attempts.error_chain_json`（`socks connect tcp 10.144.144.12:10900 → dial tcp → i/o timeout`）、grok2api `media_jobs.egress_node_id` 分布、`easytier-cli peer` 输出。
