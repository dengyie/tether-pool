# TetherPool × 3x-ui 节点接入流水线 (Node Onboarding Pipeline)

> 唯一入口: `scripts/node_pipeline.py` · 事实源: `config/node_groups.yaml`
> 本文固化 2026-11-10 pigeonw 美国家宽 20 节点接入全过程沉淀的架构与教训。

---

## 一、两项目职责边界

| 项目 | 角色 | 管什么 | 变更入口 |
|---|---|---|---|
| **3x-ui / Clash Verge** | **数据面桥接层** | 节点注入 (Script.js)、mihomo 桥接 listener、中转链 (dialer-proxy)、客户端订阅分发 | Script.js 托管块 + Merge.yaml + Verge 重新激活 |
| **TetherPool (ip-pool)** | **池化调度层** | nodes.yaml 注册、探活/熔断、粘性分配、参数化网关 :9101 | nodes.yaml + start.sh restart |

两项目靠 **`config/node_groups.yaml` (manifest)** 对齐 —— 它是唯一事实源，两边都从它生成/校验。

> ⚠️ **凭据策略**：manifest 含 vendor 账密，已被 `.gitignore`（仓库 remote 是公网 GitHub）。
> 仓库内只保留脱敏模板 `config/node_groups.example.yaml`；新环境初始化 = 复制模板填真实凭据。
> `check`/`heal` 等命令在 manifest 缺失时会明确报错并提示初始化方法。

## 二、标准接入流程 (`import`)

```
node_pipeline.py import <订阅url|文件> --group X --region us-west \
    [--relay-group "🇺🇸中转组名" --relay-candidates 节点1 节点2] [--restart-verge]
```

1. **拉订阅** → 解析 proxies
2. **预测活**：并发直连 vendor 测出口 IP（死节点剔除）
3. **声明端口段**：取 manifest 已用段之上的空闲段
4. **写 Script.js 托管块**（`// >>> NODE-PIPELINE:<group>:v1` 定界，幂等替换）
5. **写 Merge.yaml listeners**（底座重建保险，见教训 #1）
6. **写 nodes.yaml**（`relay_mode: direct_only`，教训 #3）
7. **重载**：Verge 重新激活（人工点击 / `--restart-verge` 自动重启应用）→ `start.sh restart`（教训 #2）
8. **实现端口映射**：并发探测 listener 出口 IP ↔ vendor server IP 匹配，回写 manifest
9. **验证门**：`verify --group X` 全绿才算完成

## 三、一致性校验 (`check`) 与实测 (`verify`)

```bash
node_pipeline.py check                    # 五方一致性 (L1-L5, 健康度仅 WARN)
node_pipeline.py verify --group pigeonw   # 出口IP 20/20 + 目标矩阵 + 网关绑定一致/粘性
node_pipeline.py verify --group pigeonw --load 20   # 加并发压测 (workers 上限 60)
node_pipeline.py heal --group pigeonw --yes         # 漂移自动修复(先备份; 含 Script.js 缺失时自动恢复最新备份)
node_pipeline.py heal --group pigeonw --realize --yes  # 出口↔端口映射漂移时: 实测回写 realized_map + nodes.yaml
```

组命名规则: `--group` 必须匹配 `^[a-z][a-z0-9_]*$`（同时用作 JS 标识符 / YAML key / 文件名片段）。

`verify` 出口 IP 断言失败时，会直接打印 `heal --realize` 修复指引。

`check` 五层（L1–L5 任一 FAIL → 退出码 2）:
- L1 manifest 完备性
- L2 manifest ↔ Script.js 托管块（**全字段**对比，凭据轮换即报 DRIFT）
- L3 manifest ↔ nodes.yaml（端口/relay_mode/region）
- L4 manifest ↔ mihomo 活体（节点存在、dialer-proxy（仅中转候选存活时要求）、两组、listener 在位）
- L5 manifest ↔ TetherPool（注册/端口一致性）
- 附带 **L5 探活健康度**：仅 WARN 不影响退出码（vendor 瞬时抖动属自愈范畴，不与一致性混淆）

### 策略组全景表（2026-11-10 全量梳理 · 每个组都有明确职责，无孤儿）

| 组 | 类型 | 用途（一句话） | 规则引用 | 来源/管理者 |
|---|---|---|---|---|
| PROXY | select | 主选择器（手动兜底出口） | 6 条（stun 等） | 订阅；pigeonw 家宽 20 节点已注入 |
| AUTO | url-test | 全节点自动选优（PROXY/MangoCF 的回退成员） | 0（作为成员被引用） | 订阅；pigeonw 已注入 |
| GLOBAL | select | mihomo 全局模式选择器 | 0（全局模式用） | 订阅 |
| MangoCF | fallback | **自家域名 mangoqwq.com 直连兜底**（直连→AUTO→PROXY；面板/frp/doh/check-api 等自建服务） | 1 条（suffix mangoqwq.com，**53 hits 最活跃自建规则**） | 订阅；同时是死节点黑名单的**订阅指纹**（Script.js dropDeadNodes 门控），不可改名 |
| 🏠家宽 | fallback | EdgeOne 大陆住宅出口要求站点（hygbzs/mulink） | 3 条 | 订阅+脚本 |
| 🌏港澳台优选 | url-test | 港澳台特定 API（ai.muapi.cn 等全节点 403 规避） | 4 条 | 脚本 |
| 🎯Claude | select | Claude 专属防封分流 | 27 条 | 订阅+脚本 |
| 🎯Google | select | Google/Gmail/Gemini/YouTube | 3 条 RuleSet | 脚本 |
| 🎯OpenAI | select | OpenAI/ChatGPT 分流 | 5 条 | 订阅+脚本 |
| 🎯ZCode | select | ZCode/z.ai → 日本 AWS 固定出口（含 5 条进程规则） | 9 条 | 脚本 |
| GROK-REG | select | **Grok 注册流量隔离**（x.ai/grok.com），成员由 grok-register proxy_rotate 动态轮换（72 个） | 2 条（**2026-11-10 修复回归**：规则重建白名单漏了它，每次重组 x.ai/grok.com 分流被丢弃回落 PROXY） | 链式 groups/rules 文件（grok-register 管理） |

> pigeonw 家宽 20 节点：不独立成组，注入 PROXY/AUTO；中转固定 `dialer-proxy → [自建] 🇺🇸VLESS-AWS-Oregon-Reality`。

## 四、教训目录（每条都有当日事故实证）

1. **Verge 冻结底座**：listener 的真实现驻留在 root 侧 service 配置，Verge 重组以其为底、按**目标代理**去重合并 Merge listeners。同名/同代理的 Merge 条目是零副作用的**重建保险**（底座丢失时下次重新激活自动重建）；异名同代理会被去重丢弃。
2. **网关无热重载**：`gateway/server.py` 注册表是进程启动快照，`/v1/config/reload` 只管控制面 (9100)。动过 nodes.yaml 必须 `start.sh restart`。
3. **环回桥必须 `relay_mode: direct_only`**：否则 `resolve_relay_hop` 会把兄弟桥当中继跳板（级联里 127.0.0.1 语义错乱），且失败污染探活记录（假 dead）。
4. **网关拨号预算**：`GATEWAY_ATTEMPT_TIMEOUT` 默认 2.0s，AWS 中转链 CONNECT 建立需 1.5–4s → start.sh 已提到 8s。新增更长链路时按需调整。
5. **dash 格式用户名表达不了 `us-west`**（按 `-` 切分）——业务侧用 query 格式 `acc?region=us-west&type=clash_proxy`，通过代码设 Proxy-Authorization 没问题；**curl 命令行必须用 `-U` 传凭据**，不能把 `?` 写进 `-x` URL（会被当 query 截断 → fake-ip）。
6. **跨GFW 明文 CONNECT 会被 RST**：中国直拨 vendor HTTP 代理的 TLS 在部分链路必挂 → 一律走 `dialer-proxy` 经 AWS 美国机加密中转（新增美国类节点默认挂中转组）。
7. **凭据轮换路径**：商家换账密 → 更新 manifest `nodes[]` → `heal --group X --yes`（heal 检测到 L2 全字段漂移会**自动**重建 Script.js 托管块，`--force-js` 仅为强制语义）→ Verge 重新激活。
8. **mihomo 对代理字段强校验**：注入节点必须带 `type`（生成器已净化输出，manifest 节点缺 type 会补 `group.protocol`）。

## 五、本次 pigeonw 组终态速查

| 项 | 值 |
|---|---|
| 订阅 | nodes.pigeonw.com/s/<token>/clash.yaml（固定 yaml，账密在 manifest） |
| mihomo 节点 | 🇺🇸PIGEONW-家宽-01…20（http + Basic Auth，udp:false） |
| 中转 | 🇺🇸PIGEONW中转 fallback：AWS-Oregon → TUIC/AnyTLS-googlevps |
| 桥接 | 127.0.0.1:**11053–11072**（冻结底座 tether-socks-053…072） |
| TetherPool | clash-052…071-pigeonw-01…20 · us-west · standard · direct_only |
| manifest 关键字段 | `tether_seq_start: 52`（注册名序号基）、`bridge_ports.realized_map`（可选：级联漂移后的 per-node 端口映射，优先于 realized 区间） |
| 单测 | `tests/test_node_pipeline.py` 15 用例（gen↔parse 往返 / upsert 幂等与坏状态规范化 / 命名补零 / 端口规划 / 漂移判定 / realize 回写 / Script.js 缺失自动恢复） |
| 网关路由 | `region=us-west&type=clash_proxy` |
| 验证基线 | check 5/5 PASS · 出口 IP 20/20 · google/x/claude 20/20 · 60 并发 60/60 · 网关绑定一致+粘性 ✓ · pytest 11/11 |
