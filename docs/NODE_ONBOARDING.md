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

### 策略组全景表（2026-11-10 全量梳理 · 10 组明确职责，无孤儿；MangoCF 已退役）

| 组 | 类型 | 用途（一句话） | 规则引用 | 来源/管理者 |
|---|---|---|---|---|
| PROXY | select | 主选择器（手动兜底出口） | 6 条（stun 等） | 订阅；pigeonw 家宽 20 节点已注入 |
| AUTO | url-test | 全节点自动选优（PROXY 的回退成员） | 0（作为成员被引用） | 订阅；pigeonw 已注入 |
| GLOBAL | select | mihomo 全局模式选择器 | 0（全局模式用） | 订阅 |
| 🏠家宽 | fallback | EdgeOne 大陆住宅出口要求站点（hybgzs/mulink） | 3 条 | 订阅+脚本（首选 `[自建] 🏠home-win-mesh`，已移除 `🏠home-win-CF`） |
| 🌏港澳台优选 | url-test | 港澳台特定 API（ai.muapi.cn 等全节点 403 规避） | 4 条 | 脚本 |
| 🎯Claude | select | Claude 专属防封分流 | 27 条 | 订阅+脚本 |
| 🎯Google | select | Google/Gmail/Gemini/YouTube | 3 条 RuleSet | 脚本 |
| 🎯OpenAI | select | OpenAI/ChatGPT 分流 | 5 条 | 订阅+脚本 |
| 🎯ZCode | select | ZCode/z.ai → 日本 AWS 固定出口（含 5 条进程规则） | 9 条 | 脚本 |
| GROK-REG | select | **Grok 注册流量隔离**（x.ai/grok.com），成员由 grok-register proxy_rotate 动态轮换（72 个） | 2 条（**2026-11-10 修复回归**：规则重建白名单漏了它，每次重组 x.ai/grok.com 分流被丢弃回落 PROXY） | 链式 groups/rules 文件（grok-register 管理） |

> **注（MangoCF 退役）**：2026-11-10 订阅源端已升级至 REV 14，彻底移除 `MangoCF` 策略组与 `mangoqwq.com` 专项路由规则（所有公开/Cloudflare 橙标子域统一回落 `MATCH,PROXY` 走优质代理出口）；Script.js 指纹已解耦为 `🏠家宽` + `🎯Claude` + `🎯OpenAI`。
> pigeonw 家宽 20 节点：不独立成组，注入 **PROXY / AUTO / 🎯Claude / 🎯Google / 🎯OpenAI 共 5 组**（US 住宅 IP 对美区服务组是优质出口）；中转固定 `dialer-proxy → [自建] 🇺🇸VLESS-AWS-Oregon-Reality`（单节点，不建组，缺失自动退直连）。
> **GROK-REG 不走脚本注入**：其成员由 `grok-register proxy_rotate` 整体重写（`_build_group_yaml`），家宽节点加入需配置到 grok-register 的 allowlist（独立集成）。
> **语义不符不注入**：🌏港澳台优选（地区）、🏠家宽（home-win 专用）、🎯ZCode（固定日本）、GLOBAL（组选择器）。

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

---

## 六、standalone 桥核变体（仅入池 · 不写 Clash Verge · 2026-11-12 teleapi 批次）

> **适用**：vendor 分享只要求入 TetherPool（业务经 `:9101` 网关消费），**明确不进本地 Clash Verge / tebi / pxed**。与 §二 的 Verge 注入型组在 manifest 中共存，互不影响；`check` 按组自动分流。

### 架构与文件

```
TetherPool 网关 :9101 (socks5 拨号, 零代码改动)
  └─ 127.0.0.1:11101–11239  ← ip-pool 自有 mihomo 桥核 (bin/mihomo, 每节点一个 socks listener)
       └─ http → brd.superproxy.io:44445 (Bright Data ISP, username 内嵌 -ip- 固定出口)
```

| 件 | 说明 |
|---|---|
| `bin/mihomo` | 自 Clash Verge 复制（`cp "/Applications/Clash Verge.app/Contents/MacOS/verge-mihomo" bin/mihomo`），gitignore 不入仓 |
| `config/teleapi-bridge.yaml` | 桥核配置：139 个 http proxy + 139 个 socks listener；**含 vendor 凭据，gitignore** |
| `config/teleapi-bridge/` | 桥核运行目录（cache.db） |
| `start.sh` + `scripts/bridge_keeper.sh` | keeper 守护的生命周期（崩溃 2s 重生；⚠️ keeper 必须可执行 `chmod +x`，否则 nohup 静默 exit 127）；与 api/gateway 同启停，PID `data/teleapi-bridge.pid` 记 keeper；启动尾自动 `POST /v1/probe?force=true` 清熔断债 |
| manifest `bridge_core` 字段 | `{type: standalone-mihomo, binary, config, home, log, pid, managed_by, note}`；`check` 据此分流 |

### 标准工具（standalone 组两阶段导入 · 2026-11-12 入仓）

```bash
# 阶段 1: 解析 vendor clash yaml → manifest + nodes.yaml + config/<group>-bridge.yaml (mihomo -t 校验)
python3 scripts/import_standalone.py import <订阅url|本地.yaml> --group NAME     [--region us-west] [--filter-type http] [--filter-server brd.superproxy.io] [--dry-run]
# 阶段 2: start.sh restart (keeper 拉桥核 + 自动 force probe) 后, 探活回写
python3 scripts/import_standalone.py realize --group NAME
```

- fault_domain 默认 `mac-<group>`（与 Verge 域隔离）；`--replace` 覆盖；节点级 region 细分（如 teleapi 的城市→国家码）在导入后按 manifest 记录调整。
- 老批次（teleapi）由一次性脚本导入，本工具为沉淀后的标准入口。

### 接入要点（teleapi 战果与差异）

1. **region 逐节点标注**：vendor 称「US-静态」但城市混杂（109 美国 / 30 非美）——按城市映射 `region`（美国→`us-west`，非美→sg/tw/jp/gb… 国家码），防污染 `region=us-west&type=clash_proxy` 路由；manifest nodes[] 增加 `region/city/vendor_name` 供 triage。
2. **check 分流**：`bridge_core.type=standalone-mihomo` 组 → L2 SKIP（无 Script.js 托管块）、L4 改验自有核 listener 在位（复用 `listening_ports()`）；L1/L3/L5 照常。L3 region 比对支持 manifest 节点级 `region` 覆盖组默认。
3. **realized_map 必须全量恒等**（死口也占位）：`realized_ports()` 按 idx 查 map，缺项 KeyError。
4. **vendor 域名策略**：Bright Data zone 对 google/x.com 域名封锁（000），Oregon 中转与直连**同败**——链路无关，勿浪费时间调 dialer；grok.com/chatgpt.com 200、claude.ai 403（可达口径 {200,403,405} 内）。
5. **中转杠杆**：Mac 直拨 brd 实测 200/0.9s 无跨 GFW RST，默认不挂 `dialer-proxy`（免 SPOF）；直拨劣化时 `import_standalone.py relay --group <g> --mode on` 在自有核给 proxies 接线 `dialer-proxy`（已实证不破坏 `-ip-` 固定出口，2026-11-12 BRD 限流事故激活）。中转节点定义不在桥核配置时，从 **`config/relay-nodes.local.yaml`**（gitignored，自建凭据仅本地，`relay_nodes:` 列表、clash proxy schema、`name` 精确匹配）读取注入；缺失时报错退出不猜测。`--mode off` 可逆。
6. **3 死口处置**：vendor 407/超时节点保持注册，断路器标记 degraded 自愈；`import_audit`（alive_first_probe/dead_ports）记入 manifest 供凭据轮换后 heal。
7. **listener 计数陷阱**：`netstat | grep '127.0.0.1\.111'` 前缀漏匹配 11200+ 段；一律用 `listening_ports()` 正则。

### 验证基线（2026-11-12）

`check` 全绿（pigeonw 五层无回归 + teleapi L1/L3/L4/L5 PASS）· 出口↔固定 IP 136/139 零漂移 · 网关 `region=us-west&type=clash_proxy` 粘性 ✓ · pytest 15/15

---

## 七、云端 mega 桥变体（huawei-vps 单核 3012 listener · 2026-11-14 gen 扩池批次）

> **适用**：云端实例（huawei-vps `/opt/ip-pool`）批量接入 http 上游。与 §六 teleapi 战例同一架构，规模验证到 **3012 http proxies + 3012 socks listeners 单 mihomo 进程**（RSS 71MB / fd 6026，2GB VPS 无压力）。gateway 仅 SOCKS5 拨号（忽略 node.protocol）→ **一切 http vendor 上游必须经 mihomo socks-bridge**。

| 件 | 值 |
|---|---|
| systemd 单元 | `ip-pool-huawei-bridge.service`（ExecStart=`bin/mihomo -d config/huawei-bridge -f config/huawei-bridge.yaml`；Restart=always RestartSec=2；**LimitNOFILE=65535 必须**；MemoryHigh=700M/MemoryMax=900M） |
| 桥核配置 | `config/huawei-bridge.yaml`（含 vendor 凭据，不入仓；`mihomo -t` 先验） |
| 端口规划 | pigeonw 12053–12072 · teleapi 11101–11239 · gen 21001–24873 |
| 节点命名 | teleapi `clash-{400+idx}-teleapi-{idx:03d}` · gen `clash-{700+idx}-gen-{idx:04d}` |

### gen 批次要点（普通/数据中心代理，随时可能批量失效）

1. **凭据逐 IP 归属**：共享代理源的账密是 **per-IP** 的——探测/生成配置时必须按行反查该 IP 自己的凭据对，拿别的行的会 400/407。
2. **region 收敛防污染**：us→`us-west`、eu→`eu-west`，fault_domain=huawei-gen-bridge 独立故障域；与 pigeonw/teleapi 路由互不可见。
3. **YAML 1.1 布尔陷阱**：国家码 `no`（挪威）会被 YAML 解析成 bool False → pydantic region 崩溃（api 起不来）。一律写成英文国名（`norway`）。
4. **大配置先 `mihomo -t`**：3012 节点全量配置一次过校验再重启；启动失败先查 yaml 陷阱再查端口占用。
5. **端口双绑定陷阱**：新桥 listener 段若与残留旧单元重叠（teleapi trial 单元曾占 11101/11102），mihomo **静默跳过**这两个 listener（不报错不退场）→ 旧单元退役后必须**重启新桥**补绑，并 curl 复测每个疑似端口。
6. **探活兜底**：5 分钟全池 probe + 熔断/半开自愈对 gen 池全效（2873 节点 probe 收敛 ~2min）；vendor 限流（探测时见过 429）会使部分节点间歇 degraded，熔断自动处理，无需人工。
7. **消费端定位**：gen 仅作池深兜底（`region=us-west/eu-west&type=clash_proxy` 可路由）；主力仍是 pigeonw/teleapi 住宅。
