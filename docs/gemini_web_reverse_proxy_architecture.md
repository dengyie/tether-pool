# Gemini 网页端（Web App）逆向反代、Imagen 3 / Veo 接口与 IP 池集成架构

## 一、概述与核心目标

Google Gemini 网页端（`gemini.google.com`）拥有极其强大的多模态原生能力：
1. **基础对话与深度思考**：支持 `Gemini 2.5 Flash / Pro`、`Gemini Advanced` 以及带有完整思考链（Thinking Process / Reasoning Tokens）的长上下文对话。
2. **Imagen 3 旗舰级生图**：支持直接在对话或专用生图指令中生成极高质量、无明显水印的图片。
3. **Veo 视频生成**：通过最新多模态生成管线生成动态高保真短视频。
4. **与全局 IP 池联动**：利用我们构建的跨国 100+ WARP IPv6 + 原生 IPv4/IPv6 出口池，实现多账号 CRC32 粘性隔离（单号单 IP 出站），防止因节点频繁变动或跨国切换触发 Google 风控降级或会话登出。

---

## 二、Google 网页端会话认证与生命周期机制

### 1. 核心 Cookie 组成
用户在浏览器登录 Google 账号后，`gemini.google.com` 依托以下两组核心认证 Cookie：
* **`__Secure-1PSID`**（长期会话凭据）：主要身份标识，通常有效期较长（数月）。
* **`__Secure-1PSIDTS`**（带时效的时间戳令牌）：高频变动令牌，通常数小时至 24 小时过期。

### 2. `RotateCookies` 无感自动续期保活
Google 提供了专门的 Cookie 轮转端点，可在会话即将过期时无感拉取最新的 `__Secure-1PSIDTS`：
* **端点**：`POST https://accounts.google.com/RotateCookies`
* **请求头**：
  * `Content-Type: application/json`
  * `Origin: https://accounts.google.com`
* **载荷（Payload）**：`[000,"-0000000000000000000"]`
* **响应**：Google 会在响应头中通过 `Set-Cookie` 返回新的 `__Secure-1PSIDTS`，反代网关将其写入本地会话持久层，实现长达数月的无人值守自动续期。

### 3. 会话上下文与 `SNlM0e` 提取
在每次与 Gemini 交互前，必须获取 Google 后端防重放与请求验签的 Access Token（即 `SNlM0e`）：
* **提取流程**：
  1. 使用已注入 `__Secure-1PSID` 与 `__Secure-1PSIDTS` 的客户端，发送 `GET https://gemini.google.com/app`；
  2. 从返回的 HTML 页面中通过正则匹配提取关键会话参数：
     * `SNlM0e`（Access Token）：`r'"SNlM0e":\s*"(.*?)"'`
     * `cfb2h`（Build Label 前端构建标签，作为请求参数 `bl`）：`r'"cfb2h":\s*"(.*?)"'`
     * `FdrFJe`（Session ID 前端会话 ID，作为请求参数 `f.sid`）：`r'"FdrFJe":\s*"(.*?)"'`
     * `TuX5cc`（Language 账号默认语言）：`r'"TuX5cc":\s*"(.*?)"'`
     * `qKIAYe`（Push ID 文件上传标识）：`r'"qKIAYe":\s*"(.*?)"'`

---

## 三、通信协议与 RPC 逆向细节

### 1. 端点与请求头
* **核心生成端点**：
  `POST https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate`
* **Query 参数**：
  * `bl`: 提取到的 `cfb2h`
  * `f.sid`: 提取到的 `FdrFJe`
  * `hl`: 语言（如 `en`）
  * `_reqid`: 自增请求计数器（从 `10000~99999` 起步，每轮 `+100000`）
  * `rt`: 固定为 `"c"`
* **请求头要求**：
  * `Content-Type: application/x-www-form-urlencoded;charset=utf-8`
  * `Origin: https://gemini.google.com`
  * `Referer: https://gemini.google.com/`
  * `X-Same-Domain: 1`
  * `x-goog-ext-525001261-jspb`: 模型选择头，例如：
    `[1,null,null,null,"<model_id>",null,null,0,[4,5,6,8],null,null,<capacity>,null,null,<model_num>]`

### 2. Payload（`f.req`）JSPB 结构
请求体为表单格式：`at=<SNlM0e>&f.req=<JSON_STRING>`
其中 `f.req` 的 JSON 结构为：
```json
[
  null,
  "[[[\"用户提示词\", 0, null, null, null, null, 0], [\"en\"], [\"\", \"\", \"\", null, null, null, null, null, null, \"\"], null, null, null, null, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 1]]"
]
```
内部数组是一个由 81 个槽位构成的稀疏数组（JSPB，JavaScript Protocol Buffers）：
* 槽位 `0`: 用户输入文本与附带文件数据
* 槽位 `1`: 客户端语言
* 槽位 `2`: 会话元数据（含 `conversation_id`, `reply_id`）
* 槽位 `7`: 流式标志位 `STREAMING_FLAG_INDEX = 1`
* 槽位 `79`: 模型选择序号
* 槽位 `80`: 思考程度（`2` 为 Extended Thinking 深度思考，`1` 为标准思考）

---

## 四、响应流式分帧与多模态解析

### 1. 流式分帧解析器（Streaming Frame Parser）
Google 返回的数据流是带有 UTF-16 长度前缀的 Chunk：
```
)]}'
1234
[["wrb.fr","assistant.lamda.BardFrontendService/StreamGenerate","[...json...]",...]]
```
* **防 XSSI 前缀**：首行移除 `)]}'`
* **长度计算**：每帧以 `<length in UTF-16 code units>\n` 开头，需要以 UTF-16 码元（Code Units，处理 Emoji 等代理对字符）为基准读取指定长度的 JSON 载荷。

### 2. 候选响应与内容提取路径
解析后的 Candidate 结构位于 `candidate_data`:
* **文本内容**：`candidate_data[1][0]`
* **思维链/思考过程（Thinking）**：`candidate_data[37][0][0]`
* **富媒体区域（Rich Content Block）**：`candidate_data[12]`
  * **Imagen 3 生成图片（Field 7）**：
    * 访问 `candidate_data[12][7][0]` 循环遍历；
    * 预览/生成图片 URL 位于 `[0][3][3]`；
    * 图片唯一 ID 位于 `[1][0]`；
    * 高清全尺寸图片下载 RPC：调用 `c8o8Fe` 获取最高分辨率原始尺寸。
  * **Veo 视频生成（Field 59）**：
    * 访问 `candidate_data[12][59][0][0][0]`；
    * 视频 URL 与缩略图列表位于 `[0][7]`（`urls[0]` 为封面缩略图，`urls[1]` 为完整 MP4 视频直链）。
  * **搜索网页图片（Field 1）**：`candidate_data[12][1]`，用于联网搜索附图。
  * **来源引用（Field 43）**：Grounded Search 引用索引。

### 3. Google CDN 防盗链与图片中转策略
Google 存放生成图片的域名 `lh3.googleusercontent.com` 开启了严格的 Referer 防盗链与跨域防护：
* 直接在前端使用 `<img>` 标签访问往往遭遇 `403 Forbidden`；
* **解决方案**：网关在解析到图片 URL 后，复用当前账号的 Proxy 会话在后台自动下载该图片二进制流，转换为标准的 `data:image/png;base64,...` 并包装为 OpenAI 标准的 `b64_json` 格式返回，或由网关本地缓存暴露自签名中转直链。

---

## 五、IP 池集成与粘性路由架构

Google 对 IP 漂移极其敏感（一旦单会话在 5 分钟内从东京 IP 突然跳到荷兰 IP，可能直接触发安全登出或强制短信验证）：

```
+-------------------------------------------------------------+
|                     gemini2api 网关                         |
|   (FastAPI OpenAI /v1/chat/completions & /v1/images)       |
+-------------------------------------------------------------+
                              |
       +----------------------+----------------------+
       | CRC32 Sticky Hashing (单账号绑定专属固定出站) |
       +----------------------+----------------------+
                              |
+-------------------------------------------------------------+
|                 全局 IP 池系统 (ip-pool:9100)               |
|      http://127.0.0.1:9100/v1/acquire?account=<google_id>    |
+-------------------------------------------------------------+
       |                      |                      |
+--------------+      +--------------+      +--------------+
| aws-tokyo    |      | mesh-nl      |      | huawei-sg    |
| 10.144.144.3 |      | 10.144.144.12|      | 10.144.144.13|
| :10881~10905 |      | :10881~10905 |      | :10881~10905 |
| (WARP IPv6)  |      | (WARP IPv6)  |      | (WARP IPv6)  |
+--------------+      +--------------+      +--------------+
```

1. **协议选择**：使用 `socks5h://`（远端代理 DNS 解析），避免宿主机无法直接解析 IPv6 端点。
2. **TLS 指纹**：采用 `curl_cffi` 强行模拟 `chrome124` / `chrome145`，通过 Google 的 TLS 握手特征校验。
3. **故障隔离**：当某个出口节点异常（探活失败或 429）时，IP 池自动标记节点下线并平滑将账号迁移至同一国家/地区的相邻 WARP 出口。
