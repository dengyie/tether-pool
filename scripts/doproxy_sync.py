#!/usr/bin/env python3
"""doproxy_sync.py - DoProxy 活跃节点 API → Clash yaml 适配器 (SUB6)。

DoProxy (http://198.200.42.250:8888, openapi /api/v1/proxy/*) 只有 text/uri
导出格式, 无 Clash yaml 输出; 而 sub_pool_refresh.py 的 fetch_and_parse 只吃
Clash yaml。本适配器把 API 的 JSON 拉下来转成标准 Clash proxies 列表, 落盘
本地 yaml (urllib 原生支持 file://), sub_pool_refresh 以
SUB6_URL=file:///opt/ip-pool/data/sub6/doproxy.yaml 消费。

筛选口径 (2026-11-19 用户需求「需要中继节点才能连到的家宽节点」):
  status=active + ip_type in (residential, business) —— 家宽/商宽/移动出口,
  此类上游多屏蔽中国大陆直连, 池侧统一经 SUB6_RELAY=SKYWARD-01 中继拨号。

设计要点:
- http/socks5 透传 (mihomo 原生支持; socks4 不支持会导致 probe yaml 整体校验
  失败, 必须在适配器侧过滤);
- 节点名带国家码/类型/序号, 与 detect_region 兼容 (国家前缀 "US." 可被
  KEYWORD_TO_REGION 识别); 名字稳定 (按 host:port 排序) 避免每次刷新漂移;
- 原子落盘 (tmp + os.replace), fetch_and_parse 并发读者不会拿到半截 yaml;
- API 全挂时保留上次 yaml (fail-open: sub_pool_refresh 拿旧数据继续跑)。
"""
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

import yaml

DOPROXY_BASE = os.environ.get("DOPROXY_BASE", "http://198.200.42.250:8888")
OUT = Path(os.environ.get("DOPROXY_OUT", "/opt/ip-pool/data/sub6/doproxy.yaml"))
IP_TYPES = ("residential", "business")   # 家宽 + 商宽; datacenter 不进 SUB6
PAGE_SIZE = 100
TIMEOUT = 30


def fetch_api(ip_type: str) -> list[dict]:
    """拉一页 active 列表; 分页守卫: total 超过单页时告警 (截断可见化, 不静默丢)。"""
    url = (f"{DOPROXY_BASE}/api/v1/proxy/list?status=active"
           f"&ip_type={ip_type}&page_size={PAGE_SIZE}&page=1")
    req = urllib.request.Request(url, headers={"User-Agent": "ip-pool-doproxy-sync/1.0"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        d = json.loads(r.read().decode("utf-8", "replace"))
    if d.get("code") != 200:
        raise RuntimeError(f"DoProxy API code={d.get('code')}")
    rows = d.get("data") or []
    total = d.get("total") or 0
    if total > len(rows):
        print(f"[DOPROXY_SYNC_WARN] ip_type={ip_type} truncated: total={total} "
              f"> returned={len(rows)}, 池扩容后需翻页支持", file=sys.stderr)
    return rows


def to_clash_proxy(x: dict, idx: int, ip_type: str) -> dict:
    ptype = str(x.get("protocol") or "http").lower()
    # mihomo 无 socks4 支持 (probe config invalid: unsupport proxy type: socks4,
    # 一个坏节点让整份 probe yaml 校验失败 → alive 0/195), 直接过滤
    if ptype not in ("http", "socks5"):
        return {}
    try:
        server, port = str(x["host"]).strip(), int(x["port"])
    except (KeyError, TypeError, ValueError):
        print(f"[DOPROXY_SYNC_WARN] bad row skipped (host/port 脏值): {x!r}", file=sys.stderr)
        return {}
    if not server or port <= 1:
        print(f"[DOPROXY_SYNC_WARN] bad row skipped (空 host 或 port<=1): {x!r}",
              file=sys.stderr)
        return {}
    cc = str(x.get("country") or "UN")
    name = f"{cc}.{x.get('country_name') or '未知'}-{ip_type}-{ptype}-{idx:03d}"
    p = {"name": name, "type": ptype, "server": server, "port": port,
         "udp": ptype == "socks5"}
    if x.get("username"):
        p["username"] = str(x["username"])
    if x.get("password"):
        p["password"] = str(x["password"])
    return p


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    proxies, seen = [], set()
    for ip_type in IP_TYPES:
        try:
            rows = fetch_api(ip_type)
        except Exception as e:
            # 单类型失败不拖垮另一类型; 两类全挂时保留旧 yaml (fail-open)
            print(f"[DOPROXY_SYNC_WARN] ip_type={ip_type} fetch fail: {e}", file=sys.stderr)
            continue
        # host:port 稳定排序 → 名字序号不随 API 返回序漂移
        rows.sort(key=lambda x: (x.get("host") or "", int(x.get("port") or 0)))
        for x in rows:
            key = (x.get("host"), x.get("port"))
            if key in seen:
                continue
            seen.add(key)
            p = to_clash_proxy(x, len(proxies) + 1, ip_type)
            if p:
                proxies.append(p)
    if not proxies:
        print("[DOPROXY_SYNC_FAIL] zero proxies from API, keep previous yaml",
              file=sys.stderr)
        return 1
    doc = {
        # file:// 缓存头: sub_pool_refresh 的 normalize_source_proxies 会剥 s6- 前缀,
        # 这里名字保持语义化即可。
        "proxies": proxies,
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    tmp = OUT.with_suffix(".tmp")
    tmp.write_text(yaml.dump(doc, allow_unicode=True, sort_keys=False, width=200),
                   encoding="utf-8")
    # 凭据可能随节点携带 (DoProxy 未来带 username/password): 600 与 secrets 惯例对齐
    os.chmod(tmp, 0o600)
    os.replace(tmp, OUT)
    print(f"[DOPROXY_SYNC_OK] wrote {len(proxies)} proxies -> {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
