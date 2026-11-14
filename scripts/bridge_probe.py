#!/usr/bin/env python3
"""
scripts/bridge_probe.py - 刷新脚本共享的桥核出口探活 (二轮 review F9 抽出)

sub_pool_refresh.py 与 pigeonw_pool_refresh.py 此前各持一份 ~90% 相同的
probe_via_listeners: 起临时 mihomo 核, 为每个 proxy 开一个本地 socks listener,
经明文 ip-api 查询出口 IP。差异只有三处常量 —— 全部参数化:

  - PROBE_POOL   监听段起点 (sub 39001 / pigeonw 39101, 避免并发冲突)
  - PROBE_DIR    独立 datadir (共享 /tmp 曾有并发写坏 geo 缓存事故)
  - MODE / 控制器 两脚本各自的 mihomo 顶层配置偏好
  - MAX_WORKERS  探活线程数 (sub 50 / pigeonw 20)
"""

import json
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

MIHOMO_BIN = "/opt/ip-pool/bin/mihomo"


def probe_via_listeners(
    proxies,
    probe_pool: int,
    probe_dir: str,
    mode: str = "direct",
    external_controller: str = "127.0.0.1:39998",
    allow_lan: bool = None,
    max_workers: int = 50,
):
    """临时 mihomo 核 + 明文 ip-api 探活, 返回 [(idx, proxy, exit_ip, country_code)] (仅活节点)。

    country_code 是 ip-api 的两字母国家码 (US/DE/...), 查不到时为 None;
    gen 区域纠偏 (gen_region_fix) 依赖它, 旧调用方按位置取前三个不受影响。
    调用方传入互不重叠的 probe_pool / probe_dir, 两个刷新脚本即可并发运行。
    """
    import yaml
    listeners = [{"name": f"probe-{i:03d}", "type": "socks", "port": probe_pool + i,
                  "listen": "127.0.0.1", "proxy": p["name"]} for i, p in enumerate(proxies)]
    cfg = {"proxies": proxies, "listeners": listeners, "mode": mode,
           "log-level": "warning", "ipv6": False,
           "external-controller": external_controller}
    if allow_lan is not None:
        cfg["allow-lan"] = allow_lan
    pdir = Path(probe_dir)
    pdir.mkdir(exist_ok=True)
    tmp = pdir / "probe.yaml"
    fd = os.open(str(tmp), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False))
    proc = subprocess.run([MIHOMO_BIN, "-t", "-d", str(pdir), "-f", str(tmp)],
                          capture_output=True, text=True)
    if "successful" not in proc.stdout + proc.stderr:
        print("probe config invalid:", proc.stdout[-300:], proc.stderr[-300:], file=sys.stderr)
        return []
    core = subprocess.Popen([MIHOMO_BIN, "-d", str(pdir), "-f", str(tmp)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(4)

    def probe(i_p):
        i, p = i_p
        try:
            s = socket.create_connection(("127.0.0.1", probe_pool + i), timeout=10)
            s.sendall(b"\x05\x01\x00")
            if s.recv(2) != b"\x05\x00":
                return None
            ipb = bytes(int(x) for x in "208.95.112.1".split("."))
            s.sendall(b"\x05\x01\x00\x01" + ipb + b"\x00\x50")
            if s.recv(10)[1] != 0:
                return None
            s.sendall(b"GET /json HTTP/1.0\r\nHost: ip-api.com\r\n\r\n")
            s.settimeout(6)
            data = b""
            while True:
                c = s.recv(4096)
                if not c:
                    break
                data += c
                if len(data) > 8192:
                    break
            body = data.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in data else b""
            j = json.loads(body)
            if j.get("query"):
                return (i, p, j["query"], j.get("countryCode") or None)
        except Exception:
            return None
        return None

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            return [r for r in ex.map(probe, enumerate(proxies)) if r]
    finally:
        core.terminate()
        try:
            core.wait(timeout=5)
        except Exception:
            core.kill()
