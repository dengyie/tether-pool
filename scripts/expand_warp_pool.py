#!/usr/bin/env python3
"""expand_warp_pool.py — 经代理扩 WARP identity 库 (2026-11-19)。

背景: cue 宿主机直连 api.cloudflareclient.com 被 GFW SNI 阻断 (TCP 通、
TLS ClientHello 即 RST), wgcf 二进制不支持 socks 代理 → 纯 Python
x25519 本地生成密钥对 + curl --proxy 经本机 WARP ep01 (10881) 走 API。

注册响应是顶层对象 (非 {result:}), 解析路径与 node_worker.fetch_reserved
一致。成功条目增量追加 endpoints.json (crash-safe: 每注册一个落盘一次)。

用法: python3 expand_warp_pool.py [target_total]   # 默认 150
"""
import base64
import datetime
import json
import subprocess
import sys
import time

from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
from cryptography.hazmat.primitives import serialization

POOL_FILE = "/root/warp-pool/endpoints.json"
PROXY = "socks5h://10.144.144.81:10881"   # ep01; 死了换 10881+ 的活端口
API = "https://api.cloudflareclient.com/v0a5641/reg"
UA = "okhttp/3.12.1"
PEER_PUB = "bmXOC+F1FxEMF9dyiK2H5/1SUtzH0JuVo51h2wPfgyo="


def api(method, path, token=None, body=None):
    cmd = ["curl", "-s", "--max-time", "20", "-x", PROXY, "-X", method,
           "-H", "User-Agent: " + UA, "-H", "Content-Type: application/json"]
    if token:
        cmd += ["-H", "Authorization: Bearer " + token]
    if body:
        cmd += ["-d", body]
    out = subprocess.run(cmd + [API + path], capture_output=True, text=True).stdout
    return json.loads(out) if out.strip() else {}


def gen_one(idx):
    priv = X25519PrivateKey.generate()
    priv_b64 = base64.b64encode(priv.private_bytes(
        serialization.Encoding.Raw, serialization.PrivateFormat.Raw,
        serialization.NoEncryption())).decode()
    pub_b64 = base64.b64encode(priv.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw)).decode()

    body = json.dumps({
        "tos": datetime.datetime.now(datetime.timezone.utc)
               .strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "type": "wireguard", "locale": "en_US",
        "install_id": "", "fcm_token": "", "key": pub_b64})
    d = api("POST", "", body=body)
    if d.get("errors") or "id" not in d:
        raise RuntimeError("register: " + json.dumps(d)[:150])
    dev_id, token = d["id"], d["token"]
    if not token:
        raise RuntimeError("register ok but no token")

    cfg = api("GET", "/" + dev_id, token=token)
    config = cfg.get("config") or cfg
    cid = config.get("client_id")
    reserved = list(base64.b64decode(cid)) if cid else [0, 0, 0]
    iface_addr = ((config.get("interface") or {}).get("addresses")
                  or {})  # {"v4": "172.16.0.2", "v6": "2606:..."}
    v4 = iface_addr.get("v4") or ""
    v6 = iface_addr.get("v6") or ""
    # CF API 返回裸地址; sing-box endpoint address 需要 CIDR 前缀 (netip.ParsePrefix)
    if v6 and "/" not in v6:
        v6 += "/128"
    v4 = v4 if v4 and "/" in v4 else (f"{v4}/32" if v4 else "")

    addresses = [x for x in (v4 or "172.16.0.2/32",
                             v6 if v6 else "") if x]
    return {
        "tag": f"warp-ep{idx:03d}",
        "device_id": dev_id,
        "private_key": priv_b64,
        "addresses": addresses,
        "peer_pub": PEER_PUB,
        "reserved": reserved,
        "v6": v6,
        "access_token": token,   # 备用: 以后可查/删该 device
    }


def main():
    target = int(sys.argv[1]) if len(sys.argv) > 1 else 150
    pool = json.load(open(POOL_FILE))
    print("pool:", len(pool), "-> target:", target)
    fails = 0
    while len(pool) < target and fails < 10:
        idx = len(pool) + 1
        try:
            info = gen_one(idx)
            pool.append(info)
            with open(POOL_FILE, "w") as f:
                json.dump(pool, f, indent=2)
            print(f"[+] #{idx}: {info['device_id'][:8]} v6={info['v6'][:28]}")
            fails = 0
            time.sleep(4)
        except Exception as e:
            fails += 1
            print(f"[-] #{idx}: {str(e)[:120]} (fail {fails}/10)")
            time.sleep(60 if "429" in str(e) or "Expecting" in str(e) else 5)
    print("DONE pool:", len(pool), "spare:", len(pool) - 77)


if __name__ == "__main__":
    main()
