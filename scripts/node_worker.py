#!/usr/bin/env python3
"""
node_worker.py - Deploys or expands WARP identities and native inbounds inside sing-box on a node.
Runs on the target node (via python3).
"""

import sys
import os
import json
import subprocess
import time
import base64
import datetime

WGCF_BIN = os.environ.get("WGCF_BIN", "/usr/local/bin/wgcf")
if not os.path.exists(WGCF_BIN) and os.path.exists("/root/warp-identities/bin/wgcf"):
    WGCF_BIN = "/root/warp-identities/bin/wgcf"

SING_BOX_CONFIG = os.environ.get("SING_BOX_CONFIG", "/etc/sing-box/config.json")
WORK_DIR = os.environ.get("WARP_WORK_DIR", "/root/warp-pool")


def run_cmd(cmd, check=True):
    res = subprocess.run(cmd, shell=isinstance(cmd, str), capture_output=True, text=True)
    if check and res.returncode != 0:
        raise RuntimeError(f"Command failed ({res.returncode}): {cmd}\nSTDOUT: {res.stdout}\nSTDERR: {res.stderr}")
    return res.stdout.strip()


def fetch_reserved(device_id, access_token):
    url = f"https://api.cloudflareclient.com/v0a5641/reg/{device_id}"
    cmd = [
        "curl", "-s", "--max-time", "15",
        "-H", "User-Agent: okhttp/3.12.1",
        "-H", f"Authorization: Bearer {access_token}",
        url
    ]
    stdout = run_cmd(cmd)
    try:
        data = json.loads(stdout)
        cid = data.get("config", {}).get("client_id")
        if not cid:
            # Check result.config.client_id
            cid = data.get("result", {}).get("config", {}).get("client_id")
        if cid:
            return list(base64.b64decode(cid))
    except Exception as e:
        print(f"[-] Warning: Failed to parse client_id for {device_id}: {e}\nRaw output: {stdout[:200]}")
    # Default fallback reserved
    return [0, 0, 0]


def generate_single_warp_identity(idx, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    acc_toml = os.path.join(out_dir, "wgcf-account.toml")
    profile_conf = os.path.join(out_dir, "wgcf-profile.conf")

    if not os.path.exists(acc_toml):
        run_cmd([WGCF_BIN, "register", "--accept-tos", "--config", acc_toml])
        time.sleep(1)

    if not os.path.exists(profile_conf):
        run_cmd([WGCF_BIN, "generate", "--config", acc_toml, "--profile", profile_conf])

    # Parse acc_toml
    device_id = ""
    access_token = ""
    private_key = ""
    with open(acc_toml, "r", encoding="utf-8") as f:
        for line in f:
            if "device_id =" in line:
                device_id = line.split("=", 1)[1].strip().strip("'\"")
            elif "access_token =" in line:
                access_token = line.split("=", 1)[1].strip().strip("'\"")
            elif "private_key =" in line:
                private_key = line.split("=", 1)[1].strip().strip("'\"")

    # Parse profile_conf
    addresses = []
    public_key = "bmXOC+F1FxEMF9dyiK2H5/1SUtzH0JuVo51h2wPfgyo="
    with open(profile_conf, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("Address"):
                val = line.split("=", 1)[1].strip()
                addresses = [x.strip() for x in val.split(",")]
            elif line.startswith("PublicKey"):
                public_key = line.split("=", 1)[1].strip()

    reserved = fetch_reserved(device_id, access_token)

    return {
        "tag": f"warp-ep{idx:02d}",
        "device_id": device_id,
        "private_key": private_key,
        "addresses": addresses,
        "peer_pub": public_key,
        "reserved": reserved,
        "v6": addresses[1] if len(addresses) > 1 else ""
    }


def ensure_endpoints(target_count=25):
    os.makedirs(WORK_DIR, exist_ok=True)
    endpoints_file = os.path.join(WORK_DIR, "endpoints.json")
    existing = []
    if os.path.exists(endpoints_file):
        try:
            with open(endpoints_file, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = []

    # Also check if /root/warp-identities has existing endpoints (e.g. on huawei-sg)
    if not existing and os.path.exists("/root/warp-identities/endpoints.json"):
        try:
            with open("/root/warp-identities/endpoints.json", "r", encoding="utf-8") as f:
                old_eps = json.load(f)
                for i, ep in enumerate(old_eps, 1):
                    existing.append({
                        "tag": f"warp-ep{i:02d}",
                        "device_id": ep.get("reg_id", ""),
                        "private_key": ep.get("private_key", ""),
                        "addresses": ep.get("addresses", []),
                        "peer_pub": ep.get("peer_pub", "bmXOC+F1FxEMF9dyiK2H5/1SUtzH0JuVo51h2wPfgyo="),
                        "reserved": ep.get("reserved", [0, 0, 0]),
                        "v6": ep.get("v6", "")
                    })
        except Exception as e:
            print(f"[-] Could not import old endpoints: {e}")

    print(f"[*] Found {len(existing)} existing WARP identities in {WORK_DIR}.")

    # Self-heal private_key and peer_pub if truncated
    changed = False
    for i, ep in enumerate(existing, 1):
        if not ep.get("private_key", "").endswith("="):
            acc_toml = os.path.join(WORK_DIR, f"id{i:02d}", "wgcf-account.toml")
            if os.path.exists(acc_toml):
                with open(acc_toml, "r", encoding="utf-8") as f:
                    for line in f:
                        if "private_key =" in line:
                            ep["private_key"] = line.split("=", 1)[1].strip().strip("'\"")
                            changed = True
        peer_pub = ep.get("peer_pub", "")
        if not peer_pub.endswith("="):
            ep["peer_pub"] = "bmXOC+F1FxEMF9dyiK2H5/1SUtzH0JuVo51h2wPfgyo="
            changed = True
    if changed:
        with open(endpoints_file, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)

    while len(existing) < target_count:
        idx = len(existing) + 1
        print(f"[*] Generating WARP identity #{idx}/{target_count}...")
        sub_dir = os.path.join(WORK_DIR, f"id{idx:02d}")
        try:
            acc_toml = os.path.join(sub_dir, "wgcf-account.toml")
            if os.path.exists(acc_toml) and os.path.getsize(acc_toml) == 0:
                os.remove(acc_toml)
            info = generate_single_warp_identity(idx, sub_dir)
            existing.append(info)
            with open(endpoints_file, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2)
            print(f"    -> Successfully generated #{idx}: v6={info['v6']}, reserved={info['reserved']}")
            time.sleep(3)
        except Exception as e:
            err_str = str(e)
            print(f"[-] Error generating #{idx}: {err_str[:120]}")
            if "429" in err_str:
                print("    [!] Rate limited by Cloudflare (429). Cooling down for 15s...")
                time.sleep(15)
            else:
                time.sleep(3)

    return existing[:target_count]


def update_sing_box_config(endpoints, mesh_ip, start_port=10881, has_native_v6=False):
    print(f"[*] Reading sing-box config from {SING_BOX_CONFIG}...")
    if not os.path.exists(SING_BOX_CONFIG):
        cfg = {"log": {"level": "info"}, "inbounds": [], "outbounds": [], "route": {"rules": []}}
    else:
        # Backup
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        bak_file = f"{SING_BOX_CONFIG}.bak_pool_{ts}"
        run_cmd(f"cp {SING_BOX_CONFIG} {bak_file}")
        print(f"[+] Backup saved: {bak_file}")
        with open(SING_BOX_CONFIG, "r", encoding="utf-8") as f:
            cfg = json.load(f)

    # 1. Clean previous pool inbounds, endpoints, route rules
    inbounds = [ib for ib in cfg.get("inbounds", []) if not ib.get("tag", "").startswith("socks-warp-") and not ib.get("tag", "").startswith("socks-native-") and not ib.get("tag", "").startswith("socks-in")]
    old_endpoints = [ep for ep in cfg.get("endpoints", []) if not ep.get("tag", "").startswith("warp-ep")]
    rules = [r for r in cfg.get("route", {}).get("rules", []) if not str(r.get("inbound", "")).startswith("socks-warp-") and not str(r.get("inbound", "")).startswith("socks-native-") and not str(r.get("inbound", "")).startswith("socks-in")]
    outbounds = [ob for ob in cfg.get("outbounds", []) if ob.get("tag") not in ["direct-v4", "direct-v6"]]

    # Ensure base outbounds
    direct_tags = [ob.get("tag") for ob in outbounds]
    if "direct" not in direct_tags:
        outbounds.append({"type": "direct", "tag": "direct"})

    # Ensure DNS
    cfg["dns"] = {
        "servers": [{"tag": "cf-dns", "type": "https", "server": "1.1.1.1"}],
        "strategy": "prefer_ipv6"
    }

    # 2. Add Native Inbounds
    inbounds.append({
        "type": "mixed",
        "tag": "socks-native-v4",
        "listen": mesh_ip,
        "listen_port": 10801
    })
    rules.append({"inbound": "socks-native-v4", "action": "resolve", "strategy": "prefer_ipv4"})
    rules.append({"inbound": "socks-native-v4", "outbound": "direct"})

    if has_native_v6:
        inbounds.append({
            "type": "mixed",
            "tag": "socks-native-v6",
            "listen": mesh_ip,
            "listen_port": 10802
        })
        rules.append({"inbound": "socks-native-v6", "action": "resolve", "strategy": "prefer_ipv6"})
        rules.append({"inbound": "socks-native-v6", "outbound": "direct"})

    # 3. Add WARP endpoints and inbounds
    new_endpoints = list(old_endpoints)
    for i, ep in enumerate(endpoints, 1):
        ep_tag = f"warp-ep{i:02d}"
        ib_tag = f"socks-warp-{i:02d}"
        port = start_port + i - 1
        if port >= 11010:
            # Skip 11010 to avoid conflicting with EasyTier mesh listener
            port += 1

        new_endpoints.append({
            "type": "wireguard",
            "tag": ep_tag,
            "private_key": ep["private_key"],
            "address": ep["addresses"],
            "peers": [
                {
                    "address": "162.159.192.1",
                    "port": 2408,
                    "public_key": ep.get("peer_pub", "bmXOC+F1FxEMF9dyiK2H5/1SUtzH0JuVo51h2wPfgyo="),
                    "allowed_ips": ["0.0.0.0/0", "::/0"],
                    "reserved": ep.get("reserved", [0, 0, 0])
                }
            ]
        })

        inbounds.append({
            "type": "mixed",
            "tag": ib_tag,
            "listen": mesh_ip,
            "listen_port": port
        })

        rules.append({"inbound": ib_tag, "outbound": ep_tag})

    if mesh_ip == "10.144.144.13":
        inbounds.append({
            "type": "mixed",
            "tag": "socks-warp-compat-10880",
            "listen": mesh_ip,
            "listen_port": 10880
        })
        rules.append({"inbound": "socks-warp-compat-10880", "outbound": "warp-ep01"})

    cfg["inbounds"] = inbounds
    cfg["endpoints"] = new_endpoints
    cfg["outbounds"] = outbounds
    if "route" not in cfg:
        cfg["route"] = {}
    cfg["route"]["rules"] = rules

    # Write temp and check syntax
    tmp_cfg = "/tmp/sing-box-check.json"
    with open(tmp_cfg, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)

    print("[*] Checking sing-box configuration syntax...")
    run_cmd(["sing-box", "check", "-c", tmp_cfg])
    print("[+] Syntax check passed!")

    # Atomic move
    with open(SING_BOX_CONFIG, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    os.remove(tmp_cfg)
    print(f"[+] Written new configuration to {SING_BOX_CONFIG}.")

    # Restart service
    print("[*] Restarting sing-box service...")
    run_cmd("systemctl restart sing-box")
    status = "unknown"
    for _ in range(8):
        time.sleep(1)
        status = run_cmd("systemctl is-active sing-box", check=False)
        if status == "active":
            break
    print(f"[+] sing-box status: {status}")
    if status != "active":
        raise RuntimeError(f"sing-box failed to activate! Status: {status}")


def test_ports(mesh_ip, start_port=10881, count=25, has_native_v6=False):
    print("\n[*] Testing live local inbounds...")
    # Test Native IPv4
    try:
        out = run_cmd(f"curl -s --max-time 5 -x socks5h://{mesh_ip}:10801 https://api.ipify.org")
        print(f"  [✓] Native IPv4 (:10801): {out}")
    except Exception as e:
        print(f"  [!] Native IPv4 (:10801) failed: {e}")

    if has_native_v6:
        try:
            out = run_cmd(f"curl -s --max-time 5 -x socks5h://{mesh_ip}:10802 https://api64.ipify.org")
            print(f"  [✓] Native IPv6 (:10802): {out}")
        except Exception as e:
            print(f"  [!] Native IPv6 (:10802) failed: {e}")

    # Test WARP endpoints
    success = 0
    for i in range(count):
        port = start_port + i
        try:
            out = run_cmd(f"curl -s --max-time 6 -x socks5h://{mesh_ip}:{port} https://api64.ipify.org")
            if out and ":" in out:
                success += 1
                if i < 3 or i == count - 1:
                    print(f"  [✓] WARP #{i+1:02d} (:{port}): {out}")
        except Exception as e:
            print(f"  [!] WARP #{i+1:02d} (:{port}) failed: {e}")

    print(f"\n[+] Health probe: {success}/{count} WARP IPv6 inbounds healthy.")


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <mesh_ip> <target_count> [start_port] [has_native_v6:0|1]")
        sys.exit(1)

    mesh_ip = sys.argv[1]
    target_count = int(sys.argv[2])
    start_port = int(sys.argv[3]) if len(sys.argv) > 3 else 10881
    has_native_v6 = bool(int(sys.argv[4])) if len(sys.argv) > 4 else False

    print(f"=== Starting WARP Pool Deployment on {mesh_ip} (Target: {target_count} WARP + Native) ===")
    endpoints = ensure_endpoints(target_count)
    update_sing_box_config(endpoints, mesh_ip, start_port, has_native_v6)
    test_ports(mesh_ip, start_port, target_count, has_native_v6)
    print("=== Deployment Complete ===")


if __name__ == "__main__":
    main()
