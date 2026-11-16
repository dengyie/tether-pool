#!/usr/bin/env python3
"""CPA xai credentials -> ip-pool gateway proxy_url batch migration.
Snapshotted: /personal/cpa/backups/auths.pre-ippool-20261116-193412
Rollback: restore proxy_url from snapshot or delete field (fallback to global 7897).
"""
import json, os, sys, time, glob

PW = os.environ["GATEWAY_PASSWORD"]
LIVE = "/root/.cli-proxy-api"
DRY = "--dry" in sys.argv
files = sorted(glob.glob(f"{LIVE}/xai-*.json"))
ok = err = skip = 0
for i, f in enumerate(files):
    try:
        with open(f) as fh:
            d = json.load(fh)
    except Exception as e:
        print(f"READ_FAIL {f}: {e}"); err += 1; continue
    email = d.get("email")
    if not email:
        print(f"NO_EMAIL {f}"); skip += 1; continue
    acc = "xai-" + email.replace("@", "%40")   # URL-safe account id
    url = f"http://{acc}__type_warp_ipv6__service_cpa:{PW}@127.0.0.1:9101"
    if d.get("proxy_url") == url:
        skip += 1
        continue
    if DRY:
        ok += 1
        continue
    d["proxy_url"] = url
    tmp = f + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(d, fh, ensure_ascii=False)
    os.chmod(tmp, 0o600)
    os.replace(tmp, f)
    ok += 1
    if ok % 200 == 0:
        print(f"progress {ok}/{len(files)}", flush=True)
        time.sleep(1)   # watcher 呼吸窗口，避免 1580 个 CREATE 事件瞬间打满
print(f"DONE ok={ok} skip={skip} err={err} total={len(files)} dry={DRY}")

# 2026-11-17 archive note: kept in-repo as the GATEWAY_PASSWORD rotation runbook.
# Rotation = set new password in /etc/ip-pool-secrets.env (huawei-vps), restart
# ip-pool-gateway, re-run this script against tebi with GATEWAY_PASSWORD=<new>.
# Restore path unchanged (see docstring); --dry first.
