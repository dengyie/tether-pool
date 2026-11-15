#!/usr/bin/env python3
"""gen 节点区域标签纠偏 (R1 存量 + R3 卫生, 2026-11-14 需求单)。

背景: gen 批次 (clash-7xx-gen-xxxx, 端段 21001-24873) 注册时 region 硬编码
us-west, 实际 ~50% 出口在 DE/CA —— 标签失真击穿 region_strict 与 auto_rehome。

三个动作:
  R1  geo 采样全部 gen 节点 (经桥核 gen listener 查 ip-api countryCode), 把
      region/provider_region 校正为真实国家码 (us->us-west, 其余小写 ISO),
      tags 加 geo-fixed; 同步重写 config/nodes.yaml (upsert ON CONFLICT 无条件
      覆盖 region —— 只改 DB 的话 api 重启即回滚)。
  R2  不在本脚本: 探活事务持续保真 (core/storage.update_probe_result, CF loc)。
  R3  退役账号 (ag_rt2 / ag_dengyietech_bfb0c5) 的 allocation/lease 清理 +
      gen tags 补 source=huawei-bridge (需 --apply; 已获需求方授权)。

命名映射 (生产实测): 桥核 listener gen-NNNN 端口 21000+NNNN -> proxy GEN-NNNN;
DB 节点名 clash-7xx-gen-XXXX 的 XXXX == NNNN (clash-701-gen-0001 = port 21001)。

用法 (生产 /opt/ip-pool):
  .venv/bin/python scripts/gen_region_fix.py                # 默认 dry-run 只打印
  .venv/bin/python scripts/gen_region_fix.py --apply        # 写 DB + nodes.yaml
幂等: 无漂移的节点不写; --apply 前自动 sqlite 在线备份到 backups/。
"""
import argparse
import json
import re
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path("/opt/ip-pool")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).parent))

DB = ROOT / "data" / "ip_pool.db"
NODES_YAML = ROOT / "config" / "nodes.yaml"
BACKUP_DIR = ROOT / "backups"
GEN_BASE_PORT = 21001          # 桥核 gen 端段 21001-24873
PROBE_POOL = 39201             # 临时探活监听段 (sub 39001 / pigeonw 39101 不重叠)
PROBE_DIR = "/tmp/mihomo-probe-genfix"
RETIRED_ACCOUNTS = ["ag_rt2", "ag_dengyietech_bfb0c5"]  # R3 已授权

from core.storage import Storage, region_from_country  # noqa: E402


def load_gen_nodes(conn):
    rows = conn.execute(
        "SELECT name, region, provider_region, tags, exit_ip FROM nodes"
    ).fetchall()
    out = []
    for r in rows:
        tags = json.loads(r["tags"] or "[]")
        if "genproxy" in tags:
            out.append({"name": r["name"], "region": r["region"],
                        "provider_region": r["provider_region"],
                        "tags": tags, "exit_ip": r["exit_ip"]})
    return out


def geo_sample():
    """经桥核 gen listener 逐节点查 ip-api, 返回 {node_name: country_code}。
    采样失败的节点不在返回里 (调用方保持原标签, 宁缺勿错)。"""
    import yaml
    cfg_path = ROOT / "config" / "huawei-bridge.yaml"
    main = yaml.safe_load(cfg_path.read_text())
    proxies_by_name = {p["name"]: p for p in main.get("proxies", [])}
    gen_listeners = [l for l in main.get("listeners", [])
                     if re.match(r"^gen-\d+$", str(l.get("name", "")))]
    if not gen_listeners:
        print("[GEN_FIX_FAIL] no gen-* listeners in bridge config", file=sys.stderr)
        return {}

    from bridge_probe import probe_via_listeners
    # probe_via_listeners 按传入 proxies 顺序开 probe_pool+i; 每 listener 的
    # proxy 字段指回 GEN-NNNN, 采样结果经顺序 idx + 槽位号映射回 DB 节点名。
    proxies, slot_by_idx = [], []
    for l in gen_listeners:
        p = proxies_by_name.get(l.get("proxy"))
        if not p:
            continue
        slot_by_idx.append(int(l["name"].split("-")[1]))
        proxies.append(p)

    results = probe_via_listeners(proxies, probe_pool=PROBE_POOL, probe_dir=PROBE_DIR,
                                  mode="direct", external_controller="127.0.0.1:39998",
                                  max_workers=50)
    out = {}
    for idx, _p, _ip, cc in results:
        slot = slot_by_idx[idx]
        node_name = "clash-%d-gen-%04d" % (700 + slot, slot)
        out[node_name] = cc
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="写 DB + nodes.yaml (默认 dry-run)")
    args = ap.parse_args()

    if not DB.exists():
        print("[GEN_FIX_FAIL] DB not found: %s" % DB, file=sys.stderr)
        return 1

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    gen_nodes = load_gen_nodes(conn)
    print("gen nodes in DB: %d" % len(gen_nodes))

    # ---- R1: geo 采样 -> 变更计划 ----
    print("geo sampling via bridge listeners (may take a few minutes)...")
    cc_by_name = geo_sample()
    print("geo sampled: %d/%d" % (len(cc_by_name), len(gen_nodes)))

    changes = []
    for n in gen_nodes:
        cc = cc_by_name.get(n["name"])
        if cc is None:
            continue  # 采样失败 -> 保持原标签 (宁缺勿错; R2 探活会持续补正)
        target = region_from_country(cc)
        if target and target != n["region"]:
            changes.append({**n, "target": target})

    print("\n== R1 变更计划 (%d 个节点) ==" % len(changes))
    from collections import Counter
    print("  目标分布:", dict(Counter(c["target"] for c in changes)))
    for c in changes[:20]:
        print("  %s: %s -> %s (exit=%s)" % (c["name"], c["region"], c["target"], c["exit_ip"] or "?"))
    if len(changes) > 20:
        print("  ... and %d more" % (len(changes) - 20))

    # ---- R3: 退役账号现状 ----
    r3_rows = []
    for acct in RETIRED_ACCOUNTS:
        allocs = conn.execute(
            "SELECT service, node_name FROM account_allocations WHERE account_id = ?", (acct,)
        ).fetchall()
        leases = conn.execute(
            "SELECT node_name FROM leases WHERE account_id = ?", (acct,)
        ).fetchall()
        r3_rows.append((acct, [dict(r) for r in allocs], [r["node_name"] for r in leases]))
        print("\n== R3 账号 %s ==" % acct)
        for r in allocs:
            print("  allocation: service=%s node=%s" % (r["service"], r["node_name"]))
        if leases:
            print("  legacy leases: %s" % ", ".join(r["node_name"] for r in leases))
        if not allocs and not leases:
            print("  (no allocation/lease rows — already clean)")

    pending = bool(changes) or any(a or l for _, a, l in r3_rows)
    if not args.apply:
        print("\n[dry-run] 无写入。加 --apply 执行 (先自动备份)。")
        return 0
    if not pending:
        print("\n[apply] 无待执行变更 (已全部到位)。")
        return 0

    # ---- 备份 (sqlite 在线 backup API, WAL 安全) ----
    BACKUP_DIR.mkdir(exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    src = sqlite3.connect(str(DB))
    dst = sqlite3.connect(str(BACKUP_DIR / ("ip_pool.genfix-%s.db" % stamp)))
    src.backup(dst)
    dst.close(); src.close()
    print("DB backed up -> backups/ip_pool.genfix-%s.db" % stamp)
    if NODES_YAML.exists():
        (BACKUP_DIR / ("nodes.yaml.genfix-%s" % stamp)).write_bytes(NODES_YAML.read_bytes())
        print("nodes.yaml backed up -> backups/nodes.yaml.genfix-%s" % stamp)

    # ---- 执行 R1: storage.update_node 逐节点 (allowed_fields 含 region/tags/provider_region) ----
    storage = Storage(str(DB))
    fixed = 0
    for c in changes:
        new_tags = list(c["tags"]) + [t for t in ("geo-fixed", "source=huawei-bridge") if t not in c["tags"]]
        r = storage.update_node(c["name"], {
            "region": c["target"],
            "provider_region": c["target"],
            "tags": new_tags,
        })
        if r:
            fixed += 1
        else:
            print("[GEN_FIX_WARN] update_node miss: %s" % c["name"], file=sys.stderr)
    print("R1 applied: %d nodes region-corrected" % fixed)

    # 未被采样到/无漂移的 gen 节点也补 source tag (幂等; 采样不到的不改 region)
    for n in gen_nodes:
        if "source=huawei-bridge" not in n["tags"]:
            storage.update_node(n["name"], {"tags": n["tags"] + ["source=huawei-bridge"]})

    # ---- 执行 R3: 退役账号清理 (delete_allocation 同事务清 legacy lease 镜像) ----
    for acct, allocs, leases in r3_rows:
        if not allocs and not leases:
            continue
        svc = allocs[0]["service"] if allocs else "default"
        ok = storage.delete_allocation(svc, acct)
        print("R3 released %s (service=%s): %s" % (acct, svc, "ok" if ok else "no rows"))

    # ---- nodes.yaml 同步 (防重启回滚: upsert ON CONFLICT 无条件覆盖 region) ----
    import yaml
    chk = sqlite3.connect(str(DB)); chk.row_factory = sqlite3.Row
    final = {r["name"]: (r["region"], json.loads(r["tags"] or "[]"))
             for r in chk.execute("SELECT name, region, tags FROM nodes")}
    chk.close()
    data = yaml.safe_load(NODES_YAML.read_text())
    nl = data if isinstance(data, list) else data.get("nodes")
    y_fixed = 0
    for item in nl:
        if "genproxy" not in (item.get("tags") or []):
            continue
        name = item.get("name", "")
        db_region, db_tags = final.get(name, (item.get("region"), item.get("tags") or []))
        if item.get("region") != db_region:
            item["region"] = db_region
            item["provider_region"] = db_region
            y_fixed += 1
        item["tags"] = db_tags
    if isinstance(data, dict):
        data["nodes"] = nl
    from atomic_yaml import atomic_write_text
    atomic_write_text(NODES_YAML, yaml.safe_dump(data, allow_unicode=True, sort_keys=False))
    print("nodes.yaml synced: %d entries region-updated" % y_fixed)

    conn.close()
    print("\n[GEN_FIX_OK] done. 建议重启 api 使内存态刷新: systemctl restart ip-pool-api")
    return 0


if __name__ == "__main__":
    sys.exit(main())
