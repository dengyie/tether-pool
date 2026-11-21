#!/usr/bin/env python3
"""warp_ep_redial.py — WARP 死会话 identity 轮换 (2026-11-19)。

背景 (cue-1 事故复盘认知升级):
  - CF 对同出口 IP 的并发 WARP 会话有上限 (~75-90/100, 有抖动), 超额会话
    「链路层假活 + 隧道内全超时」; 死会话 15min+ 不自愈 (零和分配)。
  - sing-box 1.14 无 per-endpoint 热重载 → 换 identity 必须 restart。
  - restart 会重摇全部会话骰子 (活的可能死, 死的可能活), 因此策略不是
    「重启救活」而是「每次 restart 前把已知死 identity 换成库里未分配的
    全新 device」—— 新注册 identity = 干净会话配额, 单调减少死 ep 数。

策略:
  1. 本机并发探针全部 warp 端口 (CF trace 断言 v6 出口)。
  2. dead 比例 < DEAD_MIN (默认 3) → 无事退出 (避免为 1-2 个抖动重启)。
  3. 对每个 dead ep: 从 /root/warp-pool/endpoints.json 里找未在 config
     中使用的 identity (未分配的 device_id), 替换 private_key/address/
     reserved/peer_pub; 备用不足则跳过该 ep。
  4. sing-box check 通过才写回 + restart; 失败保持原配置 (带 .bad 快照)。
  5. 冷却 COOLDOWN (默认 1800s) 防连轴重摇; mkdir 原子锁。
  6. 轮换历史追加 /root/warp-redial/history.jsonl (tag, old/new device)。

注意: 脚本不缩减 endpoint 数量 (那是容量决策), 只换身份。裁剪用
  trim 模式人工做 (config.json.bak-trim* 系列快照)。
"""
import base64
import datetime
import fcntl
import json
import os
import subprocess
import sys
import time

CONFIG = os.environ.get("SING_BOX_CONFIG", "/etc/sing-box/config.json")
POOL_FILE = os.environ.get("WARP_POOL_FILE", "/root/warp-pool/endpoints.json")
HIST_FILE = os.environ.get("WARP_REDIAL_HIST", "/root/warp-redial/history.jsonl")
LOCK_DIR = os.environ.get("WARP_REDIAL_LOCK", "/root/warp-redial")
TAGMAP_FILE = os.environ.get("WARP_TAGMAP", os.path.join(
    os.environ.get("WARP_REDIAL_LOCK", "/root/warp-redial"), "tagmap.json"))
DEAD_MIN = int(os.environ.get("WARP_DEAD_MIN", "3"))
COOLDOWN = int(os.environ.get("WARP_COOLDOWN", "1800"))
# restart 重摇全部会话: 21 轮实测 alive>=66 时期望净 -3.3/轮 (贴 CF 上限,
# 重摇必亏), alive<66 时 +1.8/轮。>= ALIVE_CEILING 不 swap (只探针观察)。
ALIVE_CEILING = int(os.environ.get("WARP_ALIVE_CEILING", "70"))
PROBE_HOST = os.environ.get("WARP_PROBE_HOST", "10.144.144.81")
PROBE_TIMEOUT = int(os.environ.get("WARP_PROBE_TIMEOUT", "6"))
DRYRUN = os.environ.get("WARP_DRYRUN", "0") == "1"

LOG = os.environ.get("WARP_REDIAL_LOG", "/var/log/warp-ep-redial.log")


def log(msg):
    line = "%s %s" % (datetime.datetime.now().strftime("%F %T"), msg)
    print(line)
    try:
        with open(LOG, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def acquire_lock():
    os.makedirs(LOCK_DIR, exist_ok=True)
    fd = open(os.path.join(LOCK_DIR, "lock"), "w")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except OSError:
        sys.exit(0)  # 上一个实例在跑


def cooldown_ok():
    last_f = os.path.join(LOCK_DIR, "last_restart")
    try:
        last = os.path.getmtime(last_f)
        if time.time() - last < COOLDOWN:
            log("SKIP cooldown %ds" % int(time.time() - last))
            return False
    except OSError:
        pass
    return True


def mark_restart():
    with open(os.path.join(LOCK_DIR, "last_restart"), "w") as f:
        f.write(str(time.time()))


def warp_ok(port):
    """探针: CF trace 且出口 v6 (2a09:bac 段) — 与池侧契约一致。"""
    try:
        out = subprocess.run(
            ["curl", "-s", "--max-time", str(PROBE_TIMEOUT),
             "-x", "socks5h://%s:%d" % (PROBE_HOST, port),
             "https://cloudflare.com/cdn-cgi/trace"],
            capture_output=True, text=True, timeout=PROBE_TIMEOUT + 2
        ).stdout
        return bool([l for l in out.splitlines() if l.startswith("ip=2a09:bac")])
    except Exception:
        return False


def main():
    acquire_lock()
    cfg = json.load(open(CONFIG))
    inbounds = {i["tag"]: i for i in cfg.get("inbounds", [])}
    endpoints = cfg.get("endpoints", [])

    # tag → port: socks-warp-NN ↔ warp-epNN (port = 10880 + NN)
    probes = {}
    for ep in endpoints:
        tag = ep.get("tag", "")  # warp-epNN
        nn = tag.replace("warp-ep", "")
        if not nn.isdigit():
            continue
        inb = inbounds.get("socks-warp-" + nn)
        if not inb:
            continue  # 已裁剪的 ep (inbound 没了) 跳过
        probes[ep["tag"]] = inb.get("listen_port")

    dead = [t for t, p in probes.items() if not warp_ok(p)]
    alive = len(probes) - len(dead)
    log("PROBE alive=%d dead=%d (%s)" % (alive, len(dead), ",".join(sorted(dead)[:10]) + ("..." if len(dead) > 10 else "")))

    if len(dead) < DEAD_MIN:
        log("OK dead<%d, skip" % DEAD_MIN)
        return

    if alive >= ALIVE_CEILING:
        log("OK alive=%d >= ceiling %d, restart expected-negative, skip" % (
            alive, ALIVE_CEILING))
        return

    # 净亏熔断: 上一轮 swap 后 alive 不升反降 → 贴 CF 配额上限, 连续 swap
    # 只会继续重摇随机死 (21:30 轮 61→56 实证)。熔断直到 alive 回升或人工
    # touch /root/warp-redial/resume。
    # 解除条件: 本轮探针 alive 已回到上轮 swap 前 (alive_before) 水平 →
    # 配额低谷已过去, 可以重试; 只看 alive_after 永远不会自动解除 (history
    # 只在 swap 成功后写, 熔断阻止 swap → 死锁, 2026-11-20 00:01 实证)。
    try:
        last = [json.loads(l) for l in open(HIST_FILE)][-1]
        if last["alive_after"] < last["alive_before"] and \
                not os.path.exists(os.path.join(LOCK_DIR, "resume")):
            if alive >= last["alive_before"]:
                log("CIRCUIT lifted: alive=%d recovered >= before=%d" % (
                    alive, last["alive_before"]))
            else:
                log("CIRCUIT last round net %d, skip until alive recovers "
                    "(now %d, before %d; touch %s/resume to override)" % (
                        last["alive_after"] - last["alive_before"], alive,
                        last["alive_before"], LOCK_DIR))
                return
    except (OSError, KeyError, ValueError):
        pass

    if not cooldown_ok():
        return

    # identity 库: 找未在 config 中的 device_id
    # sing-box schema 不认 device_id 字段, 无法写进 config; 改用持久映射
    # 文件 tagmap.json (tag -> device_id) 记录轮换结果, 初始值按 tag 编号
    # 对齐 identity 库 (tag NN = 库序号 NN, 1-based)。
    pool = json.load(open(POOL_FILE))
    pool_by_id = {i["device_id"]: i for i in pool if i.get("device_id")}
    dev_by_tag = {}
    for ep in endpoints:
        m = ep.get("tag", "").replace("warp-ep", "")
        if m.isdigit() and 1 <= int(m) <= len(pool):
            dev = pool[int(m) - 1].get("device_id")
            if dev:
                dev_by_tag[ep["tag"]] = dev
    try:
        dev_by_tag.update(json.load(open(TAGMAP_FILE)))
    except (OSError, ValueError):
        pass
    used_devices = set(dev_by_tag.values())
    # 黑名单: history.jsonl 出现过的 old/new device 一律不复用 —
    # f5d30d25 案例 (2026-11-19): freed 死 identity 会重新漂进 spare 被第三次
    # 分配 (ep04→ep16→ep12), 每次 swap 都白费一个名额; 死过的 device 不自愈。
    banned = set()
    try:
        for line in open(HIST_FILE):
            try:
                e = json.loads(line)
                banned.add(e.get("old"))
                banned.add(e.get("new"))
            except ValueError:
                continue
    except OSError:
        pass
    banned.discard(None)
    spare = [i for i in pool if i.get("device_id") not in used_devices
             and i.get("device_id") not in banned and i.get("device_id")]
    log("spare identities: %d (used %d, banned %d)" % (
        len(spare), len(used_devices), len(banned)))

    cfg_by_tag = {ep["tag"]: ep for ep in endpoints}
    swaps = []
    for tag in dead:
        if not spare:
            log("WARN no spare identity left; %s kept dead" % tag)
            continue
        new = spare.pop(0)
        old_dev = dev_by_tag.get(tag, "?")
        swaps.append((tag, old_dev, new))

    if not swaps:
        log("OK nothing to swap (no spare)")
        return 0

    if DRYRUN:
        for tag, old_dev, new in swaps:
            log("DRYRUN %s: %s -> %s" % (tag, old_dev[:8], new["device_id"][:8]))
        return 0

    # 应用替换: endpoint 换身份; address 数组也换 (新 identity 的双栈地址)。
    # device_id 不写入 config (sing-box schema 不认), 记 tagmap。
    tagmap_new = {}
    for tag, old_dev, new in swaps:
        ep = cfg_by_tag[tag]
        ep["private_key"] = new["private_key"]
        ep["address"] = new["addresses"]
        tagmap_new[tag] = new["device_id"]
        for peer in ep.get("peers", []):
            peer["public_key"] = new.get("peer_pub", peer.get("public_key"))
            peer["reserved"] = new.get("reserved", peer.get("reserved"))
        log("SWAP %s: %s -> %s (v6=%s)" % (tag, old_dev[:8], new["device_id"][:8], new.get("v6", "")[:24]))

    # check 门禁
    tmp = CONFIG + ".redial-tmp"
    with open(tmp, "w") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)
    chk = subprocess.run(["sing-box", "check", "-c", tmp], capture_output=True, text=True)
    if chk.returncode != 0:
        os.replace(tmp, CONFIG + ".bad-" + datetime.datetime.now().strftime("%H%M%S"))
        log("ERROR check failed: %s" % chk.stderr[:200])
        return 1

    with open(CONFIG, "rb") as f:
        config_before = f.read()
    try:
        with open(TAGMAP_FILE, "rb") as f:
            tagmap_before = f.read()
    except FileNotFoundError:
        tagmap_before = None

    def restore_file(path, content):
        if content is None:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
            return
        restore_tmp = path + ".redial-restore"
        with open(restore_tmp, "wb") as f:
            f.write(content)
        os.replace(restore_tmp, path)

    os.replace(tmp, CONFIG)
    dev_by_tag.update(tagmap_new)
    tagmap_tmp = TAGMAP_FILE + ".redial-tmp"
    with open(tagmap_tmp, "w") as f:
        json.dump(dev_by_tag, f, indent=1)
    os.replace(tagmap_tmp, TAGMAP_FILE)

    restart = subprocess.run(["systemctl", "restart", "sing-box"], check=False)
    active = subprocess.run(["systemctl", "is-active", "--quiet", "sing-box"], check=False)
    if restart.returncode != 0 or active.returncode != 0:
        restore_file(CONFIG, config_before)
        restore_file(TAGMAP_FILE, tagmap_before)
        subprocess.run(["systemctl", "restart", "sing-box"], check=False)
        log("ERROR restart failed (restart=%d active=%d); config and tagmap restored" % (
            restart.returncode, active.returncode))
        return 1

    time.sleep(20)
    ok2 = sum(1 for t, p in probes.items() if warp_ok(p))
    log("RESTART done: alive %d -> %d (swapped %d)" % (alive, ok2, len(swaps)))
    if ok2 < alive:
        restore_file(CONFIG, config_before)
        restore_file(TAGMAP_FILE, tagmap_before)
        restored = subprocess.run(["systemctl", "restart", "sing-box"], check=False)
        if restored.returncode != 0:
            log("ERROR probe regressed and config restored, but rollback restart failed")
        log("ERROR post-restart probes regressed (%d < %d); config and tagmap restored" % (
            ok2, alive))
        return 1

    mark_restart()
    os.makedirs(os.path.dirname(HIST_FILE), exist_ok=True)
    with open(HIST_FILE, "a") as f:
        for tag, old_dev, new in swaps:
            f.write(json.dumps({
                "ts": datetime.datetime.now().isoformat(),
                "tag": tag, "old": old_dev, "new": new["device_id"],
                "alive_before": alive, "alive_after": ok2,
            }) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
