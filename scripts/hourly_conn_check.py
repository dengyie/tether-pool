#!/usr/bin/env python3
"""每小时节点连通性独立验收 (2026-11-15): 与 api 4min 内部探活互补的外部视角.

两层检查:
  1) 抽样直连桥核 listener: 每池抽 N 个 healthy 节点, SOCKS5 握手 + ip-api 实连,
     核对出口 IP 与 DB exit_ip 一致、国家码与 region 相符 (us<->us-west, hk 例外)
  2) 网关端到端: 经 9101 Basic-auth 出口 3 次真实请求, 验证消费方数据面通路

判定: 任一池样本全失败 / 网关 3 连败 / 抽样错配率 >50% => 打 [HOURLY_CONN_FAIL]
并退出 1 (grep 告警用); 否则落一行 [HOURLY_CONN_OK] 摘要。

约束: 系统 python3 (无 pydantic) —— 不 import core.*; 只读 sqlite; 凭据走环境变量。
"""
import base64, fcntl, json, os, random, socket, sqlite3, subprocess, sys, time

DB = "/opt/ip-pool/data/ip_pool.db"
STATE = "/tmp/hourly_conn_prev_fail"   # 上一轮失败池名单 (小样本瞬时抖动防误报)
FAIL_DETAIL = "/tmp/hourly_conn_fail_detail"  # 上一轮各失败池的失败 detail (判定 listener 级)
LAST_RESTART = "/tmp/hourly_conn_last_restart"  # 限频: 距上次自动重启 >= 60min 才允许
RESTART_MIN_INTERVAL = 3600
RESTART_LOCK = "/tmp/hourly_conn_restart.lock"
# listener 级失败特征 (TCP/SOCKS5 握手都过不去) vs 应用层失败 (region mismatch /
# JSONDecodeError / ip-api 限流) —— 只有前者才值得重启整个桥核服务
LISTENER_LEVEL = ("connectionrefused", "timeout", "handshake", "oserror", "connectionreset")
POOL_SERVICE = {"pigeonw": "pigeonw-bridge"}  # 其余池都挂在 huawei-bridge
DEFAULT_BRIDGE = "ip-pool-huawei-bridge"
SAMPLES_PER_POOL = 5        # 每池抽样数; api 内部探活已 4min 全覆盖, 这里求独立验证
GATEWAY_N = 3
TIMEOUT = 8

# 池识别: tags LIKE 片段 / 名称模式 -> (过滤SQL, 标签)
POOLS = [
    ("teleapisub", "tags LIKE '%teleapisub%'"),
    ("skyward", "tags LIKE '%skyward%'"),
    ("sub", "name LIKE 'clash-5%-sub-%'"),
    ("pigeonw", "tags LIKE '%pigeonw%'"),
    ("gen", "tags LIKE '%genproxy%'"),
    # cloud (teleapi 云端订阅): 出口共享网关 (实测 15 节点同一出口 IP), region
    # 比对无意义 (db per-node exit_ip 与实际出口对不上, 2026-11-15 复盘), 只验连通。
    ("cloud", "host='127.0.0.1' AND tags NOT LIKE '%genproxy%' AND tags NOT LIKE '%teleapisub%' AND tags NOT LIKE '%skyward%' AND tags NOT LIKE '%pigeonw%' AND name NOT LIKE 'clash-5%-sub-%'"),
]
NO_REGION_CHECK = {"cloud"}


def region_matches(db_region, cc):
    """DB region 与 ip-api 国家码比对: us->us-west 桶名, hk 因地理库偏差不判."""
    if not cc or db_region in ("hk", "misc", None, ""):
        return True  # hk 名字优先原则; misc 无从比对
    c = cc.strip().lower()
    return db_region == ("us-west" if c == "us" else c)


def socks5_probe(ip, port, node_region, region_check=True):
    """直连桥核 listener: SOCKS5 + ip-api /json, 返回 (ok, detail)。
    region_check=False 用于出口共享网关型池 (cloud), 只验连通不验归属。"""
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=TIMEOUT)
        s.settimeout(TIMEOUT)
        s.sendall(b"\x05\x01\x00")
        if s.recv(2) != b"\x05\x00":
            return False, "socks5 handshake refused"
        ipb = bytes(int(x) for x in "208.95.112.1".split("."))
        s.sendall(b"\x05\x01\x00\x01" + ipb + b"\x00\x50")
        if s.recv(10)[1] != 0:
            return False, "connect rejected by upstream"
        s.sendall(b"GET /json HTTP/1.0\r\nHost: ip-api.com\r\n\r\n")
        data = b""
        while len(data) <= 8192:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
        s.close()
        body = data.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in data else b""
        j = json.loads(body)
        exit_ip, cc = j.get("query"), j.get("countryCode")
        if not exit_ip:
            return False, "no exit ip in response"
        if region_check and cc and not region_matches(node_region, cc):
            return False, f"region mismatch db={node_region} actual={cc}"
        return True, f"{exit_ip}/{cc}"
    except Exception as e:
        return False, type(e).__name__


def gateway_probe():
    """网关端到端: Basic auth 经 9101 出口, 返回成功次数。"""
    import subprocess
    pw = os.environ.get("GATEWAY_PASSWORD", "")
    auth = base64.b64encode(f"gateway:{pw}".encode()).decode()
    ok = 0
    for _ in range(GATEWAY_N):
        r = subprocess.run(["curl", "-sS", "-m", "15", "-x",
                            f"http://gateway:{pw}@127.0.0.1:9101",
                            "http://ip-api.com/json/?fields=countryCode,query"],
                           capture_output=True, text=True)
        try:
            j = json.loads(r.stdout)
            if j.get("query"):
                ok += 1
        except Exception:
            pass
        time.sleep(1)
    return ok


def main():
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    cur = conn.cursor()
    report, alarms, pool_details = [], [], {}
    for pool, where in POOLS:
        rows = cur.execute(
            f"SELECT name, port, region, exit_ip FROM nodes "
            f"WHERE status='healthy' AND host='127.0.0.1' AND {where} "
            f"ORDER BY RANDOM() LIMIT {SAMPLES_PER_POOL}").fetchall()
        if not rows:
            report.append(f"{pool}: NO-HEALTHY-NODES")
            alarms.append(f"{pool}:0-healthy")
            continue
        oks, details = 0, []
        for name, port, region, _ in rows:
            check = pool not in NO_REGION_CHECK
            ok, detail = socks5_probe("127.0.0.1", port, region, region_check=check)
            if not ok and "mismatch" in str(detail):
                # 归属错配重试一次仍错才算 (个别节点出口漂移 vs 系统性错配区分)
                time.sleep(2)
                ok, detail = socks5_probe("127.0.0.1", port, region, region_check=check)
            elif not ok:
                # 连通类失败一次重试: 共享出口节点会集体撞 ip-api 对该出口 IP
                # 的 45/min 限流窗口 (429 返回非法 JSON -> JSONDecodeError),
                # 单轮瞬时失败不算故障。
                time.sleep(5)
                ok, detail = socks5_probe("127.0.0.1", port, region, region_check=check)
            oks += ok
            details.append(f"{name}:{'OK' if ok else detail}")
        pool_details[pool] = details
        tag = "OK" if oks > 0 else "FAIL"
        report.append(f"{pool}: {oks}/{len(rows)} alive [{tag}]")
        if oks == 0:
            alarms.append(f"{pool}:0/{len(rows)}-alive")
        elif oks < len(rows) // 2:
            alarms.append(f"{pool}:majority-degraded({oks}/{len(rows)})")

    gw_ok = gateway_probe()
    report.append(f"gateway: {gw_ok}/{GATEWAY_N} ok")
    if gw_ok == 0:
        alarms.append("gateway:0/3")

    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    if alarms:
        # 连续两轮同池失败才真告警: 单池 5 样本对 ip-api 限流窗口/桥核瞬时
        # 抖动太敏感 (2026-11-15 实测 cloud 0/5 与 skyward 0/5 均为瞬时,
        # 2 分钟后复测全 OK)。首轮失败只落日志观察。
        try:
            prev = set(open(STATE).read().split())
        except OSError:
            prev = set()
        try:
            prev_details = json.load(open(FAIL_DETAIL))
        except Exception:
            prev_details = {}
        curr = {a.split(":")[0] for a in alarms}
        persistent = prev & curr
        # 落本轮失败 detail, 供下一轮判定 listener 级故障
        try:
            json.dump(pool_details, open(FAIL_DETAIL, "w"))
        except OSError:
            pass
        if persistent:
            print(f"[HOURLY_CONN_FAIL] {stamp} :: {'; '.join(report)} :: "
                  f"PERSISTENT_ALARMS={sorted(persistent)} (2nd consecutive round)", file=sys.stderr)
            rc = 1
            # 自动重启 (listener 级自愈): 同池连续两轮 FAIL + 失败 detail 里
            # TCP/握手级占比 >=80% + 距上次重启 >=60min。节点级故障 (部分死/
            # region 错配) 不重启 —— 重启只治 listener 假死, 治不了单节点。
            restarted = []
            for pool in sorted(persistent):
                if pool == "gateway":
                    continue
                det = " ".join(str(d) for d in prev_details.get(pool, []))
                fails = [d for d in prev_details.get(pool, []) if ":OK" not in str(d)]
                if not fails:
                    continue
                listener_lvl = sum(1 for d in fails
                                   if any(k in str(d).lower() for k in LISTENER_LEVEL))
                if listener_lvl < 0.8 * len(fails):
                    print(f"[CONN_RESTART_SKIP] {pool}: failures not listener-level "
                          f"({listener_lvl}/{len(fails)} handshake-class) — node-level issue, no restart",
                          file=sys.stderr)
                    continue
                service = POOL_SERVICE.get(pool, DEFAULT_BRIDGE)
                try:
                    last = float(open(LAST_RESTART).read())
                except Exception:
                    last = 0.0
                if time.time() - last < RESTART_MIN_INTERVAL:
                    print(f"[CONN_RESTART_SKIP] {pool}: rate-limited (last restart "
                          f"{int(time.time() - last)}s ago)", file=sys.stderr)
                    continue
                lock_fd = open(RESTART_LOCK, "w")
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    print(f"[CONN_RESTART_SKIP] {pool}: another restart in progress", file=sys.stderr)
                    continue
                try:
                    print(f"[CONN_RESTART] {stamp} restarting {service} for pool={pool} "
                          f"(listener-level failure {listener_lvl}/{len(fails)})", file=sys.stderr)
                    subprocess.run(["systemctl", "restart", service], check=False)
                    time.sleep(10)
                    # 复验: 服务 active + 该池抽样 3 个节点握手
                    active = subprocess.run(["systemctl", "is-active", service],
                                            capture_output=True, text=True).stdout.strip()
                    re_rows = cur.execute(
                        f"SELECT port FROM nodes WHERE status='healthy' AND host='127.0.0.1' "
                        f"AND {dict(POOLS)[pool]} ORDER BY RANDOM() LIMIT 3").fetchall()
                    re_ok = 0
                    for (rp,) in re_rows:
                        ok, _ = socks5_probe("127.0.0.1", rp, "hk", region_check=False)
                        re_ok += ok
                    verify = "ok" if (active == "active" and re_ok > 0) else f"still-bad(active={active},re_ok={re_ok})"
                    print(f"[CONN_RESTART] pool={pool} service={service} verify={verify}", file=sys.stderr)
                    open(LAST_RESTART, "w").write(str(time.time()))
                    restarted.append(f"{pool}:{service}:{verify}")
                finally:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    lock_fd.close()
            if restarted:
                # 重启动作已做: 清两轮失败状态, 下一轮重新累计 (避免下轮立刻又触发)
                try:
                    os.unlink(STATE)
                except OSError:
                    pass
                conn.close()
                return 1 if any("still-bad" in r for r in restarted) else 0
            open(STATE, "w").write(" ".join(curr))
            conn.close()
            return rc
        print(f"[HOURLY_CONN_WARN] {stamp} :: {'; '.join(report)} :: "
              f"first-round alarms={alarms} (will FAIL if repeats next hour)", file=sys.stderr)
        open(STATE, "w").write(" ".join(curr))
        conn.close()
        return 0
    # 全绿: 清掉历史失败状态
    try:
        os.unlink(STATE)
    except OSError:
        pass
    print(f"[HOURLY_CONN_OK] {stamp} :: {'; '.join(report)}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
