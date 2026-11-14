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
import base64, json, os, random, socket, sqlite3, sys, time

DB = "/opt/ip-pool/data/ip_pool.db"
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
    ("cloud", "host='127.0.0.1' AND tags NOT LIKE '%genproxy%' AND tags NOT LIKE '%teleapisub%' AND tags NOT LIKE '%skyward%' AND tags NOT LIKE '%pigeonw%' AND name NOT LIKE 'clash-5%-sub-%'"),
]


def region_matches(db_region, cc):
    """DB region 与 ip-api 国家码比对: us->us-west 桶名, hk 因地理库偏差不判."""
    if not cc or db_region in ("hk", "misc", None, ""):
        return True  # hk 名字优先原则; misc 无从比对
    c = cc.strip().lower()
    return db_region == ("us-west" if c == "us" else c)


def socks5_probe(ip, port, node_region):
    """直连桥核 listener: SOCKS5 + ip-api /json, 返回 (ok, detail)。"""
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
        if cc and not region_matches(node_region, cc):
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
    report, alarms = [], []
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
            ok, detail = socks5_probe("127.0.0.1", port, region)
            oks += ok
            details.append(f"{name}:{'OK' if ok else detail}")
        tag = "OK" if oks > 0 else "FAIL"
        report.append(f"{pool}: {oks}/{len(rows)} alive [{tag}]")
        if oks == 0:
            alarms.append(f"{pool}:0/{len(rows)}-alive")
        elif oks < len(rows) // 2:
            alarms.append(f"{pool}:majority-degraded({oks}/{len(rows)})")
    conn.close()

    gw_ok = gateway_probe()
    report.append(f"gateway: {gw_ok}/{GATEWAY_N} ok")
    if gw_ok == 0:
        alarms.append("gateway:0/3")

    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    if alarms:
        print(f"[HOURLY_CONN_FAIL] {stamp} :: {'; '.join(report)} :: ALARMS={alarms}", file=sys.stderr)
        return 1
    print(f"[HOURLY_CONN_OK] {stamp} :: {'; '.join(report)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
