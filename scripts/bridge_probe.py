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
import re
import socket
import subprocess
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

MIHOMO_BIN = "/opt/ip-pool/bin/mihomo"


def _parse_ss_ports(text):
    """从 `ss -tlnp` 输出提取 (端口, pid) 对; 无 pid 归属时 pid=None。

    行样例: `LISTEN 0 4096 127.0.0.1:39001 0.0.0.0:* users:(("mihomo",pid=1234,fd=18))`
    pid 归属是防串扰的关键: 刷新脚本各自 flock, 但探活段可并发,
    整机端口集会把别的临时核 listener 误当自己就绪 (出口 IP 错配)。
    """
    pairs = set()
    for line in text.splitlines():
        m = re.search(r":(\d+)\s", line)
        if not m:
            continue
        pm = re.search(r"pid=(\d+)", line)
        pairs.add((int(m.group(1)), int(pm.group(1)) if pm else None))
    return pairs


def _parse_lsof_ports(text):
    """从 lsof -nP -iTCP -sTCP:LISTEN 输出提取 (端口, pid) 对 (跳过表头)。"""
    pairs = set()
    for line in text.splitlines()[1:]:
        f = line.split()
        if len(f) >= 9 and f[1].isdigit():
            port = f[8].rsplit(":", 1)[-1]
            if port.isdigit():
                pairs.add((int(port), int(f[1])))
    return pairs


def _listening_ports():
    """(端口, pid) 集合; ss 不可用 (如 macOS 本地调试) 时回退 lsof, 再不行返回 None。"""
    try:
        r = subprocess.run(["ss", "-tlnp"], capture_output=True, text=True, timeout=5)
        if r.returncode == 0 and r.stdout.strip():
            return _parse_ss_ports(r.stdout)
    except Exception:
        pass
    try:  # macOS / 无 ss 环境: lsof 按端口归属查
        r = subprocess.run(["lsof", "-nP", "-iTCP", "-sTCP:LISTEN"],
                           capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            return _parse_lsof_ports(r.stdout)
    except Exception:
        pass
    return None


def wait_for_listeners(core, probe_pool, count, timeout=None):
    """等临时核的 probe listener 全部就位再开探。

    固定 sleep(4) 在节点数多时不够: mihomo 按配置顺序建 listener, 末尾节点首轮
    探活时端口尚未 listen → 被误判 dead (2026-11-19 SUB4 实测 15 全活只入 5;
    2026-11-22 SUB10 复发并升级: 716 listener 时尾部 15 条 ConnectionRefused,
    40s 固定超时耗尽后 probing anyway, 重放同配置单独探 2/17 立即活)。
    → 超时改为随 count 缩放: 40s 基底 + 每 100 listener +10s (1300 条 ≈ 170s)。

    就绪判定优先按 pid 圈定 (core.pid 的 listener 才算数) —— 各刷新脚本独立
    flock 可并发, 整机端口集会把别的临时核 listener 误当自己就绪, 探到的端口
    实际落在别的核上 (出口 IP 错配)。pid 不可得时退回整机端口集, 都不可得时
    逐端口 connect 试探首尾两个 (宽松但可用的最后兜底)。
    """
    if count <= 0:
        return True
    if timeout is None:
        timeout = 40 + (count // 100) * 10
    want = set(range(probe_pool, probe_pool + count))
    want_pairs = {(p, core.pid) for p in want}
    want_any = {(p, None) for p in want}
    deadline = time.time() + timeout
    while time.time() < deadline and core.poll() is None:
        have = _listening_ports()
        if have is not None:
            # 严格: 本核 pid 持有全部端口; 宽容: 端口在且无 pid 归属 (ss 无权限等)
            if want_pairs <= have or (want_any <= have and not (want & {p for p, pid in have if pid is not None})):
                return True
        else:  # 端口归属完全不可得: 逐端口 connect 试探 (首尾)
            if all(_port_open(p) for p in (probe_pool, probe_pool + count - 1)):
                return True
        time.sleep(0.4)
    return False


def _port_open(port, timeout=0.4):
    try:
        socket.create_connection(("127.0.0.1", port), timeout=timeout).close()
        return True
    except OSError:
        return False


def _reality_ok(p):
    """REALITY 数据完整性: 有 public-key 必须有 short-id (0-16 hex)。

    上游订阅存在"半截 reality"节点 (s8 批 14/255 实测), mihomo 对其报
    invalid REALITY short ID 并拒整份探活配置 —— 主探活全灭会级联成
    刷新部署残缺池。缺 short-id 的 reality 节点本就无法握手, 剔除不损失
    真实可用性。"""
    ro = p.get("reality-opts")
    if isinstance(ro, dict) and "public-key" in ro:
        sid = ro.get("short-id")
        return isinstance(sid, str) and 0 < len(sid) <= 16
    return True


class _Quoted(str):
    """标记类: 数字样字符串强制带引号 dump (见 _numeric_like_strings)。"""


def _numeric_like_strings(obj):
    """递归把"数字样字符串"标记为必须带引号的 dump 形式。

    PyYAML safe_dump 对 '906152e4' (科学计数法样) 裸输出, go-yaml 读回解析成
    float -> mihomo 报 invalid REALITY short ID (2026-11-22 SUB10 接入实证:
    s10 全源 38 条中 '906152e4'/'99141482' 两条带引号值, dump 后被吞)。
    解决: 给这类字符串包一层单元素自定义类型, 强制 PyYAML 带引号输出。"""
    import yaml

    def _repr(dumper, data):
        return dumper.represent_scalar("tag:yaml.org,2002:str", str(data), style="'")

    yaml.add_representer(_Quoted, _repr, Dumper=yaml.SafeDumper)
    if isinstance(obj, dict):
        return {k: _numeric_like_strings(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_numeric_like_strings(v) for v in obj]
    if isinstance(obj, str) and re.fullmatch(
            r"[-+]?[0-9][0-9_]*([eE][-+]?[0-9]+)?", obj.strip()):
        return _Quoted(obj)
    return obj


def probe_via_listeners(
    proxies,
    probe_pool: int,
    probe_dir: str,
    mode: str = "direct",
    external_controller: str = "127.0.0.1:39998",
    allow_lan: bool = None,
    max_workers: int = 50,
    extra_proxies=None,
):
    """临时 mihomo 核 + 明文 ip-api 探活, 返回 [(idx, proxy, exit_ip, country_code)] (仅活节点)。

    country_code 是 ip-api 的两字母国家码 (US/DE/...), 查不到时为 None;
    gen 区域纠偏 (gen_region_fix) 依赖它, 旧调用方按位置取前三个不受影响。
    调用方传入互不重叠的 probe_pool / probe_dir, 两个刷新脚本即可并发运行。

    extra_proxies: 只进配置、不开 listener 的辅助节点 (如 dialer-proxy 中转),
    供代理里的 `dialer-proxy: <name>` 解析, 不参与探活结果。
    """
    import yaml
    # 探活前剔除半截 reality 节点 (见 _reality_ok docstring)
    dropped_reality = [p["name"] for p in proxies if not _reality_ok(p)]
    if dropped_reality:
        print("probe: dropped %d proxies with invalid REALITY opts (e.g. %s)" % (
            len(dropped_reality), dropped_reality[0]), file=sys.stderr)
        proxies = [p for p in proxies if _reality_ok(p)]
    extra_proxies = [p for p in (extra_proxies or [])
                     if p.get("name") not in {x.get("name") for x in proxies}]
    listeners = [{"name": f"probe-{i:03d}", "type": "socks", "port": probe_pool + i,
                  "listen": "127.0.0.1", "proxy": p["name"]} for i, p in enumerate(proxies)]
    cfg = {"proxies": extra_proxies + list(proxies), "listeners": listeners, "mode": mode,
           "log-level": "warning", "ipv6": False,
           "external-controller": external_controller}
    if allow_lan is not None:
        cfg["allow-lan"] = allow_lan
    pdir = Path(probe_dir)
    pdir.mkdir(exist_ok=True)
    tmp = pdir / "probe.yaml"
    fd = os.open(str(tmp), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(yaml.safe_dump(_numeric_like_strings(cfg), allow_unicode=True, sort_keys=False))
    proc = subprocess.run([MIHOMO_BIN, "-t", "-d", str(pdir), "-f", str(tmp)],
                          capture_output=True, text=True)
    if "successful" not in proc.stdout + proc.stderr:
        print("probe config invalid:", proc.stdout[-300:], proc.stderr[-300:], file=sys.stderr)
        return []
    core = subprocess.Popen([MIHOMO_BIN, "-d", str(pdir), "-f", str(tmp)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_for_listeners(core, probe_pool, len(proxies)):
        print("probe listeners not all up after timeout; probing anyway", file=sys.stderr)

    # 按上游 server 分组节流: 单入口型上游对同源 IP 的并发 reality 握手限流
    # (2026-11-22 SUB10 yuetong 实证: 17 条同 server 并发探 0/17 全拒,
    # 逐条串行 17/17 全活)。同组串行、跨组仍吃满 max_workers —— 正常多源
    # 订阅各节点 server 互异, 全局吞吐不受影响。
    # 分组键取注册域后二段 (yuetong.online): 同一上游商的不同子域入口
    # (17 个 *.e.yuetong.online) 共享同一真实出口, 只按整串 server 分组
    # 防不住跨子域并发互撞 (实测全量探活 s10 仅 7/17 活)。
    import socket as _socket

    # 分组键: 纯 IP 用全串; 域名取注册域后二段 (yuetong.online)。同一上游商
    # 的不同子域入口 (17 个 *.e.yuetong.online) 共享同一真实出口, 只按整串
    # server 分组防不住跨子域并发互撞 (实测全量探活 s10 仅 7/17 活)。
    # 注意 IP 不能取末二段 (181.201.5.6 会与 181.201.7.8 错并成同组硬串行)。
    _ip_re = re.compile(r"\d{1,3}(?:\.\d{1,3}){3}|\[[0-9a-f:]+\]")
    _domain_re = re.compile(r"^[^.]+(?:\.[^.]+)*?\.([^.]+\.[^.]+)$")
    _sems = defaultdict(threading.Semaphore)

    def _sem_key(p):
        srv = str(p.get("server") or "")
        if _ip_re.fullmatch(srv):
            return srv
        m = _domain_re.match(srv)
        return m.group(1).lower() if m else (srv or p.get("name"))

    def probe(i_p):
        i, p = i_p
        try:
            with _sems[_sem_key(p)]:
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
