"""tests/test_bridge_probe.py - 探针辅助函数单测 (离线)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import bridge_probe  # noqa: E402


def test_parse_ss_ports_extracts_pid_pairs():
    text = """State  Recv-Q Send-Q Local Address:Port  Peer Address:Port Process
LISTEN 0      4096        127.0.0.1:39001      0.0.0.0:*     users:(("mihomo",pid=1234,fd=18))
LISTEN 0      4096              [::]:39002         [::]:*     users:(("mihomo",pid=5678,fd=20))
LISTEN 0      128             0.0.0.0:22         0.0.0.0:*     users:(("sshd",pid=99,fd=5))
LISTEN 0      64              127.0.0.1:39003    0.0.0.0:*
"""
    assert bridge_probe._parse_ss_ports(text) == {
        (39001, 1234), (39002, 5678), (22, 99), (39003, None)}


def test_parse_ss_ports_ignores_header_words():
    # 表头 "Address:Port" 不含纯数字端口, 不应产生假阳性
    assert bridge_probe._parse_ss_ports("Local Address:Port Peer Address:Port Process\n") == set()


def test_parse_lsof_ports():
    """macOS 回退路径: lsof 输出 (COMMAND PID ... NAME), NAME 尾段为端口。"""
    text = """COMMAND   PID USER   FD   TYPE  DEVICE SIZE/OFF NODE NAME
mihomo   1234 mango   18u  IPv4  0xabcd    0t0  TCP 127.0.0.1:39001 (LISTEN)
mihomo   1234 mango   20u  IPv6  0xabce    0t0  TCP [::]:39002 (LISTEN)
"""
    assert bridge_probe._parse_lsof_ports(text) == {(39001, 1234), (39002, 1234)}


class _FakeCore:
    def __init__(self, pid=4242, alive=True):
        self.pid = pid
        self._alive = alive

    def poll(self):
        return None if self._alive else 0


def test_wait_for_listeners_count_zero_returns_true():
    """count=0 无可等待端口, 必须立即 True (旧版 fallback 探 probe_pool-1 假超时)。"""
    assert bridge_probe.wait_for_listeners(_FakeCore(), 39001, 0, timeout=0.5) is True


def test_wait_for_listeners_pid_scope_rejects_foreign_listeners(monkeypatch):
    """串扰防护: 端口已 listen 但属于别的核 (pid 不符) 不得判就绪。"""
    import time as _time
    monkeypatch.setattr(bridge_probe.time, "sleep", lambda s: None)
    monkeypatch.setattr(bridge_probe, "_listening_ports",
                        lambda: {(39001, 9999), (39002, 9999)})  # 别的核持有
    assert bridge_probe.wait_for_listeners(_FakeCore(pid=4242), 39001, 2, timeout=0.4) is False


def test_wait_for_listeners_pid_scope_accepts_own_listeners(monkeypatch):
    monkeypatch.setattr(bridge_probe.time, "sleep", lambda s: None)
    monkeypatch.setattr(bridge_probe, "_listening_ports",
                        lambda: {(39001, 4242), (39002, 4242), (22, 99)})
    assert bridge_probe.wait_for_listeners(_FakeCore(pid=4242), 39001, 2, timeout=1) is True


def test_wait_for_listeners_accepts_pidless_entries_when_no_pid_owner(monkeypatch):
    """ss 无权限时 pid 缺失: 端口在且无任何 pid 占有 → 宽容判定就绪。"""
    monkeypatch.setattr(bridge_probe.time, "sleep", lambda s: None)
    monkeypatch.setattr(bridge_probe, "_listening_ports",
                        lambda: {(39001, None), (39002, None)})
    assert bridge_probe.wait_for_listeners(_FakeCore(pid=4242), 39001, 2, timeout=1) is True


def test_wait_for_listeners_falls_back_to_port_open_when_no_tool(monkeypatch):
    """ss/lsof 全不可得 (macOS 裸环境): 回退首尾端口 connect 试探。"""
    monkeypatch.setattr(bridge_probe.time, "sleep", lambda s: None)
    monkeypatch.setattr(bridge_probe, "_listening_ports", lambda: None)
    opens = []

    def fake_open(port, timeout=0.4):
        opens.append(port)
        return port in (39001, 39005)

    monkeypatch.setattr(bridge_probe, "_port_open", fake_open)
    assert bridge_probe.wait_for_listeners(_FakeCore(), 39001, 5, timeout=1) is True
    assert opens == [39001, 39005]  # 只探首尾两个
