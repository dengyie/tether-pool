"""tests/test_sub_pool_refresh.py - 多源订阅刷新纯函数单测 (离线, 不触网)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import sub_pool_refresh as spr  # noqa: E402


# ---------------------------------------------------------------- 区域识别
@pytest.mark.parametrize("name,region", [
    ("s1-HK_11【商宽】", "hk"),                 # 老 s1 命名
    ("s1-US_03【商宽】", "us-west"),            # us 统一收敛 us-west
    ("US MCI BGPSA Mieru IPv4", "us-west"),     # 新源: 国家码 + 空格
    ("VN Hanoi VNPT", "vn"),
    ("JP A Mieru-TCP", "jp"),
    ("🇸🇬 新加坡 | 01", "sg"),                  # 国旗 emoji 优先
    ("🇭🇰 香港 | 04", "hk"),
    ("🇺🇸 美国 | 1000M", "us-west"),
    ("东京 IEPL 专线", "misc"),                  # 无法识别时不猜
    ("📅时间| 2026-12-23 23:59", "misc"),
])
def test_region_of(name, region):
    assert spr.region_of(name) == region


def test_flag_cc_only_matches_flag_pairs():
    assert spr._flag_cc("🇸🇬 新加坡") == "sg"
    assert spr._flag_cc("SG Singapore") is None


# ---------------------------------------------------------------- 源内规范
def test_normalize_prefixes_and_filters_junk():
    src = [
        {"name": "HK_11【商宽】", "server": "1.2.3.4", "port": 8080, "type": "http"},
        {"name": "s1-JP_01", "server": "5.6.7.8", "port": "9090", "type": "socks5"},
        # 订阅常见的到期提示伪节点
        {"name": "📅时间| 2026-12-23", "server": "127.0.0.1", "port": 1, "type": "socks5"},
        {"name": "bad-port", "server": "9.9.9.9", "port": 0, "type": "http"},
        {"name": "", "server": "9.9.9.9", "port": 80, "type": "http"},
    ]
    out = spr.normalize_source_proxies(src, 2)
    assert [p["name"] for p in out] == ["s2-HK_11【商宽】", "s2-JP_01"]
    # 端口统一成 int, 供 mihomo/探活消费
    assert [p["port"] for p in out] == [8080, 9090]


def test_normalize_is_idempotent_across_sources():
    src = [{"name": "s3-HK_01", "server": "1.1.1.1", "port": 80, "type": "http"}]
    assert spr.normalize_source_proxies(src, 3)[0]["name"] == "s3-HK_01"


# ---------------------------------------------------------------- 上游去重
def test_upstream_key_ignores_source_prefix_but_not_credentials():
    a = {"name": "s1-HK", "type": "http", "server": "1.1.1.1", "port": 80,
         "username": "u", "password": "p"}
    b = dict(a, name="s2-HK")
    c = dict(a, name="s3-HK", password="other")
    assert spr.upstream_key(a) == spr.upstream_key(b)
    assert spr.upstream_key(a) != spr.upstream_key(c)


# ---------------------------------------------------------------- 多源发现
def test_sub_sources_sorted_by_index(monkeypatch):
    monkeypatch.setenv("SUB2_URL", "https://example.com/2")
    monkeypatch.setenv("SUB10_URL", "https://example.com/10")
    monkeypatch.setenv("SUB1_URL", "https://example.com/1")
    monkeypatch.setenv("SUB_NOT_URL", "x")
    assert spr._sub_sources() == [(1, "https://example.com/1"),
                                  (2, "https://example.com/2"),
                                  (10, "https://example.com/10")]


def test_sub_sources_exits_without_env(monkeypatch):
    for k in list(__import__("os").environ):
        if k.startswith("SUB") and k.endswith("_URL"):
            monkeypatch.delenv(k, raising=False)
    with pytest.raises(SystemExit):
        spr._sub_sources()


# ---------------------------------------------------------------- 拉取
def test_fetch_sends_browser_ua(monkeypatch):
    """机场订阅常拒默认 python-urllib UA (403), 必须带 clash-verge 身份。"""
    class Resp:
        def read(self):
            return b"proxies: []\n"

    seen = {}

    def fake_urlopen(req, timeout):
        seen["ua"] = req.get_header("User-agent")
        return Resp()

    monkeypatch.setattr(spr.urllib.request, "urlopen", fake_urlopen)
    assert spr.fetch_and_parse("https://example.com/sub") == []
    assert seen["ua"] == "clash-verge/1.6.6"


# ---------------------------------------------------------------- 源级中转
def test_probe_forwards_extra_relays(monkeypatch):
    import types

    calls = {}
    fake = types.ModuleType("bridge_probe")

    def fake_probe(proxies, **kw):
        calls["proxies"] = proxies
        calls["extra"] = kw.get("extra_proxies")
        return []

    fake.probe_via_listeners = fake_probe
    monkeypatch.setitem(sys.modules, "bridge_probe", fake)
    spr.probe_via_listeners([{"name": "s3-x"}], extra_proxies=[{"name": "SKYWARD-01"}])
    assert calls["proxies"] == [{"name": "s3-x"}]
    assert calls["extra"] == [{"name": "SKYWARD-01"}]
