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
    # 英文城市名 (机场 "[vless]Tokyo1 - B Group" 风格)
    ("[vless]Tokyo1 - B Group", "jp"),
    ("[vless]Frankfurt2 - B Group", "de"),
    ("[vless]Singapore3 - B Group", "sg"),
    ("[vless]HongKong1 - B Group", "hk"),
    ("[vless]California1 - B Group", "us-west"),
    # 词边界: 子串误报不得命中 (india 在 Indianapolis 内)
    ("Indianapolis 01", "misc"),
    ("India Gate 01", "in"),
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


def _http_error(code, body=b""):
    import io
    import urllib.error
    return urllib.error.HTTPError("https://example.com/sub", code, "err", {}, io.BytesIO(body))


def test_fetch_does_not_retry_on_frozen_410(monkeypatch):
    """410 = 机场冻结链接, 重试会被计为异常拉取, 只会更难解封。"""
    import urllib.error
    calls = []

    def fake_urlopen(req, timeout):
        calls.append(1)
        raise _http_error(410, "这条链接因异常拉取已被冻结".encode())

    monkeypatch.setattr(spr.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(spr.time, "sleep", lambda s: None)
    with pytest.raises(spr.PermanentSubError) as ei:
        spr.fetch_and_parse("https://example.com/sub")
    assert len(calls) == 1
    assert "410" in str(ei.value) and "冻结" in str(ei.value)


@pytest.mark.parametrize("code", [401, 403, 404, 410])
def test_fetch_treats_other_4xx_as_permanent(monkeypatch, code):
    calls = []

    def fake_urlopen(req, timeout):
        calls.append(1)
        raise _http_error(code)

    monkeypatch.setattr(spr.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(spr.time, "sleep", lambda s: None)
    with pytest.raises(spr.PermanentSubError):
        spr.fetch_and_parse("https://example.com/sub")
    assert len(calls) == 1


def test_fetch_429_with_retry_after_waits_once(monkeypatch):
    """429 + Retry-After ≤60s: 退避等一次, 不再重试 (限速下连打有害)。"""
    import io
    import urllib.error
    calls = []

    def fake_urlopen(req, timeout):
        calls.append(1)
        raise urllib.error.HTTPError("u", 429, "Too Many Requests",
                                     {"Retry-After": "30"}, io.BytesIO(b""))

    monkeypatch.setattr(spr.urllib.request, "urlopen", fake_urlopen)
    slept = []
    monkeypatch.setattr(spr.time, "sleep", lambda s: slept.append(s))
    with pytest.raises(spr.RateLimitedSubError):
        spr.fetch_and_parse("https://example.com/sub")
    assert len(calls) == 1 and slept == [30]


def test_fetch_429_without_header_retries_once(monkeypatch):
    """429 无 Retry-After: 只重试 1 次 (瞬时错误是 3 次), 之后交回主流程。"""
    calls = []

    def fake_urlopen(req, timeout):
        calls.append(1)
        raise _http_error(429)

    monkeypatch.setattr(spr.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(spr.time, "sleep", lambda s: None)
    with pytest.raises(Exception) as ei:
        spr.fetch_and_parse("https://example.com/sub")
    assert not isinstance(ei.value, spr.PermanentSubError)
    assert len(calls) == 2  # 首次 + 1 重试


def test_fetch_429_long_retry_after_skips_waiting(monkeypatch):
    """Retry-After > 60s: 不傻等, 走 1 次重试预算。"""
    import io
    import urllib.error
    calls = []

    def fake_urlopen(req, timeout):
        calls.append(1)
        raise urllib.error.HTTPError("u", 429, "T",
                                     {"Retry-After": "3600"}, io.BytesIO(b""))

    monkeypatch.setattr(spr.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(spr.time, "sleep", lambda s: None)
    with pytest.raises(spr.RateLimitedSubError):
        spr.fetch_and_parse("https://example.com/sub")
    assert len(calls) == 2


def test_fetch_still_retries_transient_errors(monkeypatch):
    calls = []

    def fake_urlopen(req, timeout):
        calls.append(1)
        raise _http_error(502)

    monkeypatch.setattr(spr.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(spr.time, "sleep", lambda s: None)
    with pytest.raises(Exception) as ei:
        spr.fetch_and_parse("https://example.com/sub")
    assert not isinstance(ei.value, spr.PermanentSubError)
    assert len(calls) == 3


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


# ---------------------------------------------------------------- 死链接淘汰 (2026-11-19 review 根修)

def test_apply_source_failure_counts_to_death():
    """PermanentSubError 连续 3 轮 → 判源死亡 (got=空, SUB_SOURCE_DEAD 标记)."""
    fails = {}
    carried = [{"name": "s1-a"}]
    err = spr.PermanentSubError("HTTP 410 Gone")
    got, msg = spr.apply_source_failure(fails, 1, err, carried, 3)
    assert got == carried and fails["1"] == 1 and "FROZEN" in msg
    got, msg = spr.apply_source_failure(fails, 1, err, carried, 3)
    assert got == carried and fails["1"] == 2
    got, msg = spr.apply_source_failure(fails, 1, err, carried, 3)
    assert got == [] and fails["1"] == 3 and "SUB_SOURCE_DEAD" in msg


def test_apply_source_failure_rate_limited_never_counts():
    """429 限速是瞬时的, 不计入死亡判定 (重试语义归 RateLimitedSubError)。"""
    fails = {}
    err = spr.RateLimitedSubError("HTTP 429", retry_after=10)
    for _ in range(5):
        got, msg = spr.apply_source_failure(fails, 2, err, [{"name": "x"}], 3)
        assert got == [{"name": "x"}]
    assert fails == {}
    assert "THROTTLED" in msg


def test_apply_source_failure_revive_resets_counter():
    """成功路径 pop 计数后, 再次失效从 0 重新累计 (链接复活又死是新一轮)。"""
    fails = {"1": 2}
    fails.pop("1")  # main 内成功分支的清零语义
    err = spr.PermanentSubError("HTTP 404")
    spr.apply_source_failure(fails, 1, err, [], 3)
    assert fails["1"] == 1
