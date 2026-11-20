"""tests/test_doproxy_sync.py - DoProxy 适配器单测 (纯离线, 不触网)。

2026-11-19 SUB6 接入实弹回归: mihomo 不支持 socks4, 24 条混入让 probe yaml
整体校验失败 → alive 0/195。协议白名单是生产事故根修, 必须有测试护栏。
"""
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import doproxy_sync as ds  # noqa: E402


def _row(**kw):
    base = {"host": "1.2.3.4", "port": 8080, "protocol": "http",
            "country": "US", "country_name": "美国"}
    base.update(kw)
    return base


# ---------------------------------------------------------------- 协议白名单

def test_to_clash_proxy_socks4_rejected():
    """socks4 必须拒收 (mihomo unsupport → probe 整体失败 alive 0/195 根修)。"""
    assert ds.to_clash_proxy(_row(protocol="socks4"), 1, "residential") == {}


def test_to_clash_proxy_http_socks5_passthrough():
    p = ds.to_clash_proxy(_row(protocol="http"), 1, "residential")
    assert p["type"] == "http" and p["udp"] is False
    p5 = ds.to_clash_proxy(_row(protocol="socks5"), 2, "business")
    assert p5["type"] == "socks5" and p5["udp"] is True


def test_to_clash_proxy_unknown_protocol_rejected():
    assert ds.to_clash_proxy(_row(protocol="vmess"), 1, "residential") == {}
    assert ds.to_clash_proxy(_row(protocol=None), 1, "residential")["type"] == "http"


# ---------------------------------------------------------------- 脏行容错

def test_to_clash_proxy_bad_host_port_skipped(capsys):
    """port=None/字符串/缺失不炸全批 (review P2: per-row 容错)。"""
    assert ds.to_clash_proxy(_row(port=None), 1, "residential") == {}
    assert ds.to_clash_proxy(_row(port="abc"), 1, "residential") == {}
    assert ds.to_clash_proxy(_row(host=""), 1, "residential") == {}
    assert ds.to_clash_proxy(_row(port=0), 1, "residential") == {}
    out = capsys.readouterr().err
    assert out.count("bad row skipped") == 4
    # port 字符串数字合法
    assert ds.to_clash_proxy(_row(port="8080"), 1, "residential")["port"] == 8080


def test_to_clash_proxy_auth_conditional():
    no_auth = ds.to_clash_proxy(_row(), 1, "residential")
    assert "username" not in no_auth and "password" not in no_auth
    auth = ds.to_clash_proxy(_row(username="u", password="p"), 1, "residential")
    assert auth["username"] == "u" and auth["password"] == "p"


def test_to_clash_proxy_naming_stable_and_indexed():
    p = ds.to_clash_proxy(_row(), 7, "residential")
    assert p["name"] == "US.美国-residential-http-007"


# ---------------------------------------------------------------- 分页

def _fake_urlopen_factory(pages):
    """pages: ip_type 无关的响应序列 (每页一个 dict)。"""
    seq = iter(pages)

    class FakeResp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            import json
            return json.dumps(next(seq)).encode()

    return lambda *a, **k: FakeResp()


def test_fetch_api_paginates_until_total(monkeypatch, capsys):
    """total > 单页时自动翻页取满 (review P2 根修升级: 截断→翻页)。"""
    monkeypatch.setattr(ds.urllib.request, "urlopen",
                        _fake_urlopen_factory([
                            {"code": 200, "total": 250, "data": [_row() for _ in range(100)]},
                            {"code": 200, "total": 250, "data": [_row() for _ in range(100)]},
                            {"code": 200, "total": 250, "data": [_row() for _ in range(50)]},
                        ]))
    monkeypatch.setattr(ds.time, "sleep", lambda s: None)
    rows = ds.fetch_api("business")
    assert len(rows) == 250
    assert "incomplete" not in capsys.readouterr().err


def test_fetch_api_incomplete_after_max_pages_warns(monkeypatch, capsys):
    """超 MAX_PAGES 仍不满足 total: 告警可见化, 返回已取到的部分。"""
    monkeypatch.setattr(ds.urllib.request, "urlopen",
                        _fake_urlopen_factory([
                            {"code": 200, "total": 9999, "data": [_row() for _ in range(100)]},
                        ] * ds.MAX_PAGES))
    monkeypatch.setattr(ds.time, "sleep", lambda s: None)
    rows = ds.fetch_api("business")
    assert len(rows) == 100 * ds.MAX_PAGES
    assert "incomplete" in capsys.readouterr().err


def test_fetch_api_bad_code_raises(monkeypatch):
    monkeypatch.setattr(ds.urllib.request, "urlopen",
                        _fake_urlopen_factory([{"code": 500, "data": []}]))
    try:
        ds.fetch_api("residential")
        raise AssertionError("code!=200 必须 raise")
    except RuntimeError as e:
        assert "code=500" in str(e)


# ---------------------------------------------------------------- 端到端: 去重/原子落盘/fail-open

def _run_main(monkeypatch, tmp_path, api_pages):
    """api_pages: 每次 fetch_api 调用依次返回一个页面 (list of rows)。"""
    monkeypatch.setattr(ds, "OUT", tmp_path / "sub6" / "doproxy.yaml")
    seq = iter(api_pages)
    monkeypatch.setattr(ds, "fetch_api", lambda ip_type: next(seq))
    return ds.main()


def test_main_dedup_and_atomic_write(tmp_path, monkeypatch):
    rows = [_row(host="1.1.1.1", port=80),
            _row(host="1.1.1.1", port=80),               # 重复 host:port
            _row(host="2.2.2.2", port=443, protocol="socks4"),  # socks4 过滤
            _row(host="2.2.2.2", port=4443, protocol="socks5")]
    rc = _run_main(monkeypatch, tmp_path, [rows])  # residential 页给全量, business 页 StopIteration→fail-open
    assert rc == 0
    out = tmp_path / "sub6" / "doproxy.yaml"
    d = yaml.safe_load(out.read_text())
    assert [p["server"] for p in d["proxies"]] == ["1.1.1.1", "2.2.2.2"]
    assert d["proxies"][0]["name"].endswith("-001")
    assert (tmp_path / "sub6" / "doproxy.tmp").exists() is False


def test_main_zero_proxies_keeps_previous_yaml(tmp_path, monkeypatch, capsys):
    """API 全挂/空池: 保留上次 yaml (fail-open)。"""
    out = tmp_path / "sub6" / "doproxy.yaml"
    out.parent.mkdir(parents=True)
    out.write_text("proxies: [{name: old, type: http, server: 9.9.9.9, port: 1}]\n")
    rc = _run_main(monkeypatch, tmp_path,
                   [[_row(protocol="socks4")], []])  # 两页全被过滤 → zero
    assert rc == 1
    assert yaml.safe_load(out.read_text())["proxies"][0]["name"] == "old"
    assert "keep previous yaml" in capsys.readouterr().err


def test_main_chmod_600(tmp_path, monkeypatch):
    _run_main(monkeypatch, tmp_path, [[_row()]])
    mode = (tmp_path / "sub6" / "doproxy.yaml").stat().st_mode & 0o777
    assert mode == 0o600
