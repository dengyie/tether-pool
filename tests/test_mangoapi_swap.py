"""tests/test_mangoapi_swap.py - mangoapi-egress-swap agent 单测 (纯离线).

覆盖 review 根修点: 作用域收窄(仅迁移渠道) / EOF 提取正则(真实日志行形态) /
触发判定(阈值/冷却/失败退避/日限)。"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import mangoapi_egress_swap as sw  # noqa: E402


TP_PROXY = "http://mangoapi-ch425__type_clash_proxy__service_mangoapi:***@127.0.0.1:9101"


def _rows():
    return [
        (425, "https://mazal.lol", json.dumps({"proxy": TP_PROXY})),          # 已迁移
        (90, "https://demo.dev2.mulink.top", json.dumps({"proxy": TP_PROXY})),  # 已迁移
        (56, "https://api.feixingwawa.cn", json.dumps({"proxy": ""})),        # CN 直连未迁移
        (121, "http://127.0.0.1:8790", json.dumps({"proxy": ""})),            # 内环未迁移
        (138, "https://ai.hybgzs.com", json.dumps({"proxy": "http://127.0.0.1:10823"})),  # 绑定线
        (42, "https://developer.amd.com.cn/radeon/api", None),                # null setting
    ]


# ---------------------------------------------------------------- 作用域收窄 (P1 根修)

def test_build_watch_index_only_migrated():
    host_map, ids = sw.build_watch_index(_rows())
    assert ids == {425, 90}
    assert set(host_map) == {"mazal.lol", "demo.dev2.mulink.top"}
    # 非迁移渠道 (CN/内环/绑定线/null-setting) 不得进入 watch 面
    assert 56 not in ids and 121 not in ids and 138 not in ids and 42 not in ids
    assert "api.feixingwawa.cn" not in host_map
    assert "127.0.0.1" not in host_map


def test_extract_stream_broken_migrated_channel():
    host_map, ids = sw.build_watch_index(_rows())
    line = ('[INFO] 2026/11/18 - 02:29:41 | 流中断：渠道 #425 传输异常中断'
            '（reason=eof soft_errors=1, 已收块=36, 字节=8461），触发换渠道重试并熔断')
    assert sw.extract_channel_ids(line, host_map, ids) == {425}


def test_extract_conn_eof_fans_out_to_host_channels():
    host_map, ids = sw.build_watch_index(_rows())
    line = '[ERR] ... upstream error: do request failed: Post "https://mazal.lol/v1/chat/completions": EOF'
    assert sw.extract_channel_ids(line, host_map, ids) == {425}


def test_extract_ignores_non_migrated_channels():
    """CN 直连渠道的 EOF 不得触发换线 (P1: 防垃圾绑定)."""
    host_map, ids = sw.build_watch_index(_rows())
    line = '[INFO] ... 流中断：渠道 #56 传输异常中断（reason=eof），触发换渠道重试'
    assert sw.extract_channel_ids(line, host_map, ids) == set()
    line2 = '[ERR] ... do request failed: Post "https://api.feixingwawa.cn/v1/chat/completions": EOF'
    assert sw.extract_channel_ids(line2, host_map, ids) == set()


def test_extract_ignores_success_and_non_eof_lines():
    host_map, ids = sw.build_watch_index(_rows())
    # 渠道号出现但无 EOF 信标 -> 不计
    assert sw.extract_channel_ids("渠道 #425 恢复, 200 OK", host_map, ids) == set()
    # reason 非 eof/scanner_error -> 不计
    line = '流中断：渠道 #425 传输异常中断（reason=upstream_500），触发换渠道重试'
    assert sw.extract_channel_ids(line, host_map, ids) == set()
    # scanner_error 变体计入
    line2 = '流中断：渠道 #90 ... upstream stream broken reason=scanner_error'
    assert sw.extract_channel_ids(line2, host_map, ids) == {90}


# ---------------------------------------------------------------- 触发判定 (P2 根修)

def _events(cid, n, now, window=300):
    import collections
    d = collections.defaultdict(collections.deque)
    d[cid].extend([now - i for i in range(n)])
    return d


def test_swap_due_requires_threshold():
    now = time.time()
    ev = _events(425, 2, now)  # 窗口内只有 2 次
    assert not sw.swap_due(425, now, ev, {}, {}, 0, cap_reached=False)
    ev = _events(425, 3, now)
    assert sw.swap_due(425, now, ev, {}, {}, 0, cap_reached=False)


def test_swap_due_success_cooldown_blocks():
    now = time.time()
    ev = _events(425, 5, now)
    assert not sw.swap_due(425, now, ev, {425: now - 300}, {}, 0, cap_reached=False)  # 600s 内
    assert sw.swap_due(425, now, ev, {425: now - 601}, {}, 0, cap_reached=False)


def test_swap_due_fail_retry_keeps_events():
    """失败短退避: 60s 内不重试, 但事件保留 -> 退避结束即重试 (P2 根修语义)."""
    now = time.time()
    ev = _events(425, 5, now)
    assert not sw.swap_due(425, now, ev, {}, {425: now - 30}, 0, cap_reached=False)
    assert sw.swap_due(425, now, ev, {}, {425: now - 61}, 0, cap_reached=False)


def test_swap_due_daily_cap_blocks():
    now = time.time()
    ev = _events(425, 5, now)
    assert not sw.swap_due(425, now, ev, {}, {}, sw.DAILY_CAP, cap_reached=True)


# ---------------------------------------------------------------- LogTail

def test_logtail_increment_truncation_rotation(tmp_path):
    p = tmp_path / "x.log"
    p.write_text("line1\nline2\n")
    t = sw.LogTail(str(p))
    assert t.poll() == []
    with open(p, "a") as f:
        f.write("line3\n")
    assert t.poll() == ["line3"]
    # 截断 -> 从头重读
    p.write_text("fresh\n")
    assert t.poll() == ["fresh"]
    # 轮转 (inode 变化) -> 新文件从 0 开始
    old_ino = t.inode
    p2 = tmp_path / "x.log.new"
    p2.write_text("rotated\n")
    p2.rename(p)
    got = t.poll()
    assert got == ["rotated"] and t.inode != old_ino
