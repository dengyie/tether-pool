"""tests/test_refresh_common.py - 刷新脚本共享工具回归 (2026-11-23 优化 9)。

重点: sub(无后缀) 备份轮转不得跨脚本误删 skyward/teleapisub/pigeonw 的
带后缀备份 —— 旧版 glob `huawei-bridge.yaml.*` 跨脚本匹配, 轮转最老位时
会把其它脚本的备份删掉 (实证: fnmatch 模拟误删 True)。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.refresh_common import backup_with_rotation, sub_url, unbool_proxies  # noqa: E402


def test_unbool_proxies_restores_yaml11_bool_credentials():
    proxies = [{"name": "a", "username": True, "password": False, "server": "ok"}]
    out = unbool_proxies(proxies)
    assert out[0]["username"] == "true"
    assert out[0]["password"] == "false"
    assert out[0]["server"] == "ok"          # 非 bool 不动
    assert unbool_proxies(proxies) is proxies  # 原地修改并返回


def _mk_backup(tmp_path, name):
    p = tmp_path / name
    p.write_bytes(b"x")
    return p


def test_rotation_no_suffix_never_deletes_other_scripts_backups(tmp_path):
    """sub(无后缀) 轮转: 带后缀备份 (其它脚本命名空间) 必须原样保留。"""
    bdir = tmp_path / "backups"
    bdir.mkdir()
    other = [_mk_backup(bdir, f"huawei-bridge.yaml.20261119-0000{s}-skyward") for s in "123"]
    other += [_mk_backup(bdir, f"nodes.yaml.20261119-0000{s}-teleapisub") for s in "12"]
    other += [_mk_backup(bdir, "pigeonw-bridge.yaml.20261119-00001-pigeonw")]
    # 15 个本脚本纯时间戳备份 (8+7) → keep=7 只留 14, 删最老 1 个
    own = [_mk_backup(bdir, f"huawei-bridge.yaml.2026112{i}-000000") for i in range(1, 9)]
    own += [_mk_backup(bdir, f"nodes.yaml.2026112{i}-000000") for i in range(1, 8)]

    backup_with_rotation(bdir, [], 7)

    for p in other:
        assert p.exists(), f"跨脚本备份被误删: {p.name}"
    assert own[0].exists() is False           # 本脚本最老的被轮转
    assert all(p.exists() for p in own[1:])


def test_rotation_with_suffix_only_rotates_own_namespace(tmp_path):
    """带后缀脚本轮转: 只动自己 -suffix 的备份, 不碰纯时间戳/别家后缀。"""
    bdir = tmp_path / "backups"
    bdir.mkdir()
    own = [_mk_backup(bdir, f"huawei-bridge.yaml.2026112{i}-000000-skyward") for i in range(1, 9)]
    foreign = _mk_backup(bdir, "huawei-bridge.yaml.20261120-000000")
    foreign2 = _mk_backup(bdir, "nodes.yaml.20261120-000000-teleapisub")

    backup_with_rotation(bdir, [], 2, suffix="-skyward")

    assert own[0].exists() is False and own[1].exists() is False   # 只留最新 4 个
    assert all(p.exists() for p in own[4:])
    assert not own[2].exists() and not own[3].exists()
    assert foreign.exists() and foreign2.exists()


def test_rotation_writes_new_backup_files(tmp_path):
    bdir = tmp_path / "backups"
    cfg = tmp_path / "bridge.yaml"
    cfg.write_bytes(b"cfg-content")
    backup_with_rotation(bdir, [cfg], 3, suffix="-pigeonw")
    made = list(bdir.glob("bridge.yaml.*-pigeonw"))
    assert len(made) == 1 and made[0].read_bytes() == b"cfg-content"


def test_sub_url_reads_env_or_exits(capsys, monkeypatch):
    monkeypatch.setenv("X_SUB_URL", "https://example.com/sub")
    assert sub_url("X_SUB_URL", "TAG") == "https://example.com/sub"
    monkeypatch.delenv("X_SUB_URL")
    try:
        sub_url("X_SUB_URL", "TAG")
        raise AssertionError("should have exited")
    except SystemExit:
        pass
    err = capsys.readouterr().err
    assert "[TAG]" in err and "/etc/ip-pool-secrets.env" in err
