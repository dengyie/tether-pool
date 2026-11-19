import os
import sqlite3
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from scripts import recovery_audit


ROOT = Path(__file__).resolve().parents[1]
CURRENT_RELAY = "[自建] 🇺🇸VLESS-AWS-Oregon-Reality"


def _executable(path, text):
    path.write_text(text, encoding="utf-8")
    path.chmod(0o755)
    return path


def test_recovery_audit_uses_historical_dead_anchor_after_two_stage_recovery(tmp_path):
    db = sqlite3.connect(tmp_path / "audit.db")
    db.executescript("""
        CREATE TABLE nodes (name TEXT PRIMARY KEY, host TEXT, dead_since TEXT);
        CREATE TABLE circuit_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_name TEXT, from_state TEXT, to_state TEXT,
            reason TEXT, created_at TEXT
        );
    """)
    t0 = datetime.now() - timedelta(hours=2)
    t1 = t0 + timedelta(minutes=1)
    t2 = t0 + timedelta(minutes=3)
    db.execute("INSERT INTO nodes VALUES (?, ?, NULL)", ("n1", "host-a"))
    db.executemany(
        "INSERT INTO circuit_events(node_name, from_state, to_state, reason, created_at) VALUES (?, ?, ?, ?, ?)",
        [
            ("n1", "degraded", "dead", "trip", t0.isoformat()),
            ("n1", "dead", "degraded", "canary", t1.isoformat()),
            ("n1", "degraded", "healthy", "rise", t2.isoformat()),
        ],
    )
    db.commit()

    rows = recovery_audit.recovery_rows(db, (t0 - timedelta(minutes=1)).isoformat())

    assert len(rows) == 1
    assert rows[0]["dead_started"] == t0.isoformat()
    assert rows[0]["recovered_at"] == t2.isoformat()
    assert recovery_audit.recovery_latency(rows[0]) == 180.0


def test_recovery_audit_preserves_local_naive_timestamp_semantics():
    local_naive = datetime(2026, 11, 19, 12, 0, 0)
    local_aware = local_naive.astimezone()
    parsed = recovery_audit._parse_timestamp(local_naive.isoformat())
    assert parsed == local_aware


def test_recovery_audit_skips_invalid_timestamps_without_crashing(tmp_path):
    db = sqlite3.connect(tmp_path / "bad-audit.db")
    db.executescript("""
        CREATE TABLE nodes (name TEXT PRIMARY KEY, host TEXT, dead_since TEXT);
        CREATE TABLE circuit_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_name TEXT, from_state TEXT, to_state TEXT,
            reason TEXT, created_at TEXT
        );
    """)
    db.execute("INSERT INTO nodes VALUES ('n1', 'host-a', NULL)")
    db.executemany(
        "INSERT INTO circuit_events(node_name, from_state, to_state, reason, created_at) VALUES (?, ?, ?, ?, ?)",
        [
            ("n1", "degraded", "dead", "trip", "not-a-date"),
            ("n1", "degraded", "healthy", "rise", datetime.now().isoformat()),
        ],
    )
    db.commit()

    with pytest.warns(RuntimeWarning, match="invalid timestamp"):
        rows = recovery_audit.recovery_rows(db, (datetime.now() - timedelta(days=1)).isoformat())
    assert len(rows) == 1
    assert recovery_audit.recovery_latency(rows[0]) is None


def _relay_env(tmp_path, cli_exit="0", import_exit="0", peer_rows=None):
    repo = tmp_path / "repo"
    (repo / "config").mkdir(parents=True)
    (repo / "scripts").mkdir()
    (repo / "data").mkdir()
    bridge_cfg = repo / "config/teleapi-bridge.yaml"
    bridge_cfg.write_text("proxies: []\n", encoding="utf-8")
    (repo / "data/teleapi-bridge.pid").write_text("99999999\n", encoding="utf-8")

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    import_log = tmp_path / "import.log"
    pgrep_log = tmp_path / "pgrep.log"
    cli_rows = peer_rows or (
        "|10.144.144.3/peer|p2p|x|100ms|0%\n"
    )
    _executable(fake_bin / "fake-python", "#!/bin/sh\n"
        "if [ \"$1\" = \"-\" ]; then\n"
        "  printf '%s\\n' \"$RELAY_WD_TEST_CURRENT\"\n"
        "  exit 0\n"
        "fi\n"
        "printf '%s\\n' \"$*\" >> \"$RELAY_WD_IMPORT_LOG\"\n"
        "echo simulated-import\n"
        "exit \"${RELAY_WD_IMPORT_EXIT:-0}\"\n")
    _executable(fake_bin / "easytier-cli", "#!/bin/sh\n"
        "printf '%s' \"$RELAY_WD_PEER_ROWS\"\n"
        "exit \"${RELAY_WD_CLI_EXIT:-0}\"\n")
    _executable(fake_bin / "pgrep", "#!/bin/sh\n"
        "echo called >> \"$RELAY_WD_PGREP_LOG\"\nexit 1\n")

    lockdir = tmp_path / "relay.lock"
    tsf = tmp_path / "last-flip"
    downf = tmp_path / "down-count"
    cli_out = tmp_path / "cli.out"
    env = os.environ.copy()
    env.update({
        "PATH": f"{fake_bin}:/usr/bin:/bin",
        "RELAY_WD_REPO": str(repo),
        "RELAY_WD_PY": str(fake_bin / "fake-python"),
        "RELAY_WD_ET_CLI": str(fake_bin / "easytier-cli"),
        "RELAY_WD_BRIDGE_CFG": str(bridge_cfg),
        "RELAY_WD_LOCKDIR": str(lockdir),
        "RELAY_WD_TSF": str(tsf),
        "RELAY_WD_DOWNF": str(downf),
        "RELAY_WD_CLI_OUT": str(cli_out),
        "RELAY_WD_TEST_CURRENT": CURRENT_RELAY,
        "RELAY_WD_IMPORT_LOG": str(import_log),
        "RELAY_WD_PGREP_LOG": str(pgrep_log),
        "RELAY_WD_PEER_ROWS": cli_rows,
        "RELAY_WD_CLI_EXIT": str(cli_exit),
        "RELAY_WD_IMPORT_EXIT": str(import_exit),
    })
    return env, tsf, downf, import_log, pgrep_log


def test_relay_watchdog_does_not_switch_after_peer_cli_failure(tmp_path):
    env, tsf, downf, import_log, pgrep_log = _relay_env(tmp_path, cli_exit="7")
    tsf.write_text("123\n", encoding="utf-8")
    downf.write_text("1\n", encoding="utf-8")

    result = subprocess.run(
        ["bash", str(ROOT / "scripts/relay_watchdog.sh")],
        env=env, text=True, capture_output=True, timeout=10,
    )

    assert result.returncode == 0
    assert "easytier-cli peer 失败/超时" in result.stdout
    assert tsf.read_text(encoding="utf-8").strip() == "123"
    assert downf.read_text(encoding="utf-8").strip() == "1"
    assert not import_log.exists()
    assert not pgrep_log.exists()


def test_relay_watchdog_config_failure_preserves_flip_state_and_bridge(tmp_path):
    env, tsf, downf, import_log, pgrep_log = _relay_env(tmp_path, import_exit="9")
    tsf.write_text("123\n", encoding="utf-8")
    downf.write_text("1\n", encoding="utf-8")

    result = subprocess.run(
        ["bash", str(ROOT / "scripts/relay_watchdog.sh")],
        env=env, text=True, capture_output=True, timeout=10,
    )

    assert result.returncode == 1
    assert "配置切换失败" in result.stdout
    assert tsf.read_text(encoding="utf-8").strip() == "123"
    assert downf.read_text(encoding="utf-8").strip() == "2"
    assert import_log.exists()
    assert not pgrep_log.exists()


def _warp_env(tmp_path, dryrun="0", systemctl_exit="0"):
    fake_bin = tmp_path / "warp-bin"
    fake_bin.mkdir()
    systemctl_log = tmp_path / "systemctl.log"
    _executable(fake_bin / "curl", "#!/bin/sh\n"
        "case \"$*\" in\n"
        "  *api.ipify.org*) echo 198.51.100.10 ;;\n"
        "  *) echo ip=192.0.2.10 ;;\n"
        "esac\n")
    _executable(fake_bin / "systemctl", "#!/bin/sh\n"
        "echo \"$*\" >> \"$WARP_WD_SYSTEMCTL_LOG\"\n"
        "exit \"${WARP_WD_SYSTEMCTL_EXIT:-0}\"\n")

    rundir = tmp_path / "warp-state"
    env = os.environ.copy()
    env.update({
        "PATH": f"{fake_bin}:/usr/bin:/bin",
        "WARP_WD_PATH": f"{fake_bin}:/usr/bin:/bin",
        "WARP_WD_LOG": str(tmp_path / "warp.log"),
        "WARP_WD_RUNDIR": str(rundir),
        "WARP_WD_HOST": "127.0.0.1",
        "WARP_WD_PORTS": "10882 10930 10978",
        "WARP_WD_FAILS_NEEDED": "1",
        "WARP_WD_COOLDOWN": "0",
        "WARP_WD_DRYRUN": str(dryrun),
        "WARP_WD_SYSTEMCTL_LOG": str(systemctl_log),
        "WARP_WD_SYSTEMCTL_EXIT": str(systemctl_exit),
    })
    return env, rundir, systemctl_log


def test_warp_watchdog_dryrun_does_not_consume_cooldown(tmp_path):
    env, rundir, systemctl_log = _warp_env(tmp_path, dryrun="1")
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/warp_tunnel_watchdog.sh")],
        env=env, text=True, capture_output=True, timeout=10,
    )

    assert result.returncode == 0
    assert "DRYRUN" in (tmp_path / "warp.log").read_text(encoding="utf-8")
    assert (rundir / "fails").read_text(encoding="utf-8").strip() == "1"
    assert not (rundir / "last").exists()
    assert not systemctl_log.exists()


def test_warp_watchdog_restart_failure_keeps_streak(tmp_path):
    env, rundir, systemctl_log = _warp_env(tmp_path, systemctl_exit="1")
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/warp_tunnel_watchdog.sh")],
        env=env, text=True, capture_output=True, timeout=10,
    )

    assert result.returncode == 0
    assert "保留连续失败 streak=1" in (tmp_path / "warp.log").read_text(encoding="utf-8")
    assert (rundir / "fails").read_text(encoding="utf-8").strip() == "1"
    assert (rundir / "last").exists()
    assert systemctl_log.read_text(encoding="utf-8").strip() == "restart sing-box"
