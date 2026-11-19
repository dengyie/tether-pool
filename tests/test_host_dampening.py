"""
tests/test_host_dampening.py - 2026-11-19 warp 隧道僵死事故的回归测试:
1. host_dampen_cooldown: 同主机短窗口 mass-failure -> 新 DEAD 退避打 1/4
2. last_probe_error: 失败写节点行 / 成功清空 (P1-5 skip 语义可见化)
3. /v1/probe/host 端点: host 过滤 + 404 + force 语义
"""

import tempfile

import pytest
from fastapi.testclient import TestClient

from core.circuit import (
    CircuitBreaker,
    HOST_DEAD_CAP_FOR_SHORT_BACKOFF,
    HOST_FAILURE_WINDOW_MINUTES,
)
from core.models import EgressType, HealthStatus, ProbeResult, ProxyNode
from core.storage import Storage


@pytest.fixture
def temp_env():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    storage = Storage(db_path=db_path)
    yield storage
    from tests.conftest import cleanup_sqlite_artifacts
    cleanup_sqlite_artifacts(db_path)


def _mk(storage, name, host="10.9.9.9", port=10000,
        status=HealthStatus.HEALTHY.value, region="tokyo"):
    storage.upsert_node(ProxyNode(name=name, host=host, port=port, region=region,
                                  egress_type=EgressType.WARP_IPV6.value, status=status))
    return storage.get_node(name)


# ---------------- host_dampen_cooldown ----------------

def test_host_dampen_shortens_backoff_on_mass_failure(temp_env):
    """同主机窗口内失败 >= CAP -> 新 DEAD 退避打 1/4。"""
    storage = temp_env
    for i in range(HOST_DEAD_CAP_FOR_SHORT_BACKOFF):
        _mk(storage, f"pre-{i}", port=20000 + i)
        storage.update_probe_result(ProbeResult(
            node_name=f"pre-{i}", success=False, error="timeout"))
    with storage._get_conn() as conn:
        full = CircuitBreaker.compute_cooldown(10)
        dampened = CircuitBreaker.host_dampen_cooldown(full, conn, "10.9.9.9")
    assert dampened == round(full / 4.0, 2)


def test_host_dampen_noop_below_threshold(temp_env):
    """窗口内失败不足 CAP -> 退避不变。"""
    storage = temp_env
    for i in range(HOST_DEAD_CAP_FOR_SHORT_BACKOFF - 1):
        _mk(storage, f"few-{i}", port=21000 + i)
        storage.update_probe_result(ProbeResult(
            node_name=f"few-{i}", success=False, error="timeout"))
    with storage._get_conn() as conn:
        full = CircuitBreaker.compute_cooldown(10)
        assert CircuitBreaker.host_dampen_cooldown(full, conn, "10.9.9.9") == full


def test_host_dampen_isolated_host_no_cross_effect(temp_env):
    """其它主机的失败不传染 (host 维度隔离)。"""
    storage = temp_env
    _mk(storage, "other-host-node", host="10.8.8.8", port=22000)
    storage.update_probe_result(ProbeResult(node_name="other-host-node",
                                            success=False, error="timeout"))
    with storage._get_conn() as conn:
        full = CircuitBreaker.compute_cooldown(10)
        assert CircuitBreaker.host_dampen_cooldown(full, conn, "10.9.9.9") == full


def test_host_dampen_ignores_repeated_failures_from_same_node(temp_env):
    """同一节点多次失败不冒充多节点共同故障信号。"""
    storage = temp_env
    _mk(storage, "repeat-node")
    for _ in range(HOST_DEAD_CAP_FOR_SHORT_BACKOFF):
        storage.update_probe_result(ProbeResult(
            node_name="repeat-node", success=False, error="timeout"))
    with storage._get_conn() as conn:
        full = CircuitBreaker.compute_cooldown(10)
        assert CircuitBreaker.host_dampen_cooldown(full, conn, "10.9.9.9") == full


def test_force_probe_can_recover_frozen_but_not_disabled(temp_env):
    storage = temp_env
    frozen = _mk(storage, "frozen-force", status=HealthStatus.DEAD.value)
    frozen.tags.append("frozen")
    storage.update_node("frozen-force", {"tags": frozen.tags})
    storage.update_probe_result(ProbeResult(
        node_name="frozen-force", success=True, exit_ip="2a09:bac5::1"), force=True)
    recovered = storage.get_node("frozen-force")
    assert recovered.status == HealthStatus.DEGRADED.value
    assert "frozen" not in recovered.tags

    disabled = _mk(storage, "disabled-force", status=HealthStatus.DISABLED.value,
                    port=10001)
    storage.update_probe_result(ProbeResult(
        node_name="disabled-force", success=True, exit_ip="2a09:bac5::2"), force=True)
    unchanged = storage.get_node("disabled-force")
    assert unchanged.status == HealthStatus.DISABLED.value
    assert unchanged.consecutive_successes == 0


def test_evaluate_with_conn_applies_dampening(temp_env):
    """evaluate(conn=...) 端到端: mass-failure 主机的 tripped 退避被压短。"""
    storage = temp_env
    # 造 5 个同主机失败 (窗口内)
    for i in range(HOST_DEAD_CAP_FOR_SHORT_BACKOFF):
        _mk(storage, f"mass-{i}", port=23000 + i)
        storage.update_probe_result(ProbeResult(node_name=f"mass-{i}",
                                                success=False, error="timeout"))
    n = _mk(storage, "victim", port=24000)
    storage.update_probe_result(ProbeResult(node_name="victim",
                                            success=False, error="t1"))
    n = storage.get_node("victim")  # now DEGRADED
    with storage._get_conn() as conn:
        new_state, reason, next_allowed = CircuitBreaker.evaluate(
            n, success=False, error="t2", conn=conn)
    assert new_state == HealthStatus.DEAD.value
    full = CircuitBreaker.compute_cooldown(n.consecutive_failures)
    # 压缩验证: next_allowed 与 now 的间隔应 <= full/4 + 容差
    from datetime import datetime
    delta = (next_allowed - datetime.now()).total_seconds()
    assert delta <= full / 4.0 + 5.0


# ---------------- last_probe_error ----------------

def test_last_probe_error_written_on_failure_and_cleared_on_success(temp_env):
    storage = temp_env
    _mk(storage, "err-01", port=25000)
    storage.update_probe_result(ProbeResult(node_name="err-01",
                                            success=False, error="Timeout (>12.0s)"))
    n = storage.get_node("err-01")
    assert n.last_probe_error == "Timeout (>12.0s)"
    storage.update_probe_result(ProbeResult(node_name="err-01", success=True,
                                            exit_ip="2a09:bac5::1"))
    n = storage.get_node("err-01")
    assert n.last_probe_error is None


def test_last_probe_error_truncated_to_120(temp_env):
    storage = temp_env
    _mk(storage, "err-02", port=25001)
    storage.update_probe_result(ProbeResult(node_name="err-02",
                                            success=False, error="x" * 300))
    assert len(storage.get_node("err-02").last_probe_error) == 120


# ---------------- /v1/probe/host ----------------

def test_probe_host_endpoint_404_on_unknown_host():
    """未知 host -> 404 (不探任何节点)。"""
    import api.server
    client = TestClient(api.server.app)
    resp = client.post("/v1/probe/host?host=203.0.113.99")
    assert resp.status_code == 404
