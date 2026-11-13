"""
tests/test_circuit_breaker.py - Unit tests for 4-state Circuit Breaker & Lease Re-homing
"""

import os
import tempfile
import pytest
from datetime import datetime, timedelta
from core.models import ProxyNode, ProbeResult, EgressType, HealthStatus
from core.storage import Storage
from core.router import Router
from core.circuit import CircuitBreaker


@pytest.fixture
def temp_env():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    storage = Storage(db_path=db_path)
    router = Router(storage)
    yield storage, router
    if os.path.exists(db_path):
        os.remove(db_path)


def test_circuit_state_transitions(temp_env):
    storage, router = temp_env

    node = ProxyNode(
        name="cb-tokyo-01",
        host="10.144.144.3",
        port=10881,
        region="tokyo",
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.HEALTHY.value
    )
    storage.upsert_node(node)

    # 1. First probe failure -> Transition to DEGRADED
    res1, event1 = storage.update_probe_result(ProbeResult(node_name="cb-tokyo-01", success=False, error="timeout"))
    assert event1 is not None
    assert event1.from_state == HealthStatus.HEALTHY.value
    assert event1.to_state == HealthStatus.DEGRADED.value

    n1 = storage.get_node("cb-tokyo-01")
    assert n1.status == HealthStatus.DEGRADED.value
    assert n1.consecutive_failures == 1

    # 2. Second consecutive failure -> Tripped to DEAD (Open) with cooldown
    res2, event2 = storage.update_probe_result(ProbeResult(node_name="cb-tokyo-01", success=False, error="timeout"))
    assert event2 is not None
    assert event2.from_state == HealthStatus.DEGRADED.value
    assert event2.to_state == HealthStatus.DEAD.value

    n2 = storage.get_node("cb-tokyo-01")
    assert n2.status == HealthStatus.DEAD.value
    assert n2.consecutive_failures == 2
    assert n2.next_probe_allowed_at is not None

    # Check cooldown expiration logic
    assert not CircuitBreaker.is_cooldown_expired(n2)

    # Simulate time passing beyond cooldown
    past_time = (datetime.now() - timedelta(minutes=5)).isoformat()
    n2.next_probe_allowed_at = past_time
    storage.upsert_node(n2)
    n2_updated = storage.get_node("cb-tokyo-01")
    assert CircuitBreaker.is_cooldown_expired(n2_updated)

    # 3. Successful probe -> Recovers to HEALTHY (Closed)
    res3, event3 = storage.update_probe_result(ProbeResult(node_name="cb-tokyo-01", success=True, exit_ip="2a09:bac5::1", latency_ms=100.0))
    assert event3 is not None
    assert event3.to_state == HealthStatus.HEALTHY.value

    n3 = storage.get_node("cb-tokyo-01")
    assert n3.status == HealthStatus.HEALTHY.value
    assert n3.consecutive_failures == 0
    assert n3.next_probe_allowed_at is None


def test_lease_rehoming_on_node_trip(temp_env):
    storage, router = temp_env

    # Insert 2 healthy nodes in Tokyo
    n_primary = ProxyNode(
        name="tokyo-node-primary",
        host="10.144.144.3",
        port=10881,
        region="tokyo",
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.HEALTHY.value
    )
    n_backup = ProxyNode(
        name="tokyo-node-backup",
        host="10.144.144.3",
        port=10882,
        region="tokyo",
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.HEALTHY.value
    )
    storage.upsert_node(n_primary)
    storage.upsert_node(n_backup)

    # Assign lease for user_A on tokyo-node-primary
    storage.set_lease("user_A", "tokyo-node-primary")
    storage.set_lease("user_B", "tokyo-node-primary")

    lease_a = storage.get_lease("user_A")
    assert lease_a.node_name == "tokyo-node-primary"

    # Trigger rehoming for tokyo-node-primary
    rehome_result = router.rehome_leases_for_node("tokyo-node-primary")
    assert rehome_result["rehomed_count"] == 2

    # Check that leases have migrated to tokyo-node-backup
    new_lease_a = storage.get_lease("user_A")
    new_lease_b = storage.get_lease("user_B")
    assert new_lease_a.node_name == "tokyo-node-backup"
    assert new_lease_b.node_name == "tokyo-node-backup"

    # One audit event per rehome sweep, not one per re-homed account
    events = storage.list_circuit_events(limit=10)
    rehome_events = [e for e in events if e.to_state == "rehomed"]
    assert len(rehome_events) == 1
    assert rehome_events[0].node_name == "tokyo-node-primary"
    assert "auto_rehome_2_accounts" in rehome_events[0].reason


def test_manual_circuit_reset(temp_env):
    storage, router = temp_env

    node = ProxyNode(
        name="dead-node",
        host="10.144.144.5",
        port=10881,
        region="india",
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.DEAD.value,
        consecutive_failures=5
    )
    storage.upsert_node(node)

    assert storage.get_node("dead-node").status == HealthStatus.DEAD.value

    # Reset circuit
    success = storage.reset_node_circuit("dead-node")
    assert success is True

    reset_node = storage.get_node("dead-node")
    assert reset_node.status == HealthStatus.HEALTHY.value
    assert reset_node.consecutive_failures == 0


# ---------------------------------------------------------------------------
# Canary probes are single-trial: a HALF_OPEN canary must not burn the
# caller's retry budget (each retry costs ~timeout+0.5s per canary per sweep,
# and the circuit reopens on failure either way — the retry adds no info).
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_half_open_canary_skips_retry(temp_env, monkeypatch):
    from core.prober import Prober

    storage, _router = temp_env
    node = ProxyNode(
        name="canary-sg-01",
        host="127.0.0.1",
        port=59999,
        region="sg",
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.DEAD.value,
        consecutive_failures=5,
        next_probe_allowed_at=(datetime.now() - timedelta(seconds=3600)).isoformat()
    )
    storage.upsert_node(node)

    prober = Prober(storage, timeout=0.2)
    attempts = []

    class _FailClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url):
            attempts.append(url)
            raise RuntimeError("dial refused")

    async def fake_wait_for(coro, timeout=None):
        return await coro

    monkeypatch.setattr("core.prober.httpx.AsyncClient", _FailClient)
    monkeypatch.setattr("core.prober.asyncio.wait_for", fake_wait_for)

    res = await prober.probe_node(storage.get_node("canary-sg-01"), retries=2)
    assert res.success is False
    # canary must be a single attempt despite retries=2
    assert len(attempts) == 1, f"canary retried: {len(attempts)} attempts"
    assert storage.get_node("canary-sg-01").status == HealthStatus.DEAD.value


@pytest.mark.asyncio
async def test_healthy_node_keeps_retries(temp_env, monkeypatch):
    from core.prober import Prober

    storage, _router = temp_env
    node = ProxyNode(
        name="healthy-sg-01",
        host="127.0.0.1",
        port=59998,
        region="sg",
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.HEALTHY.value
    )
    storage.upsert_node(node)

    prober = Prober(storage, timeout=0.2)
    attempts = []

    class _FailOnceClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url):
            attempts.append(url)
            if len(attempts) == 1:
                raise RuntimeError("transient")
            class _Resp:
                status_code = 200
                text = "ip=2001:db8::1\n"
            return _Resp()

    async def fake_wait_for(coro, timeout=None):
        return await coro

    monkeypatch.setattr("core.prober.httpx.AsyncClient", _FailOnceClient)
    monkeypatch.setattr("core.prober.asyncio.wait_for", fake_wait_for)

    res = await prober.probe_node(storage.get_node("healthy-sg-01"), retries=1)
    assert res.success is True
    assert len(attempts) == 2, "non-canary probes must keep the retry budget"
