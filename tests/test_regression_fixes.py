"""
tests/test_regression_fixes.py - Regression tests for defects found in the full project review.

Each test pins down one previously-broken behaviour so it cannot silently regress:
- capacity accounting across incremental batches and per real exit IP
- consistent-hash placement under capacity limits
- battery swap actually moving off the failing exit
- service isolation for lookups and battery events
- self-healing re-home updating universal allocations, not just legacy leases
- registration cooldown refusing burned exits
- circuit backoff overflow and probe_history retention
- gateway internal-header stripping
"""

import os
import tempfile

import pytest

from core.models import (
    ProxyNode, HealthStatus, EgressType,
    BatchAllocationRequest, BatteryEventReport, BatteryEventType, ProbeResult
)
from core.storage import Storage
from core.allocator import AllocationEngine
from core.router import Router
from core.circuit import CircuitBreaker, MAX_COOLDOWN_SECONDS, JITTER_MAX_SECONDS
from gateway.server import _is_internal_header


def _node(name: str, exit_ip: str = "", region: str = "tokyo", port: int = 10881) -> ProxyNode:
    return ProxyNode(
        name=name,
        host="10.144.144.3",
        port=port,
        region=region,
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.HEALTHY.value,
        exit_ip=exit_ip or None,
    )


@pytest.fixture
def storage(tmp_path):
    return Storage(db_path=str(tmp_path / "regression.db"))


# ---------------------------------------------------------------- storage path

def test_bare_filename_db_path_does_not_crash(monkeypatch, tmp_path):
    """Storage('pool.db') used to call makedirs('') -> FileNotFoundError."""
    monkeypatch.chdir(tmp_path)
    s = Storage(db_path="pool.db")
    assert os.path.exists(tmp_path / "pool.db")
    s.upsert_node(_node("bare-node"))
    assert s.get_node("bare-node") is not None


# ------------------------------------------------------- capacity accounting

def test_incremental_batches_respect_existing_occupancy(storage):
    """A second batch must count accounts already bound by the first one."""
    storage.upsert_node(_node("cap-solo"))
    engine = AllocationEngine(storage)

    first = engine.batch_allocate(BatchAllocationRequest(
        service="svc", accounts=["a1", "a2"], egress_preference=None, max_accounts_per_ip=2
    ))
    assert first.total_accounts == 2

    with pytest.raises(ValueError, match="Pool capacity exceeded"):
        engine.batch_allocate(BatchAllocationRequest(
            service="svc", accounts=["a3", "a4"], egress_preference=None, max_accounts_per_ip=2
        ))


def test_consistent_hash_respects_capacity_per_node(storage):
    """Consistent hash used to ignore max_accounts_per_ip entirely."""
    for i in range(4):
        storage.upsert_node(_node(f"ring-{i}", exit_ip=f"2a09:bac5::{i}"))

    engine = AllocationEngine(storage)
    engine.batch_allocate(BatchAllocationRequest(
        service="ring", accounts=[f"u{i}" for i in range(4)],
        strategy="consistent_hash", egress_preference=None, max_accounts_per_ip=1
    ))

    per_node = storage.count_allocations_per_node("ring")
    assert set(per_node) == {"ring-0", "ring-1", "ring-2", "ring-3"}
    assert all(count == 1 for count in per_node.values())

    with pytest.raises(ValueError, match="Pool capacity exceeded"):
        engine.batch_allocate(BatchAllocationRequest(
            service="ring", accounts=["overflow"], strategy="consistent_hash",
            egress_preference=None, max_accounts_per_ip=1
        ))


def test_capacity_is_enforced_per_exit_ip_not_per_node(storage):
    """Two listeners sharing one upstream exit IP must share the same budget."""
    storage.upsert_node(_node("shared-exit-a", exit_ip="2a09:bac5::99", port=10881))
    storage.upsert_node(_node("shared-exit-b", exit_ip="2a09:bac5::99", port=10882))

    engine = AllocationEngine(storage)
    engine.batch_allocate(BatchAllocationRequest(
        service="exit", accounts=["e1", "e2"], egress_preference=None, max_accounts_per_ip=2
    ))

    # The exit IP is saturated even though the pool has a second node with room.
    with pytest.raises(ValueError, match="Pool capacity exceeded"):
        engine.batch_allocate(BatchAllocationRequest(
            service="exit", accounts=["e3"], egress_preference=None, max_accounts_per_ip=2
        ))


# --------------------------------------------------------- router behaviours

def test_battery_event_swaps_to_a_different_node(storage):
    """force_rebind re-hashed onto the same node, making 429 handling a no-op."""
    for i in range(3):
        storage.upsert_node(_node(f"batt-{i}", exit_ip=f"2a09:bac5:1::{i}"))

    router = Router(storage)
    storage.batch_save_allocations([("svc", "acct-batt", "batt-1", "2026-11-04T00:00:00")])

    result = router.handle_battery_event(BatteryEventReport(
        account_id="acct-batt",
        event_type=BatteryEventType.RATE_LIMITED_429.value,
        target_service="svc",
        force_rebind=True,
    ))

    assert result["battery_swapped"] is True
    assert result["new_lease"]["node_name"] != "batt-1"
    assert storage.get_allocation("svc", "acct-batt")["node_name"] == result["new_lease"]["node_name"]


def test_service_scoped_lookup_does_not_leak_other_services(storage):
    """service=None must map to DEFAULT_SERVICE, not 'newest binding across all services'."""
    for i in range(3):
        storage.upsert_node(_node(f"svc-{i}", exit_ip=f"2a09:bac5:2::{i}"))
    router = Router(storage)

    storage.batch_save_allocations([
        ("svcA", "shared", "svc-0", "2026-11-04T00:00:00"),
        ("svcB", "shared", "svc-2", "2026-11-04T00:00:01"),
    ])

    lease = router.acquire_for_account("shared", service=None)
    # No 'default' binding exists, so a fresh one is created and persisted under 'default'
    assert storage.get_allocation("svcA", "shared")["node_name"] == "svc-0"
    assert storage.get_allocation("svcB", "shared")["node_name"] == "svc-2"
    assert storage.get_allocation("default", "shared")["node_name"] == lease.node_name


def test_battery_event_only_rewrites_reporting_service(storage):
    """A 429 from svcA must not silently move the same account inside svcB."""
    for i in range(3):
        storage.upsert_node(_node(f"multi-{i}", exit_ip=f"2a09:bac5:3::{i}"))
    router = Router(storage)

    storage.batch_save_allocations([
        ("svcA", "multi", "multi-0", "2026-11-04T00:00:00"),
        ("svcB", "multi", "multi-2", "2026-11-04T00:00:01"),
    ])

    router.handle_battery_event(BatteryEventReport(
        account_id="multi",
        event_type=BatteryEventType.RATE_LIMITED_429.value,
        target_service="svcA",
        force_rebind=True,
    ))

    assert storage.get_allocation("svcB", "multi")["node_name"] == "multi-2"


def test_rehome_updates_universal_allocations(storage):
    """Re-homing used to only rewrite legacy leases, leaving allocations on the dead node."""
    for i in range(3):
        storage.upsert_node(_node(f"rehome-{i}", exit_ip=f"2a09:bac5:4::{i}"))

    router = Router(storage)
    storage.batch_save_allocations([("svc", "alloc-only", "rehome-0", "2026-11-04T00:00:00")])
    storage.update_node_status("rehome-0", HealthStatus.DEAD.value)

    result = router.rehome_leases_for_node("rehome-0")

    assert result["rehomed_count"] == 1
    assert result["allocations_rehomed_count"] == 1
    assert storage.get_allocation("svc", "alloc-only")["node_name"] != "rehome-0"


def test_registration_refuses_fully_cooled_pool(storage):
    """All candidates cooled must yield None (HTTP 503), not a burned exit."""
    storage.upsert_node(_node("reg-only", exit_ip="203.0.113.7"))
    router = Router(storage)

    first = router.acquire_for_registration("reg1", "targetX", cooldown_seconds=1800)
    assert first is not None

    second = router.acquire_for_registration("reg2", "targetX", cooldown_seconds=1800)
    assert second is None


def test_ip_cooldown_rows_are_deduplicated(storage):
    """Re-locking the same (exit_ip, service) must not pile up duplicate rows."""
    for _ in range(3):
        storage.add_ip_cooldown("203.0.113.9", "svc", "node", cooldown_seconds=600)

    with storage._get_conn() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM ip_cooldowns WHERE exit_ip = ? AND target_service = ?",
            ("203.0.113.9", "svc"),
        ).fetchone()[0]
    assert count == 1


# --------------------------------------------------------------- circuit math

def test_compute_cooldown_does_not_overflow_after_long_outage():
    """2**1026 raised OverflowError and crashed the whole probe sweep."""
    for failures in (0, 1, 2, 3, 10, 1026, 10_000):
        cooldown = CircuitBreaker.compute_cooldown(failures)
        assert 0 < cooldown <= MAX_COOLDOWN_SECONDS + JITTER_MAX_SECONDS


# ------------------------------------------------------------ retention/prune

def test_prune_probe_history_bounds_growth(storage):
    storage.upsert_node(_node("hist-node", exit_ip="198.51.100.1"))
    for _ in range(20):
        storage.update_probe_result(ProbeResult(
            node_name="hist-node", success=True, exit_ip="198.51.100.1", latency_ms=10
        ))

    # keep_days=0 prunes everything older than "now", i.e. all of them
    pruned = storage.prune_probe_history(keep_days=0)
    assert pruned == 20

    with storage._get_conn() as conn:
        remaining = conn.execute("SELECT COUNT(*) FROM probe_history").fetchone()[0]
    assert remaining == 0


def test_prune_nodes_except_removes_stale_nodes_and_bindings(storage):
    storage.upsert_node(_node("keep-me", exit_ip="198.51.100.2"))
    storage.upsert_node(_node("drop-me", exit_ip="198.51.100.3"))
    storage.batch_save_allocations([("svc", "acc", "drop-me", "2026-11-04T00:00:00")])
    storage.set_lease("acc", "drop-me")

    pruned = storage.prune_nodes_except(["keep-me"])

    assert pruned == ["drop-me"]
    assert storage.get_node("drop-me") is None
    assert storage.get_node("keep-me") is not None
    assert storage.get_allocation("svc", "acc") is None
    assert storage.get_lease("acc") is None


# ------------------------------------------------------------------- gateway

@pytest.mark.parametrize("header", [
    "X-Account-Id", "x-region", "X-Egress-Type", "x-service",
    "X-Target-Service", "Proxy-Authorization", "Proxy-Connection", "Connection",
    "X-TetherPool-Trace",
])
def test_internal_routing_headers_are_stripped(header):
    """Routing/service tags must never leak to the origin server."""
    assert _is_internal_header(header) is True


@pytest.mark.parametrize("header", ["Host", "User-Agent", "Accept", "X-Requested-With", "Content-Type"])
def test_origin_headers_are_preserved(header):
    assert _is_internal_header(header) is False
