"""
tests/test_hardening.py - Regression tests for the root-cause fixes of the second review.

Scope: the invariants that were violated before, one test per fix.
- dual-state-source convergence (release + startup legacy migration)
- non-destructive startup reconciliation (mark DEAD by default, hard prune opt-in)
- serialized binding critical sections under real thread-pool concurrency
- binding semantics: per-call constraints never rewrite a stored binding
- control-plane token modes: dashboard cookie handshake + /metrics protection
- retention bounds every append-only event table
"""

import threading
import yaml
import pytest
from concurrent.futures import ThreadPoolExecutor
from starlette.testclient import TestClient

import api.server
from core.models import (
    ProxyNode, HealthStatus, EgressType, BatteryEventReport, BatteryEventType,
    BatchAllocationRequest, ProbeResult,
)
from core.storage import Storage
from core.allocator import AllocationEngine
from core.router import Router


def _node(name, exit_ip="", region="tokyo", port=10881,
          egress_type=EgressType.WARP_IPV6.value, status=HealthStatus.HEALTHY.value):
    return ProxyNode(
        name=name,
        host="10.144.144.3",
        port=port,
        region=region,
        egress_type=egress_type,
        status=status,
        exit_ip=exit_ip or None,
    )


@pytest.fixture
def storage(tmp_path):
    return Storage(db_path=str(tmp_path / "hardening.db"))


# ------------------------------------------------- dual state source convergence

def test_release_clears_legacy_lease_when_last_binding_is_removed(storage):
    """Releasing the last binding must drop the lease mirror too, or the account
    would stay bound forever while no longer counting against capacity."""
    storage.upsert_node(_node("rel-node", exit_ip="2a09:bac5:aa::1"))
    storage.set_lease("acc-rel", "rel-node")
    storage.batch_save_allocations([("svc", "acc-rel", "rel-node", "2026-11-04T00:00:00")])

    assert storage.delete_allocation("svc", "acc-rel") is True

    assert storage.get_allocation("svc", "acc-rel") is None
    assert storage.get_lease("acc-rel") is None
    assert storage.count_allocations_per_node("svc") == {}


def test_release_keeps_lease_while_another_service_binding_remains(storage):
    """The lease mirror may only be dropped when *no* service still holds the account."""
    storage.upsert_node(_node("multi-rel", exit_ip="2a09:bac5:aa::2"))
    storage.batch_save_allocations([
        ("svcA", "acc-multi", "multi-rel", "2026-11-04T00:00:00"),
        ("svcB", "acc-multi", "multi-rel", "2026-11-04T00:00:01"),
    ])
    storage.set_lease("acc-multi", "multi-rel")

    storage.delete_allocation("svcA", "acc-multi")

    assert storage.get_allocation("svcA", "acc-multi") is None
    assert storage.get_allocation("svcB", "acc-multi") is not None
    # svcB remains bound, so the lease must survive and keep reporting the account as active
    assert storage.get_lease("acc-multi") is not None


def test_migrate_legacy_leases_backfills_and_is_idempotent(storage):
    """Pre-universal leases (46 vs 119 in production) must be adopted, not orphaned."""
    storage.upsert_node(_node("mig-node", exit_ip="2a09:bac5:aa::3"))
    storage.set_lease("acc-old-1", "mig-node")
    storage.set_lease("acc-old-2", "mig-node")
    # Already migrated account must not be touched again
    storage.batch_save_allocations([("default", "acc-old-2", "mig-node", "2026-11-04T00:00:00")])

    assert storage.migrate_legacy_leases() == 1
    assert storage.get_allocation("default", "acc-old-1") is not None
    assert storage.get_allocation("default", "acc-old-2") is not None
    assert storage.count_allocations_per_node("default") == {"mig-node": 2}

    # Idempotent: nothing left to adopt on the second run
    assert storage.migrate_legacy_leases() == 0
    assert storage.count_allocations_per_node("default") == {"mig-node": 2}


def test_bindings_metric_reads_allocations_not_legacy_leases(storage):
    """Monitoring used to read the lease mirror, hiding every universal binding."""
    storage.upsert_node(_node("metric-node", exit_ip="2a09:bac5:aa::4", region="osaka"))
    # 3 allocations, deliberately no lease row at all
    storage.batch_save_allocations([
        ("svc", "m1", "metric-node", "2026-11-04T00:00:00"),
        ("svc", "m2", "metric-node", "2026-11-04T00:00:01"),
        ("svc", "m3", "metric-node", "2026-11-04T00:00:02"),
    ])

    dist = storage.get_metrics_bindings_distribution()
    assert dist == [{"region": "osaka", "egress_type": EgressType.WARP_IPV6.value, "tier": "standard", "count": 3}]


# ------------------------------------------- non-destructive startup reconcile

def _write_config(path, count):
    cfg = {"nodes": [
        {"name": f"cfg-{i}", "host": "10.0.0.1", "port": 10800 + i,
         "region": "tokyo", "egress_type": EgressType.WARP_IPV6.value}
        for i in range(count)
    ]}
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return cfg


def test_startup_marks_missing_nodes_dead_and_keeps_bindings(tmp_path, monkeypatch, isolate_api_storage):
    """Default startup must be non-destructive: an operator typo in nodes.yaml can no
    longer cascade-delete a live node and all of its account bindings."""
    storage = isolate_api_storage
    storage.upsert_node(_node("orphan-node", exit_ip="198.51.100.9"))
    storage.batch_save_allocations([("svc", "acc-orphan", "orphan-node", "2026-11-04T00:00:00")])

    cfg_path = tmp_path / "nodes.yaml"
    _write_config(cfg_path, api.server.MIN_CONFIG_NODES_FOR_PRUNE)
    monkeypatch.setattr(api.server, "CONFIG_FILE", str(cfg_path))
    monkeypatch.setattr(api.server, "PRUNE_MISSING_NODES", False)

    loaded = api.server.load_initial_nodes()

    assert loaded == api.server.MIN_CONFIG_NODES_FOR_PRUNE
    orphan = storage.get_node("orphan-node")
    assert orphan is not None
    assert orphan.status == HealthStatus.DEAD.value
    # Binding survives so the self-healing re-home path can move it
    assert storage.get_allocation("svc", "acc-orphan") is not None


def test_startup_hard_prune_requires_explicit_opt_in(tmp_path, monkeypatch, isolate_api_storage):
    storage = isolate_api_storage
    storage.upsert_node(_node("orphan-node", exit_ip="198.51.100.9"))
    storage.batch_save_allocations([("svc", "acc-orphan", "orphan-node", "2026-11-04T00:00:00")])

    cfg_path = tmp_path / "nodes.yaml"
    _write_config(cfg_path, api.server.MIN_CONFIG_NODES_FOR_PRUNE)
    monkeypatch.setattr(api.server, "CONFIG_FILE", str(cfg_path))
    monkeypatch.setattr(api.server, "PRUNE_MISSING_NODES", True)

    api.server.load_initial_nodes()

    assert storage.get_node("orphan-node") is None
    assert storage.get_allocation("svc", "acc-orphan") is None


# ------------------------------------------------------- concurrency: critical sections

def test_concurrent_batch_allocate_respects_capacity(storage):
    """FastAPI runs sync endpoints in a 40-thread pool; unsynchronized read-modify-write
    used to over-commit a single exit far beyond max_accounts_per_ip."""
    storage.upsert_node(_node("race-node", exit_ip="2a09:bac5:bb::1"))
    engine = AllocationEngine(storage)
    barrier = threading.Barrier(10)

    def worker(i):
        barrier.wait()
        try:
            engine.batch_allocate(BatchAllocationRequest(
                service="race", accounts=[f"r{i}"], egress_preference=None, max_accounts_per_ip=5
            ))
            return True
        except ValueError:
            return False

    with ThreadPoolExecutor(max_workers=10) as pool:
        granted = list(pool.map(worker, range(10)))

    assert sum(1 for g in granted if g) == 5
    assert sum(storage.count_allocations_per_node("race").values()) == 5


def test_concurrent_registration_never_shares_an_exit(storage):
    """Two registrations racing the cooldown filter must not both claim the same exit."""
    for i in range(2):
        storage.upsert_node(_node(f"reg-race-{i}", exit_ip=f"203.0.113.{10 + i}", port=10881 + i))
    router = Router(storage)
    barrier = threading.Barrier(6)

    def worker(i):
        barrier.wait()
        return router.acquire_for_registration(f"acct{i}", "targetX", cooldown_seconds=1800)

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(worker, range(6)))

    granted = [r for r in results if r is not None]
    # Exactly as many grants as there are distinct exits, and no exit handed out twice
    assert len(granted) == 2
    assert len({g.exit_ip for g in granted}) == 2


def test_registration_skips_nodes_without_known_exit_ip(storage):
    """An unprobed node would bypass the cooldown filter and lock its shared mesh host."""
    storage.upsert_node(_node("unprobed", exit_ip=""))

    assert Router(storage).acquire_for_registration("acc", "targetY") is None


# ----------------------------------------------------------- binding semantics


def test_per_call_constraint_does_not_rewrite_stored_binding(storage):
    """A caller asking for a different egress type must get a node for that call only;
    the account's persisted binding must stay where it was."""
    storage.upsert_node(_node("native-only", exit_ip="2a09:bac5:cc::1",
                              egress_type=EgressType.NATIVE_IPV4.value))
    storage.upsert_node(_node("warp-only", exit_ip="2a09:bac5:cc::2",
                              egress_type=EgressType.WARP_IPV6.value))
    router = Router(storage)

    first = router.acquire_for_account("acc-c", egress_type=EgressType.NATIVE_IPV4.value, service="svc")
    assert first.node_name == "native-only"
    assert storage.get_allocation("svc", "acc-c")["node_name"] == "native-only"

    second = router.acquire_for_account("acc-c", egress_type=EgressType.WARP_IPV6.value, service="svc")
    assert second.node_name == "warp-only"
    # ...and the stored binding is untouched
    assert storage.get_allocation("svc", "acc-c")["node_name"] == "native-only"


def test_force_rebind_still_persists_the_new_binding(storage):
    """The read-only guarantee must not disable explicit rebinding (battery events rely on it)."""
    storage.upsert_node(_node("rb-a", exit_ip="2a09:bac5:cc::3"))
    storage.upsert_node(_node("rb-b", exit_ip="2a09:bac5:cc::4"))
    router = Router(storage)

    first = router.acquire_for_account("acc-rb", service="svc")
    second = router.acquire_for_account("acc-rb", service="svc", force_rebind=True,
                                       exclude_node=first.node_name)

    assert second.node_name != first.node_name
    assert storage.get_allocation("svc", "acc-rb")["node_name"] == second.node_name


# --------------------------------------------------------------- auth coherence


def test_token_mode_dashboard_handshake_and_metrics(monkeypatch):
    """With a token set, the browser flow must work (?token= -> cookie) and every
    non-/health endpoint must stay protected."""
    monkeypatch.setattr(api.server, "API_TOKEN", "s3cret")
    client = TestClient(api.server.app)

    # Open endpoints
    assert client.get("/health").status_code == 200

    # Everything else is protected, including /metrics and the dashboard HTML
    assert client.get("/v1/stats").status_code == 401
    assert client.get("/metrics").status_code == 401
    assert client.get("/dashboard").status_code == 401
    assert client.get("/v1/stats", headers={"Authorization": "Bearer nope"}).status_code == 401

    # Browser handshake: ?token= is exchanged for an HttpOnly cookie
    res = client.get("/dashboard?token=s3cret", follow_redirects=False)
    assert res.status_code == 302
    assert res.headers["location"] == "/dashboard"

    # Subsequent same-origin requests now carry the cookie
    assert client.get("/v1/stats").status_code == 200
    assert client.get("/metrics").status_code == 200

    # Header auth still works independently of the cookie
    assert client.get("/metrics", headers={"Authorization": "Bearer s3cret"}).status_code == 200


# ------------------------------------------------------------------- retention


def test_prune_event_tables_bounds_every_append_only_table(storage):
    storage.upsert_node(_node("ret-node", exit_ip="198.51.100.7"))
    storage.update_probe_result(ProbeResult(
        node_name="ret-node", success=True, exit_ip="198.51.100.7", latency_ms=5
    ))
    storage.record_circuit_event("ret-node", HealthStatus.HEALTHY.value,
                                 HealthStatus.DEGRADED.value, "probe_failed")
    storage.record_battery_event(BatteryEventReport(
        account_id="acc-ret",
        event_type=BatteryEventType.RATE_LIMITED_429.value,
        target_service="svc",
    ))
    # A bind appends a binding_changes row (R5), which is append-only and must also be
    # bounded by the same retention window.
    storage.batch_save_allocations([("svc", "acc-ret", "ret-node", "2026-11-04T00:00:00")])

    pruned = storage.prune_event_tables(keep_days=0)

    assert pruned == {
        "probe_history": 1,
        "circuit_events": 1,
        "battery_events": 1,
        "binding_changes": 1,
    }
    with storage._get_conn() as conn:
        for table in ("probe_history", "circuit_events", "battery_events", "binding_changes"):
            assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
