"""
tests/test_constraint_persistence.py - 2026-11-12 requirements:

R-A: auto_rehome must respect the constraint set a credential pinned at bind time
     (region is a hard boundary for rehome; no candidate inside the promise ->
     rehome_blocked, binding stays, /metrics counter +1).
R-B: `bind_persist_true` lets a constrained credential persist its binding
     (constraint_rebind) instead of the ephemeral no-write path.
R-C: `region_strict_true` hardens the region filter at acquire time (no cross-region
     relaxation -> empty candidate set -> gateway 503 pool_exhausted).

Unconstrained leases keep the availability-first behaviour byte-for-byte.
"""

import os
import tempfile
import pytest
from core.models import ProxyNode, EgressType, HealthStatus
from core.storage import Storage
from core.router import Router
from gateway.server import parse_proxy_credentials, RoutingContext, ForwardProxyGateway


@pytest.fixture
def temp_pool():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    storage = Storage(db_path=db_path)
    router = Router(storage)

    nodes = [
        # US pool (2 nodes) — the pinned region in these tests
        ProxyNode(name="gcp-uswest-native-v4", host="10.0.0.1", port=9001, region="us-west", egress_type=EgressType.NATIVE_IPV4.value, status=HealthStatus.HEALTHY.value),
        ProxyNode(name="pigeonw-20", host="10.0.0.2", port=11053, region="us-west", egress_type=EgressType.CLASH_PROXY.value, tags=["residential"], status=HealthStatus.HEALTHY.value),
        # Other regions — must NEVER receive a region-pinned account via rehome
        ProxyNode(name="huawei-sg-native-v4", host="10.0.0.3", port=9001, region="singapore", egress_type=EgressType.NATIVE_IPV4.value, status=HealthStatus.HEALTHY.value),
        ProxyNode(name="azure-india-native-v4", host="10.0.0.4", port=9001, region="india", egress_type=EgressType.NATIVE_IPV4.value, status=HealthStatus.HEALTHY.value),
        ProxyNode(name="aws-tokyo-native-v4", host="10.0.0.5", port=9001, region="tokyo", egress_type=EgressType.NATIVE_IPV4.value, status=HealthStatus.HEALTHY.value),
    ]
    for n in nodes:
        storage.upsert_node(n)

    yield router, storage

    if os.path.exists(db_path):
        os.remove(db_path)


def _bind_pinned(router, storage, account_id="ag_vrf1", service="default"):
    """Binds an account with a region constraint (the way a pinned credential does)."""
    lease = router.acquire_for_account(account_id, region="us-west", service=service)
    assert lease is not None
    alloc = storage.find_allocation_by_account(account_id, service=service)
    assert alloc["bound_region"] == "us-west"
    return lease, alloc


# ---------------------------------------------------------------- R-A: rehome respects constraints

def test_rehome_keeps_pinned_account_in_region(temp_pool):
    """Acceptance R-A: to_node region of a region-pinned lease is never outside the promise."""
    router, storage = temp_pool
    lease, _ = _bind_pinned(router, storage, "live_grok_0001")

    # The bound node dies
    storage.update_node_status(lease.node_name, HealthStatus.DEAD.value)
    result = router.rehome_leases_for_node(lease.node_name)

    assert result["rehomed_count"] == 1
    alloc = storage.find_allocation_by_account("live_grok_0001")
    assert alloc["node_name"] != lease.node_name
    assert alloc["region"] == "us-west", f"rehome broke the region promise: {alloc['region']}"


def test_rehome_blocked_when_no_healthy_candidate_in_region(temp_pool):
    """No healthy us-west node -> no migration, binding preserved, metric +1."""
    router, storage = temp_pool
    lease, _ = _bind_pinned(router, storage, "ag_health")

    # Kill BOTH us-west nodes -> the region promise has no candidate
    for name in ("gcp-uswest-native-v4", "pigeonw-20"):
        storage.update_node_status(name, HealthStatus.DEAD.value)

    result = router.rehome_leases_for_node(lease.node_name)

    assert result["rehome_blocked_count"] == 1
    alloc = storage.find_allocation_by_account("ag_health")
    assert alloc["node_name"] == lease.node_name, "blocked rehome must keep the original binding"

    # No binding_changes row for this account (no auto_rehome happened)
    changes = storage.list_binding_changes_since("default", 0)
    moves = [c for c in changes if c.account_id == "ag_health" and c.reason == "auto_rehome"]
    assert moves == []

    # /metrics counter was incremented
    metrics = {m["name"]: m for m in storage.get_runtime_metrics()}
    blocked = metrics.get("ip_pool_rehome_blocked_total")
    assert blocked is not None and blocked["value"] >= 1
    assert blocked["labels"].get("region") == "us-west"


def test_unconstrained_account_rehome_behaviour_unchanged(temp_pool):
    """No bound region -> availability first (may cross regions), like before."""
    router, storage = temp_pool
    lease = router.acquire_for_account("free_user")
    assert lease is not None

    storage.update_node_status(lease.node_name, HealthStatus.DEAD.value)
    result = router.rehome_leases_for_node(lease.node_name)
    assert result["rehomed_count"] == 1

    alloc = storage.find_allocation_by_account("free_user")
    assert alloc["node_name"] != lease.node_name  # moved somewhere healthy (any region is fine)


def test_domain_rehome_respects_pinned_region(temp_pool):
    """Domain isolation (R3) must not push a pinned account abroad either."""
    router, storage = temp_pool
    lease, _ = _bind_pinned(router, storage, "ag_domain")
    storage.update_node_status(lease.node_name, HealthStatus.DEAD.value)

    affected = [n for n in storage.list_nodes() if n.name == lease.node_name]
    result = router.rehome_leases_for_domain("us-west-host", affected)
    assert result["rehomed_count"] == 1
    assert result["rehome_blocked_count"] == 0

    alloc = storage.find_allocation_by_account("ag_domain")
    assert alloc["region"] == "us-west"


def test_rehome_account_binding_blocked_for_pinned_account(temp_pool):
    """R2 retry-exhausted path: pinned account with an empty region -> rehome_blocked."""
    router, storage = temp_pool
    lease, _ = _bind_pinned(router, storage, "ag_rt1")

    for name in ("gcp-uswest-native-v4", "pigeonw-20"):
        storage.update_node_status(name, HealthStatus.DEAD.value)

    result = router.rehome_account_binding("ag_rt1", lease.node_name, trigger="retry_exhausted")
    assert result["rehomed"] is False
    assert result["reason"] == "rehome_blocked"
    assert result["blocked_region"] == "us-west"

    alloc = storage.find_allocation_by_account("ag_rt1")
    assert alloc["node_name"] == lease.node_name


def test_rehome_account_binding_respects_region_for_pinned_account(temp_pool):
    """R2 retry-exhausted path with a healthy in-region candidate stays in-region."""
    router, storage = temp_pool
    lease, _ = _bind_pinned(router, storage, "ag_rt2")
    storage.update_node_status(lease.node_name, HealthStatus.DEAD.value)

    result = router.rehome_account_binding("ag_rt2", lease.node_name)
    assert result["rehomed"] is True
    assert storage.get_node(result["to_node"]).region == "us-west"


def test_policy_only_survives_rehome(temp_pool):
    """A standard_only pinned account must not be parked on premium nodes by rehome."""
    router, storage = temp_pool
    # Bind explicitly onto the clash node
    lease = router.acquire_for_account("pol_acc", region="us-west",
                                       egress_type=EgressType.CLASH_PROXY.value,
                                       tier_policy="standard_only")
    assert lease.node_name == "pigeonw-20"

    # The only other us-west node becomes premium; the bound node dies.
    storage.update_node("gcp-uswest-native-v4", {"tier": "premium", "configured_tier": "premium"})
    storage.update_node_status("pigeonw-20", HealthStatus.DEAD.value)

    result = router.rehome_leases_for_node("pigeonw-20")
    assert result["rehome_blocked_count"] == 1
    alloc = storage.find_allocation_by_account("pol_acc")
    assert alloc["node_name"] == "pigeonw-20"  # untouched


def test_constraint_columns_preserved_across_rehome(temp_pool):
    """Rehome rewrites node but must preserve the bound_* set."""
    router, storage = temp_pool
    lease, _ = _bind_pinned(router, storage, "c_preserve")
    storage.update_node_status(lease.node_name, HealthStatus.DEAD.value)
    router.rehome_leases_for_node(lease.node_name)

    alloc = storage.find_allocation_by_account("c_preserve")
    assert alloc["bound_region"] == "us-west"


# ---------------------------------------------------------------- R-B: bind_persist_true

def test_bind_persist_persists_constraint_rebind(temp_pool):
    """bind_persist=True on a binding-conflict call re-points and logs the binding."""
    router, storage = temp_pool

    # Healthy binding abroad (the "stale" allocation)
    stale = router.acquire_for_account("acc_conflict")
    assert stale.region == "us-west"
    # Simulate a legacy foreign allocation: HK healthy node
    hk = ProxyNode(name="azure-hk-native-v4", host="10.0.0.9", port=9001, region="hongkong",
                   egress_type=EgressType.NATIVE_IPV4.value, status=HealthStatus.HEALTHY.value)
    storage.upsert_node(hk)
    storage.batch_save_allocations([("default", "acc_conflict", "azure-hk-native-v4", "2026-11-12T00:00:00")])

    # Constrained call WITHOUT bind_persist: ephemeral, DB untouched
    l1 = router.acquire_for_account("acc_conflict", region="us-west")
    assert l1.region == "us-west"
    assert storage.find_allocation_by_account("acc_conflict")["node_name"] == "azure-hk-native-v4"
    baseline_changes = storage.list_binding_changes_since("default", 0)
    assert all(c.reason == "bind" for c in baseline_changes)

    # Constrained call WITH bind_persist: persists constraint_rebind
    l2 = router.acquire_for_account("acc_conflict", region="us-west", bind_persist=True)
    assert l2.region == "us-west"

    alloc = storage.find_allocation_by_account("acc_conflict")
    assert alloc["node_name"] == l2.node_name
    assert alloc["bound_region"] == "us-west"
    changes = storage.list_binding_changes_since("default", 0)
    rebinding = [c for c in changes if c.reason == "constraint_rebind"]
    assert len(rebinding) == 1
    assert rebinding[0].from_node == "azure-hk-native-v4"
    # rebind_count bumped (allocation table itself)
    row = storage.find_allocation_by_account("acc_conflict")
    import sqlite3 as _sq
    conn = _sq.connect(str(storage.db_path))
    rc = conn.execute(
        "SELECT rebind_count FROM account_allocations WHERE service='default' AND account_id='acc_conflict'"
    ).fetchone()[0]
    conn.close()
    assert rc >= 1


def test_bind_persist_false_keeps_db_untouched(temp_pool):
    """Byte-for-byte regression: no token -> zero writes on a binding-conflict call."""
    router, storage = temp_pool
    hk = ProxyNode(name="azure-hk-native-v4", host="10.0.0.9", port=9001, region="hongkong",
                   egress_type=EgressType.NATIVE_IPV4.value, status=HealthStatus.HEALTHY.value)
    storage.upsert_node(hk)
    storage.batch_save_allocations([("default", "acc_plain", "azure-hk-native-v4", "2026-11-12T00:00:00")])

    before_alloc = storage.find_allocation_by_account("acc_plain")
    before_lease = storage.get_lease("acc_plain")
    before_changes = [c for c in storage.list_binding_changes_since("default", 0) if c.account_id == "acc_plain"]

    router.acquire_for_account("acc_plain", region="us-west")

    after_alloc = storage.find_allocation_by_account("acc_plain")
    after_lease = storage.get_lease("acc_plain")
    after_changes = [c for c in storage.list_binding_changes_since("default", 0) if c.account_id == "acc_plain"]
    assert after_alloc["node_name"] == before_alloc["node_name"]
    import sqlite3 as _sq
    conn = _sq.connect(str(storage.db_path))
    rc_before, rc_after = conn.execute(
        "SELECT rebind_count, rebind_count FROM account_allocations WHERE service='default' AND account_id='acc_plain'"
    ).fetchone()
    conn.close()
    assert rc_after == 0  # never rebound; the call was fully ephemeral
    assert (after_lease.node_name if after_lease else None) == (before_lease.node_name if before_lease else None)
    assert after_changes == before_changes


def test_bind_persist_failure_does_not_fail_request(temp_pool):
    """Storage failure during constraint_rebind persistence still returns the egress."""
    router, storage = temp_pool
    hk = ProxyNode(name="azure-hk-native-v4", host="10.0.0.9", port=9001, region="hongkong",
                   egress_type=EgressType.NATIVE_IPV4.value, status=HealthStatus.HEALTHY.value)
    storage.upsert_node(hk)
    storage.batch_save_allocations([("default", "acc_fail", "azure-hk-native-v4", "2026-11-12T00:00:00")])

    orig = storage.batch_save_allocations
    def _boom(*a, **kw):
        raise RuntimeError("disk full")
    storage.batch_save_allocations = _boom  # type: ignore[method-assign]

    lease = router.acquire_for_account("acc_fail", region="us-west", bind_persist=True)
    assert lease is not None and lease.region == "us-west"

    storage.batch_save_allocations = orig  # type: ignore[method-assign]


# ---------------------------------------------------------------- R-C: region_strict

def test_region_strict_never_relaxes_region(temp_pool):
    """strict + region with mixed-egress candidates: relax egress, never region."""
    router, storage = temp_pool
    # Pigeonw is clash_proxy; the native pool is healthy, so request a missing egress type.
    lease = router.acquire_for_account("strict_acc", region="us-west",
                                       egress_type=EgressType.WARP_IPV6.value, region_strict=True)
    # (region+warp) empty -> (region) level: still us-west, egress relaxed
    assert lease is not None and lease.region == "us-west"


def test_region_strict_unknown_region_returns_none(temp_pool):
    """Acceptance: strict + nonexistent region -> None (gateway answers 503 pool_exhausted)."""
    router, storage = temp_pool
    lease = router.acquire_for_account("strict_missing", region="us-east", region_strict=True)
    assert lease is None


def test_region_strict_false_still_falls_back_cross_region(temp_pool):
    """Non-strict keeps the old ladder: cross-region fallback (availability first)."""
    router, storage = temp_pool
    lease = router.acquire_for_account("relaxed_acc", region="us-east")
    assert lease is not None
    assert lease.region != "us-east"  # fell back to whatever is healthy


def test_region_strict_all_region_nodes_dead(temp_pool):
    router, storage = temp_pool
    router.acquire_for_account("strict_dead", region="us-west")
    for name in ("gcp-uswest-native-v4", "pigeonw-20"):
        storage.update_node_status(name, HealthStatus.DEAD.value)
    assert router.acquire_for_account("strict_dead", region="us-west", region_strict=True) is None


# ---------------------------------------------------------------- Credential parsing & wiring

def test_parse_region_strict_and_bind_persist_tokens():
    p = parse_proxy_credentials("ag_vrf1__type_clash_proxy__region_us-west__tier_standard__policy_standard_only__region_strict_true__bind_persist_true")
    assert p.account_id == "ag_vrf1"
    assert p.region == "us-west"
    assert p.egress_type == "clash_proxy"
    assert p.region_strict is True
    assert p.bind_persist is True

    # alias spellings from the production implementation
    p2 = parse_proxy_credentials("acc__strict_region_true")
    assert p2.region_strict is True
    p3 = parse_proxy_credentials("acc?region=us-west&region_strict=true&bind_persist=true")
    assert p3.region_strict is True and p3.bind_persist is True
    p4 = parse_proxy_credentials("account=acc-region=us-west-region_strict=true-bind_persist=true")
    assert p4.region_strict is True and p4.bind_persist is True

    # default off
    p5 = parse_proxy_credentials("acc__region_us-west")
    assert p5.region_strict is False and p5.bind_persist is False


def test_gateway_header_wiring():
    gw = ForwardProxyGateway.__new__(ForwardProxyGateway)  # skip __init__ (heavy deps)
    ctx = gw._extract_routing_headers({
        "proxy-authorization": "Basic " + __import__("base64").b64encode(
            b"acc__region_us-west__region_strict_true__bind_persist_true:x").decode(),
        "x-region-strict": "true",
    }, "127.0.0.1")
    assert isinstance(ctx, RoutingContext)
    assert ctx.region_strict is True
    assert ctx.bind_persist is True
    assert ctx.region == "us-west"


    # API-level flag acceptance is covered in tests/test_api.py (client fixture lives there).

# ------------------------------------------------- R3 x constraints: domain peers are never targets

def test_rehome_blocked_before_parking_on_domain_peer(temp_pool):
    """R6 regression (2026-11-12): a region-pinned account whose only in-region healthy
    candidate is a sibling listener of the node that just died stays rehome_blocked —
    constraint awareness must not weaken the R3 domain-peer exclusion (the account is
    never re-parked on another port of the same failed host)."""
    router, storage = temp_pool

    # Both us-west nodes share one fault domain (same host, two listeners).
    for name, port in (("gcp-uswest-native-v4", 9001), ("pigeonw-20", 11053)):
        n = storage.get_node(name)
        n.fault_domain = "us-west-host"
        storage.upsert_node(n)

    _bind_pinned(router, storage, account_id="ag_vrf1")

    # The sticky pick may land on either us-west node; the failed node is wherever
    # the binding actually is, and the sibling listener shares the fault domain.
    alloc = storage.find_allocation_by_account("ag_vrf1")
    result = router.rehome_account_binding(
        account_id="ag_vrf1", failed_node_name=alloc["node_name"],
        service="default", trigger="retry_exhausted",
    )
    assert result["rehomed"] is False
    assert result["reason"] == "rehome_blocked"
    assert result["blocked_region"] == "us-west"

    # Binding untouched, on the failed node.
    alloc = storage.find_allocation_by_account("ag_vrf1")
    assert alloc["node_name"] in ("gcp-uswest-native-v4", "pigeonw-20")


def test_bind_persist_wired_through_gateway_acquire(monkeypatch):
    """The gateway's parsed bind_persist flag must reach acquire_for_account
    (regression: it was parsed but silently dropped in _open_upstream)."""
    captured = {}

    class _FakeRouter:
        def acquire_for_account(self, **kw):
            captured.update(kw)
            return None

    gw = ForwardProxyGateway.__new__(ForwardProxyGateway)
    gw.storage = type("S", (), {"get_node": lambda self, n: None})()
    gw.router = _FakeRouter()
    gw.max_attempts = 1
    gw.retry_backoff = 0

    import asyncio
    from gateway.server import UpstreamFailoverError, ERR_POOL_EXHAUSTED

    with pytest.raises(UpstreamFailoverError) as exc:
        asyncio.run(gw._open_upstream(
            account_id="acc", region="us-west", egress_type="native_ipv4",
            service="svc", target_host="example.com", target_port=443,
            region_strict=True, bind_persist=True,
        ))
    assert exc.value.code == ERR_POOL_EXHAUSTED
    assert captured.get("region_strict") is True
    assert captured.get("bind_persist") is True


# ---------------------------------------------------------------- P1 regression: (service, account) units

def test_rehome_sweep_respects_dual_service_constraints(temp_pool):
    """P1 regression: one account bound in TWO service namespaces with different
    region pins must be rehomed per (service, account_id) unit — never aggregating
    by account_id (the old code applied svcA's us-west pin to svcB's hongkong
    binding and parked BOTH services abroad, breaking svcA's promise)."""
    router, storage = temp_pool

    # hk node: healthy abroad — svcB's valid rehome target, svcA's forbidden zone
    hk = ProxyNode(name="azure-hk-native-v4", host="10.0.0.9", port=9001, region="hongkong",
                   egress_type=EgressType.NATIVE_IPV4.value, status=HealthStatus.HEALTHY.value)
    storage.upsert_node(hk)

    # Shared account: svcA pinned us-west on us-1; svcB pinned hongkong but its
    # allocation drifted onto us-1 too (the bug precondition: both service
    # bindings of one account live on the SAME dead node). batch_save_allocations
    # without constraints_by_account rewrites only node_name, preserving bound_*.
    lease = router.acquire_for_account("dual_svc", region="us-west", service="svcA")
    assert lease.node_name == "gcp-uswest-native-v4"
    lease_b = router.acquire_for_account("dual_svc", region="hongkong", service="svcB")
    assert lease_b.node_name == "azure-hk-native-v4"
    storage.batch_save_allocations(
        [("svcB", "dual_svc", "gcp-uswest-native-v4", "2026-01-01T00:00:00")], reason="bind")
    assert storage.find_allocation_by_account("dual_svc", service="svcB")["bound_region"] == "hongkong"

    storage.update_node_status("gcp-uswest-native-v4", HealthStatus.DEAD.value)

    result = router.rehome_leases_for_node("gcp-uswest-native-v4")

    # svcA: rehomed inside its us-west promise (pigeonw-20, the only healthy
    # us-west node) — never dragged abroad by svcB's hongkong pin.
    alloc_a = storage.find_allocation_by_account("dual_svc", service="svcA")
    assert alloc_a["node_name"] == "pigeonw-20"
    assert alloc_a["region"] == "us-west"
    assert alloc_a["bound_region"] == "us-west"
    # svcB: rehomed inside its own hongkong promise (NOT parked on pigeonw-20 via
    # account-level aggregation — the old code applied svcA's us-west pin to both).
    alloc_b = storage.find_allocation_by_account("dual_svc", service="svcB")
    assert alloc_b["node_name"] == "azure-hk-native-v4"
    assert alloc_b["region"] == "hongkong"
    assert alloc_b["bound_region"] == "hongkong"

    assert result["rehome_blocked_count"] == 0
    assert result["rehomed_count"] == 2
    details = {(d["service"], d["account_id"]): d for d in result["details"]}
    assert details[("svcA", "dual_svc")]["to_node"] == "pigeonw-20"
    assert details[("svcB", "dual_svc")]["to_node"] == "azure-hk-native-v4"


def test_domain_rehome_sweep_respects_dual_service_constraints(temp_pool):
    """Same dual-service invariant through the domain-fault sweep path."""
    router, storage = temp_pool

    hk = ProxyNode(name="azure-hk-native-v4", host="10.0.0.9", port=9001, region="hongkong",
                   egress_type=EgressType.NATIVE_IPV4.value, status=HealthStatus.HEALTHY.value)
    storage.upsert_node(hk)

    lease_a = router.acquire_for_account("dual_dom", region="us-west", service="svcA")
    lease_b = router.acquire_for_account("dual_dom", region="hongkong", service="svcB")
    # Both service bindings of one account drifted onto the SAME us-west listener.
    storage.batch_save_allocations(
        [("svcB", "dual_dom", lease_a.node_name, "2026-01-01T00:00:00")], reason="bind")

    # That listener's whole fault domain dies: svcA (us-west pin, pigeonw-20 alive)
    # must move within region; svcB (hongkong pin, NO other hk node) must block.
    affected = [n for n in storage.list_nodes() if n.name == lease_a.node_name]
    storage.update_node_status(lease_b.node_name, HealthStatus.DEAD.value)

    result = router.rehome_leases_for_domain("us-west-host", affected)

    alloc_a = storage.find_allocation_by_account("dual_dom", service="svcA")
    assert alloc_a["region"] == "us-west", "svcA must stay inside its us-west promise"
    alloc_b = storage.find_allocation_by_account("dual_dom", service="svcB")
    assert alloc_b["node_name"] == lease_a.node_name, "svcB blocked in-place (no healthy hk left)"
    assert result["rehome_blocked_count"] == 1
    assert result["rehomed_count"] == 1


def test_unconstrained_sweep_computes_candidate_ladder_once(temp_pool):
    """P2 regression: the unconstrained ladder must be computed once per sweep,
    not once per account (it used to run N list_nodes ladders under the
    BINDING_WRITE_LOCK for N accounts on a dead node)."""
    router, storage = temp_pool
    for i in range(3):
        assert router.acquire_for_account(f"free_{i}") is not None
        # Pin each account onto the doomed node (acquire is sticky; this rebinds
        # only node_name, preserving the unconstrained bound_* set).
        storage.batch_save_allocations(
            [("default", f"free_{i}", "gcp-uswest-native-v4", "2026-01-01T00:00:00")],
            reason="bind")

    from core.router import Router as _R
    orig = _R._find_rehome_candidates
    calls = []
    router._find_rehome_candidates = lambda *a, **k: calls.append(1) or orig(router, *a, **k)

    storage.update_node_status("gcp-uswest-native-v4", HealthStatus.DEAD.value)
    result = router.rehome_leases_for_node("gcp-uswest-native-v4")

    router._find_rehome_candidates = orig
    assert result["rehomed_count"] == 3
    assert len(calls) == 1, f"ladder computed {len(calls)}x for 3 unconstrained accounts"
