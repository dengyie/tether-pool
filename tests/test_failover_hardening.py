"""
tests/test_failover_hardening.py - Acceptance tests for docs/REQUIREMENTS_DEAD_NODE_FAILOVER.md

Covers R1-R7 and the 2026-11-07 mesh-nl incident as a regression case:
  * R1 bounded, cumulative-exclusion retry (GATEWAY_MAX_ATTEMPTS)
  * R2 retry exhaustion -> durable account rebind (idempotent / deduplicated)
  * R3 failure-domain isolation: one event + one rehome per host, any/quorum/all
  * R4 sub-5s EasyTier peer-loss detection
  * R5 binding epoch + incremental export reconciliation
  * R6 structured X-TetherPool-Error semantics
  * R7 new Prometheus metric families
"""

import asyncio
import os
import tempfile
import pytest

from core.models import ProxyNode, EgressType, HealthStatus
from core.storage import Storage
from core.router import Router
from core.metrics import MetricsCollector
from core.failover import FailureDomainIsolator, EasytierPeerMonitor
from gateway.server import (
    ForwardProxyGateway,
    UpstreamFailoverError,
    ERR_POOL_EXHAUSTED,
    ERR_BINDING_REHOMED,
    ERR_NO_HEALTHY_TARGET,
)


WARP = EgressType.WARP_IPV6.value


def _node(name, host, port, region, egress=WARP, fault_domain="", tier="standard",
          status=HealthStatus.HEALTHY.value, exit_ip=None):
    return ProxyNode(
        name=name, host=host, port=port, region=region, egress_type=egress,
        fault_domain=fault_domain, tier=tier, status=status,
        exit_ip=exit_ip or f"2a09:bac5:{abs(hash(name)) % 9999}::1",
    )


def _make_pool(storage, region="tokyo", count=3, host="10.144.144.3", fault_domain=None):
    """
    Builds a pool. With no `fault_domain`, each node is its OWN domain (the realistic
    default for independent exits); pass one explicitly to model listeners sharing a host.
    """
    nodes = []
    for i in range(count):
        name = f"{region}-warp-{i}"
        fd = fault_domain if fault_domain is not None else name
        n = _node(name, host, 10881 + i, region, fault_domain=fd)
        storage.upsert_node(n)
        nodes.append(n)
    return nodes


def _fail_probe(storage, node_name, times=1):
    from core.models import ProbeResult
    event = None
    for _ in range(times):
        _, event = storage.update_probe_result(ProbeResult(
            node_name=node_name, success=False, error="simulated_connect_timeout"
        ))
    return event


def _kill(storage, node_name):
    storage.update_node_status(node_name, HealthStatus.DEAD.value)


# --------------------------------------------------------------------------- R1

@pytest.mark.asyncio
async def test_r1_configurable_attempts_and_cumulative_exclusion(storage):
    """Attempts == configured budget; no node is ever tried twice in one request."""
    _make_pool(storage, region="tokyo", count=4, host="10.144.144.3")
    router = Router(storage)

    for configured in (1, 2, 3):
        gateway = ForwardProxyGateway(storage, router, max_attempts=configured)
        seen = []

        async def always_fail(node, target_host, target_port, timeout=15.0):
            seen.append(node.name)
            raise OSError("dead exit")

        gateway.connect_upstream_adaptive = always_fail
        with pytest.raises(UpstreamFailoverError):
            await gateway._open_upstream(
                account_id=f"r1-acct-{configured}", region="tokyo", egress_type=WARP,
                service="grok2api", target_host="example.com", target_port=443,
            )
        assert len(seen) == configured, f"expected {configured} attempts, got {seen}"
        assert len(set(seen)) == len(seen), "retried the same node twice"

    # Each node's failure was reported to the circuit breaker.
    assert storage.get_node(seen[0]).consecutive_failures >= 1


@pytest.mark.asyncio
async def test_r1_retry_excludes_all_previously_failed_nodes(storage):
    """Cumulative exclusion: the 3rd attempt must not revisit either of the first two."""
    _make_pool(storage, region="tokyo", count=3, host="10.144.144.3")
    router = Router(storage)
    gateway = ForwardProxyGateway(storage, router, max_attempts=3)
    seen = []

    async def always_fail(node, target_host, target_port, timeout=15.0):
        seen.append(node.name)
        raise OSError("dead exit")

    gateway.connect_upstream_adaptive = always_fail
    with pytest.raises(UpstreamFailoverError):
        await gateway._open_upstream(
            account_id="r1-cum", region="tokyo", egress_type=WARP, service="grok2api",
            target_host="example.com", target_port=443,
        )
    assert len(seen) == 3
    assert len(set(seen)) == 3


# --------------------------------------------------------------------------- R2

def test_r2_rehome_persists_binding_and_keeps_egress(storage):
    """After exhaustion on a dead host, the account lands on a healthy warp_ipv6 exit."""
    _make_pool(storage, region="netherlands", count=2, host="10.144.144.12",
               fault_domain="mesh-nl")
    _make_pool(storage, region="tokyo", count=2, host="10.144.144.3",
               fault_domain="aws-tokyo")
    router = Router(storage)

    storage.batch_save_allocations([("grok2api", "acct-1", "netherlands-warp-0", "2026-11-07T00:00:00")])
    _kill(storage, "netherlands-warp-0")

    result = router.rehome_account_binding(
        account_id="acct-1", failed_node_name="netherlands-warp-0",
        service="grok2api", trigger="retry_exhausted",
    )
    assert result["rehomed"] is True
    assert result["cross_region_rebind"] is True

    alloc = storage.get_allocation("grok2api", "acct-1")
    new_node = storage.get_node(alloc["node_name"])
    assert new_node.region == "tokyo"
    assert new_node.egress_type == WARP
    assert new_node.status == HealthStatus.HEALTHY.value
    assert result["epoch"] >= 1


def test_r2_rehome_prefers_egress_type_over_same_region(storage):
    """R2 acceptance: the account must stay on warp_ipv6. A same-region native_ipv4 exit
    must NOT win over a cross-region warp exit."""
    _make_pool(storage, region="netherlands", count=2, host="10.144.144.12",
               fault_domain="mesh-nl")
    # Same region as the dead host, but a different egress type.
    storage.upsert_node(_node("nl-native-0", "10.144.144.12", 10801, "netherlands",
                              egress=EgressType.NATIVE_IPV4.value))
    _make_pool(storage, region="tokyo", count=2, host="10.144.144.3",
               fault_domain="aws-tokyo")
    router = Router(storage)
    storage.batch_save_allocations([("grok2api", "acct-eg", "netherlands-warp-0", "2026-11-07T00:00:00")])
    _kill(storage, "netherlands-warp-0")

    result = router.rehome_account_binding(
        account_id="acct-eg", failed_node_name="netherlands-warp-0", service="grok2api"
    )
    assert result["rehomed"] is True
    new_node = storage.get_node(storage.get_allocation("grok2api", "acct-eg")["node_name"])
    assert new_node.egress_type == WARP
    assert new_node.region == "tokyo"


def test_r2_rehome_is_idempotent_and_deduped_per_account(storage):
    """Five concurrent exhausted calls for one account produce exactly one migration."""
    _make_pool(storage, region="netherlands", count=2, host="10.144.144.12",
               fault_domain="mesh-nl")
    _make_pool(storage, region="tokyo", count=2, host="10.144.144.3",
               fault_domain="aws-tokyo")
    router = Router(storage)
    storage.batch_save_allocations([("grok2api", "acct-2", "netherlands-warp-0", "2026-11-07T00:00:00")])
    _kill(storage, "netherlands-warp-0")

    results = [
        router.rehome_account_binding(
            account_id="acct-2", failed_node_name="netherlands-warp-0", service="grok2api"
        )
        for _ in range(5)
    ]
    moved = [r for r in results if r["rehomed"]]
    assert len(moved) == 1, f"expected exactly one migration, got {len(moved)}"

    changes = storage.list_binding_changes_since("grok2api", 0)
    acct_moves = [c for c in changes if c.account_id == "acct-2" and c.to_node != "netherlands-warp-0"]
    assert len(acct_moves) == 1


def test_r2_no_healthy_target_reports_reason(storage):
    _make_pool(storage, region="netherlands", count=2, host="10.144.144.12",
               fault_domain="mesh-nl")
    router = Router(storage)
    storage.batch_save_allocations([("grok2api", "acct-3", "netherlands-warp-0", "2026-11-07T00:00:00")])
    for i in range(2):
        storage.update_node_status(f"netherlands-warp-{i}", HealthStatus.DEAD.value)

    result = router.rehome_account_binding(
        account_id="acct-3", failed_node_name="netherlands-warp-0", service="grok2api"
    )
    assert result["rehomed"] is False
    assert result["reason"] == "no_healthy_target"


# --------------------------------------------------------------------------- R3

def test_r3_domain_trip_is_one_event_one_rehome(storage):
    """25 dead listeners on one host -> 1 domain event, 1 rehome sweep, 511 accounts moved."""
    mesh_nodes = _make_pool(storage, region="netherlands", count=25,
                            host="10.144.144.12", fault_domain="mesh-nl")
    healthy = _make_pool(storage, region="tokyo", count=25, host="10.144.144.3",
                         fault_domain="aws-tokyo")
    router = Router(storage)
    isolator = FailureDomainIsolator(storage, router, trip_policy="any")

    # 511 accounts statically bound across the dead host's 25 listeners.
    records = []
    for i in range(511):
        node = mesh_nodes[i % len(mesh_nodes)]
        records.append(("grok2api", f"grok-acct-{i}", node.name, "2026-11-07T00:00:00"))
    storage.batch_save_allocations(records)

    before_events = len(storage.list_circuit_events(limit=1000))
    result = isolator.report_unreachable(mesh_nodes[0].name, reason="easytier_peer_missing",
                                         confirmed_dead=True)

    assert result["action"] == "isolated"
    assert len(result["affected_nodes"]) == 25

    # Exactly one new circuit event for the whole sweep: the domain-level DEAD trip is
    # one event (plus one domain rehome event), never 25 per-listener transitions.
    after_events = storage.list_circuit_events(limit=1000)
    new_events = after_events[: len(after_events) - before_events]
    assert len(new_events) == 2, [e.reason for e in new_events]
    trip_events = [e for e in new_events if e.to_state == HealthStatus.DEAD.value]
    assert len(trip_events) == 1
    assert trip_events[0].node_name == "mesh-nl"
    assert "25_listeners" in trip_events[0].reason

    # One batch rehome for the domain, covering every affected account.
    rehome_events = [e for e in new_events if e.to_state == "rehomed"]
    assert len(rehome_events) == 1
    assert rehome_events[0].node_name == "mesh-nl"
    assert "511" in rehome_events[0].reason

    # Every account now points at a healthy node outside the dead domain.
    dead_names = {n.name for n in mesh_nodes}
    for i in range(511):
        alloc = storage.get_allocation("grok2api", f"grok-acct-{i}")
        assert alloc["node_name"] not in dead_names
        assert storage.get_node(alloc["node_name"]).status == HealthStatus.HEALTHY.value

    # All 25 listeners are dead, from the single sweep.
    for n in mesh_nodes:
        assert storage.get_node(n.name).status == HealthStatus.DEAD.value

    # R7: domain counter incremented.
    metrics = {(m["name"], tuple(sorted(m["labels"].items()))): m["value"]
               for m in storage.get_runtime_metrics()}
    assert metrics.get(("ip_pool_domain_trip_total", (("domain", "mesh-nl"), ("policy", "any")))) == 1


def test_r3_quorum_policy_requires_threshold(storage):
    _make_pool(storage, region="netherlands", count=4, host="10.144.144.12",
               fault_domain="mesh-nl")
    _make_pool(storage, region="tokyo", count=2, host="10.144.144.3", fault_domain="aws-tokyo")
    router = Router(storage)
    isolator = FailureDomainIsolator(storage, router, trip_policy="quorum")

    # No listener DEAD yet, and one unconfirmed failure cannot trip a quorum domain.
    pending = isolator.report_unreachable("netherlands-warp-0", confirmed_dead=True)
    assert pending["action"] == "trip_pending"


def test_r3_quorum_does_not_trip_on_single_dead_listener(storage):
    """Regression: the reporting node was counted twice, so a 4-listener domain tripped
    at 1/4 even though quorum needs 2."""
    _make_pool(storage, region="netherlands", count=4, host="10.144.144.12",
               fault_domain="mesh-nl")
    _make_pool(storage, region="tokyo", count=2, host="10.144.144.3", fault_domain="aws-tokyo")
    router = Router(storage)
    isolator = FailureDomainIsolator(storage, router, trip_policy="quorum")

    _kill(storage, "netherlands-warp-0")
    result = isolator.report_unreachable("netherlands-warp-0", confirmed_dead=True)

    assert result["action"] == "trip_pending"
    assert storage.get_node("netherlands-warp-1").status == HealthStatus.HEALTHY.value


def test_r3_all_policy_requires_every_listener(storage):
    _make_pool(storage, region="netherlands", count=2, host="10.144.144.12",
               fault_domain="mesh-nl")
    _make_pool(storage, region="tokyo", count=2, host="10.144.144.3", fault_domain="aws-tokyo")
    router = Router(storage)
    isolator = FailureDomainIsolator(storage, router, trip_policy="all")

    _kill(storage, "netherlands-warp-0")
    pending = isolator.report_unreachable("netherlands-warp-0", confirmed_dead=True)
    assert pending["action"] == "trip_pending"

    # A peer-level signal is authoritative for the WHOLE host and must trip `all`.
    tripped = isolator.report_unreachable(
        "netherlands-warp-0", confirmed_dead=True, whole_domain=True
    )
    assert tripped["action"] == "isolated"
    for i in range(2):
        assert storage.get_node(f"netherlands-warp-{i}").status == HealthStatus.DEAD.value


def test_r3_domain_rehome_is_one_atomic_batch_with_reason(storage):
    """All accounts move in one transaction, labelled `domain_rehome`, and lease-only
    accounts are adopted into `default` instead of being left dangling."""
    mesh_nodes = _make_pool(storage, region="netherlands", count=2,
                            host="10.144.144.12", fault_domain="mesh-nl")
    _make_pool(storage, region="tokyo", count=2, host="10.144.144.3", fault_domain="aws-tokyo")
    router = Router(storage)

    storage.batch_save_allocations([
        ("grok2api", f"acct-{i}", mesh_nodes[i % 2].name, "2026-11-07T00:00:00")
        for i in range(4)
    ])
    storage.set_lease("lease-only", mesh_nodes[0].name)

    isolator = FailureDomainIsolator(storage, router, trip_policy="any")
    assert isolator.report_unreachable(mesh_nodes[0].name, confirmed_dead=True)["action"] == "isolated"

    moves = [c for c in storage.list_binding_changes_since("grok2api", 0)
             if c.reason == "domain_rehome"]
    assert len(moves) == 4

    # Lease-only account got adopted and moved, not orphaned on the dead host.
    assert storage.get_allocation("default", "lease-only") is not None
    assert storage.get_lease("lease-only").node_name != mesh_nodes[0].name


def test_r4_monitor_ignores_loopback_and_unobserved_hosts(storage):
    """127.0.0.1 (51 listeners in prod) and never-seen public hosts must never be tripped
    just because they are absent from the EasyTier peer table."""
    for i in range(3):
        storage.upsert_node(_node(f"local-{i}", "127.0.0.1", 10881 + i, "local"))
    for i in range(2):
        storage.upsert_node(_node(f"public-{i}", "10.9.9.9", 10881 + i, "public"))
    _make_pool(storage, region="tokyo", count=2, host="10.144.144.3", fault_domain="aws-tokyo")
    router = Router(storage)
    isolator = FailureDomainIsolator(storage, router)
    monitor = EasytierPeerMonitor(
        storage, isolator, runner=lambda args: "10.144.144.3  aws-tokyo  1  DIRECT  1  0%\n",
    )
    monitor.miss_threshold = 2

    for _ in range(4):
        assert monitor.scan_once()["tripped"] == []

    for i in range(3):
        assert storage.get_node(f"local-{i}").status == HealthStatus.HEALTHY.value
    for i in range(2):
        assert storage.get_node(f"public-{i}").status == HealthStatus.HEALTHY.value


def test_r4_gauge_tracks_unreachable_duration(storage):
    import time as _time

    mesh_nodes = _make_pool(storage, region="netherlands", count=2,
                            host="10.144.144.12", fault_domain="mesh-nl")
    _make_pool(storage, region="tokyo", count=2, host="10.144.144.3", fault_domain="aws-tokyo")
    router = Router(storage)
    isolator = FailureDomainIsolator(storage, router)
    peer_table = {"10.144.144.12": "mesh-nl", "10.144.144.3": "aws-tokyo"}
    monitor = EasytierPeerMonitor(
        storage, isolator, runner=lambda args: "\n".join(
            f"{ip}  DIRECT  1  {host}" for ip, host in peer_table.items()
        ),
        host_by_mesh_ip={"10.144.144.12": "mesh-nl", "10.144.144.3": "aws-tokyo"},
    )
    monitor.miss_threshold = 2

    def gauge(host):
        return next((m["value"] for m in storage.get_runtime_metrics()
                     if m["name"] == "ip_pool_host_unreachable_seconds"
                     and m["labels"].get("host") == host), None)

    monitor.scan_once()
    assert gauge("mesh-nl") == 0.0

    del peer_table["10.144.144.12"]
    monitor.scan_once()
    monitor._missing_since["10.144.144.12"] = _time.monotonic() - 5.0
    monitor.scan_once()
    assert gauge("mesh-nl") >= 4.0

    peer_table["10.144.144.12"] = "mesh-nl"
    monitor.scan_once()
    assert gauge("mesh-nl") == 0.0


# --------------------------------------------------------------------------- R4


def _noop():  # keep section anchors readable
    return None

def test_r4_monitor_trips_when_host_map_label_differs_from_stored_domain(storage):
    """Regression: EASYTIER_HOST_MAP labels ("mesh-nl") are readable names, but nodes that
    omit `fault_domain` are filed under their host IP. Using the label as the storage key
    matched zero nodes, so the isolator silently did nothing and the host stayed up."""
    _make_pool(storage, region="netherlands", count=2, host="10.144.144.12",
               fault_domain="")  # no explicit fault domain -> effective key is the host IP
    _make_pool(storage, region="tokyo", count=2, host="10.144.144.3", fault_domain="")
    router = Router(storage)
    isolator = FailureDomainIsolator(storage, router)
    table = {"10.144.144.12": "mesh-nl", "10.144.144.3": "aws-tokyo"}
    monitor = EasytierPeerMonitor(
        storage, isolator,
        runner=lambda args: "\n".join(f"{ip} DIRECT 1 host" for ip in table),
        host_by_mesh_ip={"10.144.144.12": "mesh-nl"},
    )
    monitor.miss_threshold = 2

    monitor.scan_once()
    del table["10.144.144.12"]
    monitor.scan_once()
    assert len(monitor.scan_once()["tripped"]) == 1

    assert storage.get_node("netherlands-warp-0").status == HealthStatus.DEAD.value
    assert storage.get_node("netherlands-warp-1").status == HealthStatus.DEAD.value
    # The untouched host keeps running.
    assert storage.get_node("tokyo-warp-0").status == HealthStatus.HEALTHY.value


def test_r4_peer_monitor_detects_missing_host_within_two_scans(storage):
    mesh_nodes = _make_pool(storage, region="netherlands", count=3, host="10.144.144.12",
                            fault_domain="mesh-nl")
    _make_pool(storage, region="tokyo", count=2, host="10.144.144.3", fault_domain="aws-tokyo")
    router = Router(storage)
    isolator = FailureDomainIsolator(storage, router)

    peer_table = {
        "10.144.144.12": {"ipv4": "10.144.144.12", "hostname": "mesh-nl"},
        "10.144.144.3": {"ipv4": "10.144.144.3", "hostname": "aws-tokyo"},
    }

    def runner(args):
        return "\n".join(
            f"  {p['ipv4']}  DIRECT  1  {p['hostname']}" for p in peer_table.values()
        )

    monitor = EasytierPeerMonitor(
        storage, isolator, runner=runner,
        host_by_mesh_ip={"10.144.144.12": "mesh-nl", "10.144.144.3": "aws-tokyo"},
    )
    monitor.miss_threshold = 2

    # Healthy scan: nobody missing.
    assert monitor.scan_once()["tripped"] == []

    # mesh-nl peer disappears. First missing scan only arms; second trips.
    del peer_table["10.144.144.12"]
    first = monitor.scan_once()
    assert first["tripped"] == []
    second = monitor.scan_once()
    assert len(second["tripped"]) == 1
    assert second["tripped"][0]["domain"] == "mesh-nl"

    for n in mesh_nodes:
        assert storage.get_node(n.name).status == HealthStatus.DEAD.value


def test_r4_parse_peer_output_table_and_json():
    table = (
        "ipv4          hostname      cost  next_hop  path_len  loss\n"
        "10.144.144.12 mesh-nl       1     DIRECT    1         0%\n"
        "10.144.144.3  aws-tokyo     1     DIRECT    1         0%\n"
    )
    peers = EasytierPeerMonitor.parse_peer_output(table)
    assert {p["ipv4"] for p in peers} == {"10.144.144.12", "10.144.144.3"}

    js = '[{"ipv4": "10.144.144.9", "hostname": "huawei-vps"}]'
    parsed = EasytierPeerMonitor.parse_peer_output(js)
    assert parsed == [{"ipv4": "10.144.144.9", "hostname": "huawei-vps"}]


def test_r4_empty_peer_table_is_not_a_mass_kill(storage):
    """A transiently empty peer table (daemon restarting) must trip nothing."""
    _make_pool(storage, region="netherlands", count=3, host="10.144.144.12",
               fault_domain="mesh-nl")
    router = Router(storage)
    isolator = FailureDomainIsolator(storage, router)
    monitor = EasytierPeerMonitor(
        storage, isolator, runner=lambda args: "",
        host_by_mesh_ip={"10.144.144.12": "mesh-nl"},
    )
    for _ in range(5):
        assert monitor.scan_once()["tripped"] == []


# --------------------------------------------------------------------------- R6

@pytest.mark.asyncio
async def test_r6_pool_exhausted_semantics(storage):
    gateway = ForwardProxyGateway(storage, Router(storage), max_attempts=2)
    with pytest.raises(UpstreamFailoverError) as exc:
        await gateway._open_upstream(
            account_id="no-pool", region="tokyo", egress_type=WARP, service="grok2api",
            target_host="example.com", target_port=443,
        )
    assert exc.value.code == ERR_POOL_EXHAUSTED
    assert exc.value.http_status == 503


@pytest.mark.asyncio
async def test_r6_binding_rehomed_semantics(storage):
    _make_pool(storage, region="netherlands", count=2, host="10.144.144.12",
               fault_domain="mesh-nl")
    _make_pool(storage, region="tokyo", count=2, host="10.144.144.3", fault_domain="aws-tokyo")
    router = Router(storage)
    storage.batch_save_allocations([("grok2api", "acct-r6", "netherlands-warp-0", "2026-11-07T00:00:00")])

    gateway = ForwardProxyGateway(storage, router, max_attempts=1)

    async def always_fail(node, target_host, target_port, timeout=15.0):
        raise OSError("dead exit")

    gateway.connect_upstream_adaptive = always_fail
    with pytest.raises(UpstreamFailoverError) as exc:
        await gateway._open_upstream(
            account_id="acct-r6", region="netherlands", egress_type=WARP,
            service="grok2api", target_host="example.com", target_port=443,
        )
    assert exc.value.code == ERR_BINDING_REHOMED
    assert exc.value.http_status == 502
    assert exc.value.retry == 1
    # Durable binding did move.
    assert storage.get_allocation("grok2api", "acct-r6")["node_name"] != "netherlands-warp-0"


@pytest.mark.asyncio
async def test_r6_concurrent_rebind_reports_retryable_502(storage):
    """If a parallel request already moved the account off the failed node, this caller
    must be told `binding_rehomed, retry=1`, not `no_healthy_target` (503)."""
    _make_pool(storage, region="netherlands", count=2, host="10.144.144.12",
               fault_domain="mesh-nl")
    _make_pool(storage, region="tokyo", count=2, host="10.144.144.3",
               fault_domain="aws-tokyo")
    router = Router(storage)
    storage.batch_save_allocations([("grok2api", "acct-race", "netherlands-warp-0", "2026-11-07T00:00:00")])
    gateway = ForwardProxyGateway(storage, router, max_attempts=1)

    async def fail_after_parallel_rebind(node, target_host, target_port, timeout=15.0):
        # Another in-flight request wins the rebind while this dial is failing.
        storage.move_account_binding("grok2api", "acct-race", "tokyo-warp-0", reason="auto_rehome")
        raise OSError("dead exit")

    gateway.connect_upstream_adaptive = fail_after_parallel_rebind
    with pytest.raises(UpstreamFailoverError) as exc:
        await gateway._open_upstream(
            account_id="acct-race", region="netherlands", egress_type=WARP,
            service="grok2api", target_host="example.com", target_port=443,
        )
    assert exc.value.code == ERR_BINDING_REHOMED
    assert exc.value.http_status == 502
    assert exc.value.retry == 1


@pytest.mark.asyncio
async def test_r6_no_healthy_target_semantics(storage):
    _make_pool(storage, region="netherlands", count=2, host="10.144.144.12",
               fault_domain="mesh-nl")
    router = Router(storage)
    storage.batch_save_allocations([("grok2api", "acct-r6b", "netherlands-warp-0", "2026-11-07T00:00:00")])
    gateway = ForwardProxyGateway(storage, router, max_attempts=1)

    async def always_fail(node, target_host, target_port, timeout=15.0):
        raise OSError("dead exit")

    gateway.connect_upstream_adaptive = always_fail
    with pytest.raises(UpstreamFailoverError) as exc:
        await gateway._open_upstream(
            account_id="acct-r6b", region="netherlands", egress_type=WARP,
            service="grok2api", target_host="example.com", target_port=443,
        )
    assert exc.value.code == ERR_NO_HEALTHY_TARGET
    assert exc.value.http_status == 503


# --------------------------------------------------------------------------- R5

def test_r5_binding_epoch_and_incremental_export(storage):
    import api.server
    from core.models import NodeCreateRequest

    _make_pool(storage, region="netherlands", count=2, host="10.144.144.12", fault_domain="mesh-nl")
    _make_pool(storage, region="tokyo", count=2, host="10.144.144.3", fault_domain="aws-tokyo")
    router = Router(storage)

    storage.batch_save_allocations([
        ("grok2api", "a1", "tokyo-warp-0", "2026-11-07T00:00:00"),
        ("grok2api", "a2", "tokyo-warp-1", "2026-11-07T00:00:00"),
    ])
    epoch0 = api.server.get_bindings_epoch(service="grok2api")
    assert epoch0["epoch"] >= 1

    router.rehome_account_binding(
        account_id="a1", failed_node_name="tokyo-warp-0", service="grok2api"
    )
    epoch1 = api.server.get_bindings_epoch(service="grok2api")
    assert epoch1["epoch"] > epoch0["epoch"]

    delta = api.server.export_grok2api(since_epoch=epoch0["epoch"])
    assert "a1" in delta["assignments"]
    assert "a2" not in delta["assignments"]
    assert delta["epoch"] == epoch1["epoch"]


def test_r5_node_create_accepts_fault_domain(storage):
    import api.server
    from core.models import NodeCreateRequest

    req = NodeCreateRequest(
        name="fd-node-1", host="10.144.144.12", port=10881,
        region="netherlands", egress_type=WARP,
        fault_domain="mesh-nl", provider_region="leaseweb-nl",
    )
    saved = api.server.create_or_upsert_node(req)
    assert saved.fault_domain == "mesh-nl"
    assert saved.provider_region == "leaseweb-nl"


# --------------------------------------------------------------------------- R7

def test_r7_metrics_include_failover_families(storage):
    import api.server

    storage.incr_metric("ip_pool_retry_exhausted_total", {"service": "grok2api", "reason": "connect_failed"})
    storage.incr_metric("ip_pool_rehome_total", {"service": "grok2api", "trigger": "retry_exhausted"})
    storage.incr_metric("ip_pool_domain_trip_total", {"domain": "mesh-nl", "policy": "any"})
    storage.incr_metric("ip_pool_gateway_attempts_per_request_bucket", {"le": "3"}, 1)
    storage.incr_metric("ip_pool_gateway_attempts_per_request_bucket", {"le": "+Inf"}, 1)
    storage.incr_metric("ip_pool_gateway_attempts_per_request_sum", {}, 3.0)
    storage.incr_metric("ip_pool_gateway_attempts_per_request_count", {}, 1)
    storage.set_metric("ip_pool_host_unreachable_seconds", {"host": "mesh-nl"}, 4.2)

    text = api.server.get_metrics().body.decode()

    assert "ip_pool_retry_exhausted_total{reason=\"connect_failed\",service=\"grok2api\"} 1" in text
    assert "ip_pool_rehome_total{service=\"grok2api\",trigger=\"retry_exhausted\"} 1" in text
    assert 'ip_pool_domain_trip_total{domain="mesh-nl",policy="any"} 1' in text
    assert "# TYPE ip_pool_gateway_attempts_per_request histogram" in text
    assert 'ip_pool_gateway_attempts_per_request_bucket{le="3"} 1' in text
    assert "ip_pool_host_unreachable_seconds{host=\"mesh-nl\"} 4.200" in text


# ------------------------------------------------------- Incident regression

@pytest.mark.asyncio
async def test_incident_mesh_nl_pool_never_returns_dead_exit(storage):
    """
    End-to-end regression of the 2026-11-07 incident shape: 20 accounts bound to a dead
    host; the gateway must fail over to a healthy region in the same request rather than
    burning the whole budget on the same host and returning the dead exit.
    """
    dead = _make_pool(storage, region="netherlands", count=3, host="10.144.144.12",
                      fault_domain="mesh-nl")
    healthy = _make_pool(storage, region="tokyo", count=3, host="10.144.144.3",
                         fault_domain="aws-tokyo")
    for n in dead:
        _kill(storage, n.name)
    router = Router(storage)
    storage.batch_save_allocations([
        ("grok2api", f"incident-{i}", dead[i % len(dead)].name, "2026-11-07T00:00:00")
        for i in range(20)
    ])

    gateway = ForwardProxyGateway(storage, router, max_attempts=3)
    dialled = []

    async def echo(node, target_host, target_port, timeout=15.0):
        dialled.append(node.name)
        r, w = await asyncio.open_connection("127.0.0.1", echo_port)
        return r, w

    echo_server = await asyncio.start_server(
        lambda r, w: (w.close()), "127.0.0.1", 0
    )
    echo_port = echo_server.sockets[0].getsockname()[1]
    gateway.connect_upstream_adaptive = echo

    try:
        for i in range(20):
            reader, writer = await gateway._open_upstream(
                account_id=f"incident-{i}", region=None, egress_type=WARP,
                service="grok2api", target_host="example.com", target_port=443,
            )
            writer.close()
            await reader.read(1)
    finally:
        echo_server.close()
        await echo_server.wait_closed()

    dead_names = {n.name for n in dead}
    assert dialled, "no attempts were made"
    assert not (set(dialled) & dead_names), "gateway dialled a DEAD exit"
