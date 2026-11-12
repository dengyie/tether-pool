"""
tests/test_api.py - Unit tests for FastAPI REST API endpoints
"""

import os
import time
import pytest
from starlette.testclient import TestClient
import api.server
from core.models import ProxyNode, EgressType, HealthStatus


@pytest.fixture
def client():
    # Insert a sample node into current isolated storage
    api.server.storage.upsert_node(ProxyNode(
        name="api-test-node",
        host="10.144.144.3",
        port=10881,
        region="tokyo",
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.HEALTHY.value
    ))
    return TestClient(api.server.app)


def test_health_endpoint(client):
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json() == {"status": "ok", "service": "ip-pool"}


def test_stats_endpoint(client):
    res = client.get("/v1/stats")
    assert res.status_code == 200
    data = res.json()
    assert "total_nodes" in data
    assert "healthy_nodes" in data
    assert "region_distribution" in data


def test_nodes_endpoint(client):
    res = client.get("/v1/nodes")
    assert res.status_code == 200
    nodes = res.json()
    assert isinstance(nodes, list)
    assert len(nodes) > 0


def test_acquire_endpoint(client):
    # Acquiring mutates state: POST creates, GET only reads.
    assert client.get("/v1/acquire?account_id=test_api_user").status_code == 404

    res = client.post("/v1/acquire?account_id=test_api_user")
    assert res.status_code == 200
    data = res.json()
    assert data["account_id"] == "test_api_user"
    assert "proxy_url" in data
    assert "node_name" in data

    # GET now returns the existing binding and must not create a new one.
    res = client.get("/v1/acquire?account_id=test_api_user")
    assert res.status_code == 200
    assert res.json()["node_name"] == data["node_name"]


def test_export_grok2api_endpoint(client):
    res = client.get("/v1/export/grok2api")
    assert res.status_code == 200
    data = res.json()
    assert "count" in data
    assert "nodes" in data


def test_export_cpa_endpoint(client):
    res = client.get("/v1/export/cpa")
    assert res.status_code == 200
    data = res.json()
    assert "proxies" in data


def test_dashboard_endpoint(client):
    res = client.get("/dashboard")
    assert res.status_code == 200
    assert "IP Pool" in res.text
    assert "text/html" in res.headers["content-type"]


def test_root_redirect(client):
    res = client.get("/", follow_redirects=False)
    assert res.status_code in (302, 307)
    assert res.headers["location"] == "/dashboard"


def test_probe_endpoint_force_and_coalesce(client, monkeypatch):
    """POST /v1/probe: force 触发必须确定性排队 (锁等待, 不撞锁丢弃);
    同窗口重复触发合并去重 (未鉴权端点防探活风暴)。"""
    calls = []

    async def fake_probe_all(concurrency=25, force=False):
        calls.append(force)
        return []

    monkeypatch.setattr(api.server.prober, "probe_all", fake_probe_all)
    monkeypatch.setattr(api.server, "_last_probe_trigger_at", 0.0)

    res = client.post("/v1/probe?force=true")
    assert res.status_code == 200
    body = res.json()
    assert body["force"] is True and body["coalesced"] is False
    assert calls == [True], "force probe must have executed (lock-wait, not skipped)"

    res2 = client.post("/v1/probe?force=true")
    assert res2.status_code == 200
    assert res2.json()["coalesced"] is True, "rapid repeat must be coalesced"
    assert calls == [True], "coalesced trigger must not run another sweep"


# ------------------------------------------------- region_strict / bind_persist API params

def test_acquire_region_strict_unknown_region_503(client):
    """region_strict=true + nonexistent region -> 503 (no cross-region fallback)."""
    res = client.post("/v1/acquire", params={
        "account_id": "api_strict", "region": "us-east", "region_strict": True,
    })
    assert res.status_code == 503


def test_acquire_bind_persist_persists_constraint_rebind(client):
    """bind_persist=true turns a binding-conflict call into a constraint_rebind."""
    from core.models import ProxyNode as PN

    api.server.storage.upsert_node(PN(
        name="api-hk-native", host="10.0.0.9", port=9001,
        region="hongkong", egress_type="native_ipv4", status="healthy",
    ))
    api.server.storage.upsert_node(PN(
        name="api-uswest-native", host="10.0.0.1", port=9002,
        region="us-west", egress_type="native_ipv4", status="healthy",
    ))
    # Stale foreign binding
    api.server.storage.batch_save_allocations([("default", "api_bp_acc", "api-hk-native", "2026-11-12T00:00:00")])

    res = client.post("/v1/acquire", params={
        "account_id": "api_bp_acc", "region": "us-west", "bind_persist": True,
    })
    assert res.status_code == 200
    assert res.json()["region"] == "us-west"

    alloc = api.server.storage.find_allocation_by_account("api_bp_acc")
    assert alloc["node_name"] == "api-uswest-native"
    assert alloc["bound_region"] == "us-west"
    changes = [c for c in api.server.storage.list_binding_changes_since("default", 0)
               if c.account_id == "api_bp_acc"]
    assert any(c.reason == "constraint_rebind" for c in changes)


def test_acquire_without_bind_persist_keeps_db_untouched(client):
    """No token -> binding-conflict call stays ephemeral (regression guard)."""
    from core.models import ProxyNode as PN

    api.server.storage.upsert_node(PN(
        name="api-hk-native", host="10.0.0.9", port=9001,
        region="hongkong", egress_type="native_ipv4", status="healthy",
    ))
    api.server.storage.upsert_node(PN(
        name="api-uswest-native", host="10.0.0.1", port=9002,
        region="us-west", egress_type="native_ipv4", status="healthy",
    ))
    api.server.storage.batch_save_allocations([("default", "api_bp_acc2", "api-hk-native", "2026-11-12T00:00:00")])

    res = client.post("/v1/acquire", params={
        "account_id": "api_bp_acc2", "region": "us-west",
    })
    assert res.status_code == 200
    alloc = api.server.storage.find_allocation_by_account("api_bp_acc2")
    assert alloc["node_name"] == "api-hk-native"  # untouched: call was ephemeral


def test_probe_pending_flag_blocks_retrigger(monkeypatch):
    """P2 regression: a sweep whose queue-wait exceeds the 15s coalesce window must
    still suppress duplicate triggers — _probe_pending stays set until it finishes.

    Drives the endpoint coroutine directly: TestClient runs background tasks to
    completion before returning, so a pending sweep can never be observed there.
    """
    import asyncio
    from fastapi import BackgroundTasks

    async def fake_probe_all(concurrency=25, force=False):
        await asyncio.sleep(0.05)  # sweep in flight across the second trigger
        return []

    monkeypatch.setattr(api.server.prober, "probe_all", fake_probe_all)
    monkeypatch.setattr(api.server, "_last_probe_trigger_at", 0.0)
    monkeypatch.setattr(api.server, "_probe_pending", False)

    async def scenario():
        first, second = BackgroundTasks(), BackgroundTasks()
        res1 = await api.server.trigger_probe(first)
        assert res1["coalesced"] is False
        assert api.server._probe_pending is True, "pending must be set while sweep in flight"
        res2 = await api.server.trigger_probe(second)
        assert res2["coalesced"] is True, "in-flight sweep must suppress a retrigger"
        for task in first.tasks:
            await task()  # run the queued sweep (its finally clears pending)
        assert api.server._probe_pending is False, "finally-block must clear pending"
        # Push the fake clock past the 15s coalesce window so the next trigger is
        # judged on pending alone (which is now False).
        monkeypatch.setattr(api.server, "_last_probe_trigger_at",
                            time.monotonic() - api.server.PROBE_TRIGGER_COALESCE_SECONDS - 1)
        third = BackgroundTasks()
        res3 = await api.server.trigger_probe(third)
        assert res3["coalesced"] is False, "cleared pending must accept a new trigger"
        for task in third.tasks:
            await task()

    asyncio.run(scenario())


def test_scheduled_and_manual_probe_mutually_coalesce(monkeypatch):
    """P3-2 regression: scheduled sweeps raise _probe_pending too, so a manual
    POST /v1/probe arriving during a scheduled sweep is coalesced; and a scheduled
    tick seeing an already-pending manual trigger skips instead of double-sweeping."""
    import asyncio
    from fastapi import BackgroundTasks

    calls = []

    async def fake_probe_all(concurrency=25, force=False):
        calls.append(force)
        await asyncio.sleep(0.05)
        return []

    monkeypatch.setattr(api.server.prober, "probe_all", fake_probe_all)
    monkeypatch.setattr(api.server, "_last_probe_trigger_at", 0.0)
    monkeypatch.setattr(api.server, "_probe_pending", False)

    async def scenario():
        # 1) Scheduled sweep in flight -> manual trigger coalesced
        sweep = asyncio.create_task(api.server._scheduled_probe_sweep())
        while not calls:  # wait until the sweep entered probe_all
            await asyncio.sleep(0.01)
        assert api.server._probe_pending is True, "scheduled sweep must raise pending"
        res = await api.server.trigger_probe(BackgroundTasks())
        assert res["coalesced"] is True, "manual trigger during scheduled sweep must coalesce"
        await sweep
        assert api.server._probe_pending is False, "finally must clear pending"

        # 2) Manual trigger pending -> scheduled tick skips (no duplicate sweep)
        # window long expired (uptime ≫ 15s) so acceptance depends on pending alone
        monkeypatch.setattr(api.server, "_last_probe_trigger_at", 0.0)
        bt = BackgroundTasks()
        res2 = await api.server.trigger_probe(bt)
        assert res2["coalesced"] is False
        assert api.server._probe_pending is True
        await api.server._scheduled_probe_sweep()  # tick while a manual trigger is pending
        assert len(calls) == 1, "scheduled tick must skip while a manual trigger is pending"
        for task in bt.tasks:
            await task()  # the accepted manual trigger itself still runs exactly once
        assert api.server._probe_pending is False
        assert len(calls) == 2

    asyncio.run(scenario())


def test_probe_coalesce_remaining_never_negative(monkeypatch):
    """P3-3: pending=True with an expired 15s window still coalesces, and the reported
    window_remaining_seconds must clamp to 0 instead of going negative."""
    import asyncio
    from fastapi import BackgroundTasks

    async def fake_probe_all(concurrency=25, force=False):
        await asyncio.sleep(0.01)
        return []

    monkeypatch.setattr(api.server.prober, "probe_all", fake_probe_all)
    monkeypatch.setattr(api.server, "_probe_pending", True)
    monkeypatch.setattr(api.server, "_last_probe_trigger_at",
                        time.monotonic() - api.server.PROBE_TRIGGER_COALESCE_SECONDS - 5)

    async def scenario():
        res = await api.server.trigger_probe(BackgroundTasks())
        assert res["coalesced"] is True
        assert res["window_remaining_seconds"] == 0.0, res

    asyncio.run(scenario())
