"""
tests/test_anti_sybil_and_battery.py - Tests for Registration Anti-Sybil Cooldown and Battery Lifecycle Hooks
"""

import pytest
from datetime import datetime, timedelta
from starlette.testclient import TestClient

from core.models import (
    ProxyNode, HealthStatus, EgressType,
    BatteryEventReport, BatteryEventType
)
from core.storage import Storage
from core.router import Router
import api.server


@pytest.fixture
def temp_storage(tmp_path):
    db_file = str(tmp_path / "test_anti_sybil.db")
    storage = Storage(db_path=db_file)
    return storage


@pytest.fixture
def clean_pool(temp_storage):
    node1 = ProxyNode(
        name="clash-001-vless-node-a",
        host="127.0.0.1",
        port=11001,
        region="tokyo",
        egress_type=EgressType.CLASH_PROXY.value,
        status=HealthStatus.HEALTHY.value,
        exit_ip="1.1.1.1"
    )
    node2 = ProxyNode(
        name="clash-002-vless-node-b",
        host="127.0.0.1",
        port=11002,
        region="tokyo",
        egress_type=EgressType.CLASH_PROXY.value,
        status=HealthStatus.HEALTHY.value,
        exit_ip="2.2.2.2"
    )
    temp_storage.upsert_node(node1)
    temp_storage.upsert_node(node2)
    return node1, node2


def test_registration_anti_sybil_cooldown_flow(temp_storage, clean_pool):
    router = Router(temp_storage)

    # 1. First registration acquires clean node
    lease1 = router.acquire_for_registration(
        account_id="bot1@domain.com",
        target_service="grok_reg",
        region="tokyo",
        cooldown_seconds=1800
    )
    assert lease1 is not None
    assert lease1.target_service == "grok_reg"
    assert lease1.cooldown_seconds == 1800
    first_exit_ip = lease1.exit_ip

    # Check cooldown persistence
    assert temp_storage.is_ip_cooled(first_exit_ip, "grok_reg") is True
    # Cooldown should NOT block a DIFFERENT target service
    assert temp_storage.is_ip_cooled(first_exit_ip, "openai_reg") is False

    # 2. Second registration for same service MUST pick a DIFFERENT clean exit IP
    lease2 = router.acquire_for_registration(
        account_id="bot2@domain.com",
        target_service="grok_reg",
        region="tokyo",
        cooldown_seconds=1800
    )
    assert lease2 is not None
    assert lease2.exit_ip != first_exit_ip
    assert lease2.exit_ip in ("1.1.1.1", "2.2.2.2")

    # 3. Release cooldown manually
    released = temp_storage.release_ip_cooldown(first_exit_ip, "grok_reg")
    assert released is True
    assert temp_storage.is_ip_cooled(first_exit_ip, "grok_reg") is False


def test_battery_event_lifecycle_and_hot_swapping(temp_storage, clean_pool):
    router = Router(temp_storage)
    node1, node2 = clean_pool

    # Initial lease assignment
    initial_lease = router.acquire_for_account(
        account_id="battery_acc_001",
        region="tokyo"
    )
    assert initial_lease is not None
    initial_node = initial_lease.node_name

    # Downstream service reports 429 Too Many Requests
    report = BatteryEventReport(
        account_id="battery_acc_001",
        node_name=initial_node,
        event_type=BatteryEventType.RATE_LIMITED_429.value,
        target_service="grok2api",
        force_rebind=True
    )
    result = router.handle_battery_event(report)
    assert result["status"] == "processed"
    assert result["battery_swapped"] is True
    assert result["new_lease"] is not None

    # Verify battery event audit log
    events = temp_storage.list_battery_events(account_id="battery_acc_001")
    assert len(events) == 1
    assert events[0]["event_type"] == BatteryEventType.RATE_LIMITED_429.value
    assert events[0]["node_name"] == initial_node


def test_api_acquire_for_registration_endpoint():
    api.server.storage.upsert_node(ProxyNode(
        name="api-anti-sybil-node",
        host="127.0.0.1",
        port=11001,
        region="tokyo",
        egress_type=EgressType.CLASH_PROXY.value,
        status=HealthStatus.HEALTHY.value,
        exit_ip="1.2.3.4"
    ))
    client = TestClient(api.server.app)

    # Call /v1/acquire_for_registration
    resp = client.get(
        "/v1/acquire_for_registration",
        params={
            "account_id": "test_bot_user@mango.com",
            "target_service": "grok_reg",
            "cooldown": 1800,
            "region": "tokyo"
        }
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["account_id"] == "test_bot_user@mango.com"
    assert data["target_service"] == "grok_reg"
    assert "exit_ip" in data
    assert "cooled_until" in data

    # Verify cooldown listed in /v1/cooldowns
    cd_resp = client.get("/v1/cooldowns", params={"target_service": "grok_reg"})
    assert cd_resp.status_code == 200
    cooldowns = cd_resp.json()
    assert any(c["target_service"] == "grok_reg" for c in cooldowns)

    # Release cooldown
    exit_ip = data["exit_ip"] or "127.0.0.1"
    rel_resp = client.post(
        "/v1/release_cooldown",
        params={"exit_ip": exit_ip, "target_service": "grok_reg"}
    )
    assert rel_resp.status_code == 200
    assert rel_resp.json()["success"] is True


def test_api_battery_event_endpoint():
    api.server.storage.upsert_node(ProxyNode(
        name="api-battery-test-node",
        host="127.0.0.1",
        port=11002,
        region="tokyo",
        egress_type=EgressType.CLASH_PROXY.value,
        status=HealthStatus.HEALTHY.value,
        exit_ip="2.3.4.5"
    ))
    client = TestClient(api.server.app)

    payload = {
        "account_id": "api_battery_user@corp.com",
        "node_name": "api-battery-test-node",
        "event_type": "rate_limited_429",
        "target_service": "cpa-manager-plus",
        "force_rebind": True
    }
    resp = client.post("/v1/battery/event", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "processed"
    assert data["battery_swapped"] is True

    # Check event retrieval
    list_resp = client.get("/v1/battery/events", params={"account_id": "api_battery_user@corp.com"})
    assert list_resp.status_code == 200
    events = list_resp.json()
    assert len(events) >= 1
    assert events[0]["event_type"] == "rate_limited_429"
