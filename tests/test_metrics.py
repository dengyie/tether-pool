"""
tests/test_metrics.py - Unit tests for Prometheus metrics & Circuit REST endpoints
"""

import pytest
from starlette.testclient import TestClient
import api.server
from core.models import ProxyNode, EgressType, HealthStatus


@pytest.fixture
def client():
    # Insert sample nodes into isolated storage
    api.server.storage.upsert_node(ProxyNode(
        name="metrics-tokyo-01",
        host="10.144.144.3",
        port=10881,
        region="tokyo",
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.HEALTHY.value,
        latency_ms=150.0
    ))
    return TestClient(api.server.app)


def test_prometheus_metrics_endpoint(client):
    res = client.get("/metrics")
    assert res.status_code == 200
    assert "text/plain" in res.headers["content-type"]
    text = res.text

    assert "ip_pool_up 1" in text
    assert "# HELP ip_pool_node_count" in text
    assert "# TYPE ip_pool_node_count gauge" in text
    assert "ip_pool_node_count{region=" in text

    # Verify low-cardinality enforcement: No raw internal IP in labels
    assert 'host="10.144.144.3"' not in text
    assert 'proxy_url=' not in text


def test_circuit_events_endpoint(client):
    api.server.storage.record_circuit_event(
        node_name="metrics-tokyo-01",
        from_state="healthy",
        to_state="dead",
        reason="test_timeout"
    )

    res = client.get("/v1/circuit/events")
    assert res.status_code == 200
    events = res.json()
    assert isinstance(events, list)
    assert len(events) > 0
    assert events[0]["node_name"] == "metrics-tokyo-01"


def test_circuit_reset_endpoint(client):
    res = client.post("/v1/circuit/reset?node_name=metrics-tokyo-01")
    assert res.status_code == 200
    assert "reset to healthy" in res.json()["message"]


def test_circuit_rehome_endpoint(client):
    res = client.post("/v1/circuit/rehome?node_name=metrics-tokyo-01")
    assert res.status_code == 200
    assert "rehomed_count" in res.json()
