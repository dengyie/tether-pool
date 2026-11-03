"""
tests/test_api.py - Unit tests for FastAPI REST API endpoints
"""

import os
import pytest
from starlette.testclient import TestClient
from api.server import app, storage
from core.models import ProxyNode, EgressType, HealthStatus


@pytest.fixture
def client():
    # Insert a sample node into current storage
    storage.upsert_node(ProxyNode(
        name="api-test-node",
        host="10.144.144.3",
        port=10881,
        region="tokyo",
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.HEALTHY.value
    ))
    return TestClient(app)


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
    res = client.get("/v1/acquire?account_id=test_api_user")
    assert res.status_code == 200
    data = res.json()
    assert data["account_id"] == "test_api_user"
    assert "proxy_url" in data
    assert "node_name" in data


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
