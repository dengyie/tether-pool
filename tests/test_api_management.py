"""
tests/test_api_management.py - Regression & API Direct Management Tests

Validates:
1. POST /v1/nodes: Dynamic programmatic creation / upsert of proxy nodes.
2. GET /v1/nodes/{name}: Direct retrieval of single node details (and 404 for missing).
3. PATCH /v1/nodes/{name}: In-place updating of node attributes (tier, weight, tags, status).
4. DELETE /v1/nodes/{name}: Node removal with automatic rehoming of active account bindings.
5. POST /v1/config/reload: Hot-reload of topology configuration without service reboot.
6. Auth protection: Bearer token enforcement on management endpoints when enabled.
"""

import os
import yaml
import pytest
from starlette.testclient import TestClient

import api.server
from core.models import HealthStatus, EgressType, Tier


@pytest.fixture
def client():
    return TestClient(api.server.app)


def test_post_and_get_node(client):
    """POST /v1/nodes creates a node and GET /v1/nodes/{name} retrieves it."""
    payload = {
        "name": "dyn-node-01",
        "host": "10.144.144.20",
        "port": 10881,
        "protocol": "socks5h",
        "region": "singapore",
        "egress_type": EgressType.WARP_IPV6.value,
        "tags": ["warp", "dynamic"],
        "weight": 120,
        "tier": Tier.STANDARD.value,
        "relay_mode": "relay_fallback"
    }

    # 1. Create node
    res = client.post("/v1/nodes", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert data["name"] == "dyn-node-01"
    assert data["host"] == "10.144.144.20"
    assert data["weight"] == 120
    assert data["tier"] == Tier.STANDARD.value
    assert data["configured_tier"] == Tier.STANDARD.value
    assert data["status"] == HealthStatus.UNPROBED.value

    # 2. Get node by name
    res_get = client.get("/v1/nodes/dyn-node-01")
    assert res_get.status_code == 200
    assert res_get.json()["name"] == "dyn-node-01"

    # 3. Missing node returns 404
    res_404 = client.get("/v1/nodes/nonexistent-node")
    assert res_404.status_code == 404


def test_patch_node_attributes(client):
    """PATCH /v1/nodes/{name} modifies specific node fields in place."""
    # Create node first
    client.post("/v1/nodes", json={
        "name": "patch-node-01",
        "host": "10.144.144.21",
        "port": 10882,
        "region": "tokyo",
        "egress_type": EgressType.NATIVE_IPV4.value,
        "weight": 100,
        "tier": Tier.STANDARD.value
    })

    # Update weight and tier to premium
    patch_res = client.patch("/v1/nodes/patch-node-01", json={
        "weight": 250,
        "tier": Tier.PREMIUM.value,
        "tags": ["upgraded", "datacenter"]
    })
    assert patch_res.status_code == 200
    updated = patch_res.json()
    assert updated["weight"] == 250
    assert updated["tier"] == Tier.PREMIUM.value
    assert updated["configured_tier"] == Tier.PREMIUM.value
    assert "upgraded" in updated["tags"]

    # Patch non-existent node returns 404
    res_404 = client.patch("/v1/nodes/ghost-node", json={"weight": 10})
    assert res_404.status_code == 404


def test_delete_node_with_rehome(client, isolate_api_storage):
    """DELETE /v1/nodes/{name} removes node and automatically re-homes attached accounts."""
    storage = isolate_api_storage

    # Create target node and backup node
    client.post("/v1/nodes", json={
        "name": "node-to-delete",
        "host": "10.144.144.30",
        "port": 10801,
        "region": "tokyo",
        "egress_type": EgressType.WARP_IPV6.value,
        "tier": Tier.STANDARD.value
    })
    client.post("/v1/nodes", json={
        "name": "node-backup",
        "host": "10.144.144.31",
        "port": 10802,
        "region": "tokyo",
        "egress_type": EgressType.WARP_IPV6.value,
        "tier": Tier.STANDARD.value
    })

    # Mark both as healthy
    storage.update_node("node-to-delete", {"status": HealthStatus.HEALTHY.value})
    storage.update_node("node-backup", {"status": HealthStatus.HEALTHY.value})

    # Bind account to node-to-delete
    storage.batch_save_allocations([("grok2api", "user_alpha", "node-to-delete", "2026-11-05T00:00:00")])
    assert storage.get_allocation("grok2api", "user_alpha")["node_name"] == "node-to-delete"

    # Delete with rehome=True
    del_res = client.delete("/v1/nodes/node-to-delete?rehome=true")
    assert del_res.status_code == 200
    del_data = del_res.json()
    assert del_data["deleted"] is True
    assert del_data["node_name"] == "node-to-delete"
    assert del_data["rehome_result"]["allocations_rehomed_count"] == 1

    # Verify node is gone
    assert client.get("/v1/nodes/node-to-delete").status_code == 404

    # Verify account was safely migrated to backup
    rehomed_alloc = storage.get_allocation("grok2api", "user_alpha")
    assert rehomed_alloc is not None
    assert rehomed_alloc["node_name"] == "node-backup"


def test_config_hot_reload(client, tmp_path, monkeypatch):
    """POST /v1/config/reload dynamically reloads topology from YAML."""
    cfg_path = tmp_path / "reload_nodes.yaml"
    node_list = [
        {
            "name": f"reload-node-{i}",
            "host": "10.144.144.50",
            "port": 10900 + i,
            "region": "tokyo",
            "egress_type": EgressType.WARP_IPV6.value,
            "tier": Tier.STANDARD.value
        }
        for i in range(12)
    ]
    cfg_path.write_text(yaml.safe_dump({"nodes": node_list}), encoding="utf-8")
    monkeypatch.setattr(api.server, "CONFIG_FILE", str(cfg_path))

    res = client.post("/v1/config/reload")
    assert res.status_code == 200
    data = res.json()
    assert data["reloaded"] is True
    assert data["nodes_loaded"] == 12

    # Verify nodes are accessible via API
    node_res = client.get("/v1/nodes/reload-node-0")
    assert node_res.status_code == 200
    assert node_res.json()["name"] == "reload-node-0"


def test_auth_token_protects_management_endpoints(client, monkeypatch):
    """When API_TOKEN is configured, all management endpoints require authentication."""
    monkeypatch.setattr(api.server, "API_TOKEN", "mgmt_secret_token")

    # Unauthorized requests fail with 401
    assert client.get("/v1/nodes/some-node").status_code == 401
    assert client.post("/v1/nodes", json={"name": "test"}).status_code == 401
    assert client.patch("/v1/nodes/some-node", json={"weight": 10}).status_code == 401
    assert client.delete("/v1/nodes/some-node").status_code == 401
    assert client.post("/v1/config/reload").status_code == 401

    # Authorized requests with Bearer token succeed
    headers = {"Authorization": "Bearer mgmt_secret_token"}
    res = client.post("/v1/nodes", json={
        "name": "auth-node-01",
        "host": "10.144.144.90",
        "port": 10881,
        "region": "tokyo",
        "egress_type": EgressType.WARP_IPV6.value
    }, headers=headers)
    assert res.status_code == 200

    assert client.get("/v1/nodes/auth-node-01", headers=headers).status_code == 200
