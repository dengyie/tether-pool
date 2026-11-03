"""
tests/test_adapters.py - Unit tests for CPA, Gemini, and Playwright adapters
"""

import os
import tempfile
import pytest
import yaml
from core.models import ProxyNode, EgressType, HealthStatus
from core.storage import Storage
from core.router import Router
from adapters.cpa_adapter import CpaAdapter
from adapters.gemini_adapter import GeminiAdapter
from adapters.playwright_pool import PlaywrightProxyManager


@pytest.fixture
def temp_adapter_env():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    storage = Storage(db_path=db_path)
    router = Router(storage)

    # Insert sample nodes
    storage.upsert_node(ProxyNode(
        name="adapter-tokyo-01",
        host="10.144.144.3",
        port=10881,
        region="tokyo",
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.HEALTHY.value
    ))
    storage.upsert_node(ProxyNode(
        name="adapter-sg-01",
        host="10.144.144.13",
        port=10881,
        region="singapore",
        egress_type=EgressType.WARP_IPV6.value,
        status=HealthStatus.HEALTHY.value
    ))

    yield storage, router

    if os.path.exists(db_path):
        os.remove(db_path)


def test_cpa_adapter(temp_adapter_env):
    storage, router = temp_adapter_env
    adapter = CpaAdapter(storage=storage)

    cfg = adapter.generate_cpa_config()
    assert cfg["version"] == "1.0"
    assert cfg["proxy_count"] == 2
    assert len(cfg["proxies"]) == 2
    assert "socks5://" in cfg["proxies"][0]["url"]

    # Export to temp YAML
    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as yf:
        yaml_path = yf.name
    adapter.export_to_file(yaml_path)

    with open(yaml_path, "r", encoding="utf-8") as f:
        loaded = yaml.safe_load(f)
    assert loaded["proxy_count"] == 2
    os.remove(yaml_path)


def test_gemini_adapter(temp_adapter_env):
    storage, router = temp_adapter_env
    adapter = GeminiAdapter(storage=storage, router=router)

    res = adapter.acquire_proxy_for_account("gemini_user_1", preferred_region="tokyo")
    assert res["account_id"] == "gemini_user_1"
    assert res["region"] == "tokyo"
    assert "socks5h://" in res["proxy_url"]

    gw = adapter.get_unified_gateway_config("gemini_user_1", preferred_region="tokyo")
    assert gw["gateway_url"] == "http://127.0.0.1:9101"
    assert gw["headers"]["X-Account-Id"] == "gemini_user_1"


def test_playwright_adapter(temp_adapter_env):
    storage, router = temp_adapter_env
    manager = PlaywrightProxyManager(storage=storage, router=router)

    pw_proxy = manager.get_playwright_proxy("crawler_01", region="singapore")
    assert "socks5://" in pw_proxy["server"]
    assert "10.144.144.13" in pw_proxy["server"]

    gw_cfg = manager.get_unified_gateway_proxy("crawler_02", region="tokyo")
    assert gw_cfg["proxy"]["server"] == "http://127.0.0.1:9101"
    assert gw_cfg["extra_http_headers"]["X-Account-Id"] == "crawler_02"
    assert gw_cfg["extra_http_headers"]["X-Region"] == "tokyo"
