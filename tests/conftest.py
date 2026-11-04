"""
tests/conftest.py - Pytest fixtures and test environment isolation
"""

import pytest
from core.storage import Storage
from core.router import Router
from core.prober import Prober
from core.metrics import MetricsCollector
from core.allocator import AllocationEngine
from adapters.cpa_adapter import CpaAdapter
import api.server


@pytest.fixture(autouse=True)
def isolate_api_storage(tmp_path, monkeypatch):
    """
    Guarantees test isolation: replaces global storage, router, prober, metrics_collector, and cpa_adapter
    in api.server with temporary instances backed by an isolated SQLite file.
    Prevents tests from mutating production data/ip_pool.db.
    """
    test_db = str(tmp_path / "test_isolated.db")
    monkeypatch.setenv("IP_POOL_DB", test_db)

    isolated_storage = Storage(db_path=test_db)
    isolated_router = Router(isolated_storage)
    isolated_prober = Prober(isolated_storage, router=isolated_router)
    isolated_metrics = MetricsCollector(isolated_storage)
    isolated_cpa = CpaAdapter(isolated_storage)
    isolated_allocator = AllocationEngine(isolated_storage)

    monkeypatch.setattr(api.server, "storage", isolated_storage)
    monkeypatch.setattr(api.server, "router", isolated_router)
    monkeypatch.setattr(api.server, "prober", isolated_prober)
    monkeypatch.setattr(api.server, "allocator", isolated_allocator)
    monkeypatch.setattr(api.server, "metrics_collector", isolated_metrics)
    monkeypatch.setattr(api.server, "cpa_adapter", isolated_cpa)

    yield isolated_storage
