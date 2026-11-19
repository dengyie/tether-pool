from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from core.models import EgressType, HealthStatus, ProxyNode
from core.storage import Storage
import teleapi_pool_refresh


def _node(name):
    return ProxyNode(
        name=name,
        host="127.0.0.1",
        port=25001,
        region="us-west",
        egress_type=EgressType.CLASH_PROXY.value,
        status=HealthStatus.HEALTHY.value,
        exit_ip="192.0.2.10",
    )


def test_retired_binding_cleanup_advances_epoch_preserves_history_and_clears_lease(tmp_path):
    db_path = str(tmp_path / "cleanup.db")
    storage = Storage(db_path=db_path)
    storage.upsert_node(_node("retired-a"))
    storage.upsert_node(_node("active-b"))
    storage.batch_save_allocations([
        ("svc-a", "account-a", "retired-a", datetime.now().isoformat()),
        ("svc-b", "account-b", "retired-a", datetime.now().isoformat()),
        ("svc-a", "account-c", "active-b", datetime.now().isoformat()),
    ])
    storage.set_lease("account-a", "retired-a")
    storage.set_lease("account-b", "retired-a")
    storage.set_lease("account-c", "active-b")
    epoch_a = storage.get_binding_epoch("svc-a")["epoch"]
    epoch_b = storage.get_binding_epoch("svc-b")["epoch"]
    prior_a = storage.list_binding_changes_since("svc-a", 0)
    prior_b = storage.list_binding_changes_since("svc-b", 0)

    removed = teleapi_pool_refresh.cleanup_retired_bindings(db_path, {"retired-a"})

    assert removed == 2
    assert storage.get_binding_epoch("svc-a")["epoch"] == epoch_a + 1
    assert storage.get_binding_epoch("svc-b")["epoch"] == epoch_b + 1
    assert storage.get_allocation("svc-a", "account-a") is None
    assert storage.get_allocation("svc-b", "account-b") is None
    assert storage.get_allocation("svc-a", "account-c") is not None
    assert storage.get_lease("account-a") is None
    assert storage.get_lease("account-b") is None
    assert storage.get_lease("account-c").node_name == "active-b"

    changes_a = storage.list_binding_changes_since("svc-a", epoch_a)
    changes_b = storage.list_binding_changes_since("svc-b", epoch_b)
    assert len(changes_a) == len(changes_b) == 1
    assert changes_a[0].account_id == "account-a"
    assert changes_a[0].from_node == "retired-a" and changes_a[0].to_node is None
    assert changes_a[0].reason == "node_retired_dangling_cleanup"
    assert changes_b[0].account_id == "account-b"
    assert len(storage.list_binding_changes_since("svc-a", 0)) == len(prior_a) + 1
    assert len(storage.list_binding_changes_since("svc-b", 0)) == len(prior_b) + 1
