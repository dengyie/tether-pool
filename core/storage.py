"""
core/storage.py — SQLite 存储门面 (2026-11-19 分层重构)。

原 2100+ 行 god-class 按域拆分为 mixin:
    storage_base.StorageBase   连接/事务/建表/行映射
    store_nodes.NodeStore      节点 CRUD/断路/冻结/墓碑/域隔离
    store_bindings.BindingStore  leases + account_allocations 权威绑定
    store_events.EventStore    断路/battery 事件流水
    store_bans.BanStore        IP 冷却/quarantine/ban
    store_metrics.MetricsStore runtime_metrics

对外导入面不变: `from core.storage import Storage` / `region_from_country` / `DB_PATH`。
"""
import os

from core.storage_base import StorageBase, DB_PATH as _BASE_DB_PATH, region_from_country
from core.store_nodes import NodeStore
from core.store_bindings import BindingStore
from core.store_events import EventStore
from core.store_bans import BanStore
from core.store_metrics import MetricsStore

DB_PATH = _BASE_DB_PATH  # 兼容旧导入 (环境变量 IP_POOL_DB 在 storage_base 求值)


class Storage(StorageBase, NodeStore, BindingStore, EventStore, BanStore, MetricsStore):
    """组合门面: MRO 沿用各域 mixin 的方法; 事务辅助 (_immediate_txn) 在基座。"""
    pass
