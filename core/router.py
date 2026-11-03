"""
core/router.py - Routing and proxy assignment engine
"""

import zlib
from typing import Optional, List
from core.models import ProxyNode, LeaseInfo, EgressType, HealthStatus
from core.storage import Storage


class Router:
    def __init__(self, storage: Storage):
        self.storage = storage

    def acquire_for_account(
        self,
        account_id: str,
        egress_type: Optional[str] = EgressType.WARP_IPV6.value,
        region: Optional[str] = None,
        force_rebind: bool = False
    ) -> Optional[LeaseInfo]:
        """
        Sticky assignment for an account.
        Deterministic mapping: crc32(account_id) % len(healthy_nodes)
        """
        # 1. Check existing lease if not forcing rebind
        if not force_rebind:
            existing = self.storage.get_lease(account_id)
            if existing:
                node = self.storage.get_node(existing.node_name)
                if node and node.status == HealthStatus.HEALTHY.value:
                    if (not egress_type or node.egress_type == egress_type) and (not region or node.region == region):
                        return existing

        # 2. Get eligible candidates
        candidates = self.storage.list_nodes(region=region, egress_type=egress_type, status=HealthStatus.HEALTHY.value)
        if not candidates:
            # Fallback to any node matching filters
            candidates = self.storage.list_nodes(region=region, egress_type=egress_type)
        if not candidates:
            # Fallback to all healthy nodes
            candidates = self.storage.list_nodes(status=HealthStatus.HEALTHY.value)
        if not candidates:
            candidates = self.storage.list_nodes()

        if not candidates:
            return None

        # Sort stably by node name
        candidates.sort(key=lambda x: x.name)

        # 3. Deterministic CRC32 Hash
        hash_val = zlib.crc32(account_id.encode("utf-8")) & 0xFFFFFFFF
        selected_idx = hash_val % len(candidates)
        selected_node = candidates[selected_idx]

        # 4. Save and return lease
        return self.storage.set_lease(account_id, selected_node.name)
