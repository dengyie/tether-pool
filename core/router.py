"""
core/router.py - Routing and proxy assignment engine with Circuit-Aware Lease Re-homing
"""

import zlib
from typing import Optional, List, Dict, Any
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
        Only returns nodes in HEALTHY (Closed) state unless forced to fallback.
        """
        # 1. Check existing lease if not forcing rebind
        if not force_rebind:
            existing = self.storage.get_lease(account_id)
            if existing:
                node = self.storage.get_node(existing.node_name)
                # Ensure existing node is HEALTHY (Closed)
                if node and node.status == HealthStatus.HEALTHY.value:
                    if (not egress_type or node.egress_type == egress_type) and (not region or node.region == region):
                        return existing

        # 2. Get eligible candidates (strict healthy first)
        candidates = self.storage.list_nodes(region=region, egress_type=egress_type, status=HealthStatus.HEALTHY.value)
        if not candidates:
            # Fallback to any healthy node matching region
            if region:
                candidates = self.storage.list_nodes(region=region, status=HealthStatus.HEALTHY.value)
        if not candidates:
            # Fallback to all healthy nodes globally
            candidates = self.storage.list_nodes(status=HealthStatus.HEALTHY.value)
        if not candidates:
            # Emergency fallback to degraded nodes before dead
            candidates = self.storage.list_nodes(region=region, egress_type=egress_type, status=HealthStatus.DEGRADED.value)
        if not candidates:
            # Final fallback to all nodes
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

    def rehome_leases_for_node(self, broken_node_name: str) -> Dict[str, Any]:
        """
        Automatic Self-Healing:
        When a node's circuit breaker trips OPEN (dead), proactively rehome
        all attached account leases to healthy backup nodes in the same region.
        """
        leases = self.storage.get_leases_for_node(broken_node_name)
        if not leases:
            return {"rehomed_count": 0, "details": []}

        broken_node = self.storage.get_node(broken_node_name)
        target_region = broken_node.region if broken_node else None
        target_egress = broken_node.egress_type if broken_node else None

        # Find healthy candidates in the same region
        candidates = self.storage.list_nodes(
            region=target_region,
            egress_type=target_egress,
            status=HealthStatus.HEALTHY.value
        )
        if not candidates and target_region:
            candidates = self.storage.list_nodes(region=target_region, status=HealthStatus.HEALTHY.value)
        if not candidates:
            candidates = self.storage.list_nodes(status=HealthStatus.HEALTHY.value)

        # Filter out the broken node just in case
        candidates = [c for c in candidates if c.name != broken_node_name]
        if not candidates:
            return {"rehomed_count": 0, "details": [], "warning": "No healthy backup nodes available to rehome"}

        candidates.sort(key=lambda x: x.name)

        rehomed_details = []
        for lease in leases:
            hash_val = zlib.crc32(lease.account_id.encode("utf-8")) & 0xFFFFFFFF
            idx = hash_val % len(candidates)
            new_node = candidates[idx]

            self.storage.set_lease(lease.account_id, new_node.name)
            self.storage.record_circuit_event(
                node_name=broken_node_name,
                from_state=HealthStatus.DEAD.value,
                to_state="rehomed",
                reason=f"auto_rehome_to_{new_node.name}"
            )
            rehomed_details.append({
                "account_id": lease.account_id,
                "from_node": broken_node_name,
                "to_node": new_node.name,
                "new_proxy_url": new_node.proxy_url
            })

        return {"rehomed_count": len(rehomed_details), "details": rehomed_details}
