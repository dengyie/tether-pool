"""
core/router.py - Routing and proxy assignment engine with Circuit-Aware Lease Re-homing,
Anti-Sybil Cooldown for Registration Bots, and Relay Bridge Resolution
"""

import zlib
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
from core.models import (
    ProxyNode, LeaseInfo, EgressType, HealthStatus,
    RegistrationLease, BatteryEventReport, BatteryEventType, RelayMode
)
from core.storage import Storage


class Router:
    def __init__(self, storage: Storage):
        self.storage = storage

    def acquire_for_account(
        self,
        account_id: str,
        egress_type: Optional[str] = EgressType.WARP_IPV6.value,
        region: Optional[str] = None,
        force_rebind: bool = False,
        service: Optional[str] = None,
        exclude_node: Optional[str] = None
    ) -> Optional[LeaseInfo]:
        """
        Sticky assignment for an account.
        Prioritizes universal account allocations, falling back to deterministic CRC32 mapping.
        Only returns nodes in HEALTHY (Closed) state unless forced to fallback.
        Ephemeral/anonymous requests bypass disk persistence to avoid storage leak.

        `service` is normalized to "default" for both lookup and persistence so that a
        caller omitting the service can never pick up another service's binding.
        `exclude_node` prevents a forced rebind from landing back on the failing node.
        """
        svc = service or "default"

        # 1. Check existing lease if not forcing rebind
        if not force_rebind:
            # 1a. Universal Allocation Engine bindings check first
            alloc = self.storage.find_allocation_by_account(account_id, service=svc)
            if alloc:
                node = self.storage.get_node(alloc["node_name"])
                if node and node.status == HealthStatus.HEALTHY.value and node.name != exclude_node:
                    if (not egress_type or node.egress_type == egress_type) and (not region or node.region == region):
                        return LeaseInfo(
                            account_id=account_id,
                            node_name=node.name,
                            proxy_url=node.proxy_url,
                            exit_ip=node.exit_ip,
                            region=node.region,
                            egress_type=node.egress_type,
                            assigned_at=alloc["allocated_at"]
                        )

            # 1b. Legacy lease table check
            existing = self.storage.get_lease(account_id)
            if existing:
                node = self.storage.get_node(existing.node_name)
                # Ensure existing node is HEALTHY (Closed)
                if node and node.status == HealthStatus.HEALTHY.value and node.name != exclude_node:
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

        if exclude_node:
            filtered = [c for c in candidates if c.name != exclude_node]
            # Only honour the exclusion if it leaves usable options
            if filtered:
                candidates = filtered

        if not candidates:
            return None

        # Sort stably by node name
        candidates.sort(key=lambda x: x.name)

        # 3. Deterministic CRC32 Hash
        hash_val = zlib.crc32(account_id.encode("utf-8")) & 0xFFFFFFFF
        selected_idx = hash_val % len(candidates)
        selected_node = candidates[selected_idx]

        now_str = datetime.now().isoformat()

        # 4. Ephemeral/anonymous requests do NOT pollute database
        if account_id.startswith("anon_"):
            return LeaseInfo(
                account_id=account_id,
                node_name=selected_node.name,
                proxy_url=selected_node.proxy_url,
                exit_ip=selected_node.exit_ip,
                region=selected_node.region,
                egress_type=selected_node.egress_type,
                assigned_at=now_str
            )

        # 5. Persist lease and synchronize with account_allocations
        lease = self.storage.set_lease(account_id, selected_node.name)
        self.storage.batch_save_allocations([(svc, account_id, selected_node.name, now_str)])

        return lease

    def rehome_leases_for_node(self, broken_node_name: str) -> Dict[str, Any]:
        """
        Automatic Self-Healing:
        When a node's circuit breaker trips OPEN (dead), proactively rehome
        all attached account leases to healthy backup nodes in the same region.
        """
        leases = self.storage.get_leases_for_node(broken_node_name)
        stale_allocations = self.storage.get_allocations_for_node(broken_node_name)
        if not leases and not stale_allocations:
            return {"rehomed_count": 0, "allocations_rehomed_count": 0, "details": []}

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
            return {"rehomed_count": 0, "allocations_rehomed_count": 0, "details": [], "warning": "No healthy backup nodes available to rehome"}

        candidates.sort(key=lambda x: x.name)

        now_str = datetime.now().isoformat()

        # Both the legacy lease table and the universal allocation table can point at
        # the dead node; collect the union so neither is left dangling.
        lease_accounts = {lease.account_id for lease in leases}
        affected_accounts = sorted(lease_accounts | {a["account_id"] for a in stale_allocations})

        new_node_by_account: Dict[str, ProxyNode] = {}
        for acc in affected_accounts:
            hash_val = zlib.crc32(acc.encode("utf-8")) & 0xFFFFFFFF
            new_node_by_account[acc] = candidates[hash_val % len(candidates)]

        rehomed_details = []
        for acc in affected_accounts:
            new_node = new_node_by_account[acc]

            if acc in lease_accounts:
                self.storage.set_lease(acc, new_node.name)

            self.storage.record_circuit_event(
                node_name=broken_node_name,
                from_state=HealthStatus.DEAD.value,
                to_state="rehomed",
                reason=f"auto_rehome_to_{new_node.name}"
            )
            rehomed_details.append({
                "account_id": acc,
                "from_node": broken_node_name,
                "to_node": new_node.name,
                "new_proxy_url": new_node.proxy_url
            })

        # Rewrite universal allocations still pointing at the dead node (service preserved)
        allocation_records: List[Tuple[str, str, str, str]] = [
            (a["service"], a["account_id"], new_node_by_account[a["account_id"]].name, now_str)
            for a in stale_allocations
            if a["account_id"] in new_node_by_account
        ]
        if allocation_records:
            self.storage.batch_save_allocations(allocation_records)

        return {
            "rehomed_count": len(rehomed_details),
            "leases_rehomed_count": len(lease_accounts),
            "allocations_rehomed_count": len(allocation_records),
            "details": rehomed_details
        }

    def resolve_relay_hop(self, node: ProxyNode) -> Optional[ProxyNode]:
        """
        Resolves the designated relay transit hop for a node.
        If node.relay_hop_node is set, returns that node.
        Otherwise falls back to an available Hong Kong, Tokyo, or US bridge node.
        """
        if node.relay_hop_node:
            hop = self.storage.get_node(node.relay_hop_node)
            if hop and hop.status == HealthStatus.HEALTHY.value:
                return hop

        # Fallback candidate pool: healthy clash_proxy nodes in key transit hubs
        for hub_region in ["hongkong", "tokyo", "singapore", "us-west"]:
            hub_nodes = self.storage.list_nodes(
                region=hub_region,
                egress_type=EgressType.CLASH_PROXY.value,
                status=HealthStatus.HEALTHY.value
            )
            if hub_nodes:
                # Avoid picking itself as relay
                valid = [h for h in hub_nodes if h.name != node.name]
                if valid:
                    return valid[0]

        return None

    def acquire_for_registration(
        self,
        account_id: str,
        target_service: str,
        region: Optional[str] = None,
        egress_type: Optional[str] = None,
        cooldown_seconds: int = 1800
    ) -> Optional[RegistrationLease]:
        """
        Dedicated anti-sybil egress assignment for batch account registration bots.
        Ensures that allocated exit IPs have not been used for the same target service
        within the cooldown window.
        """
        self.storage.clean_expired_cooldowns()

        # 1. Fetch eligible candidates
        candidates = self.storage.list_nodes(region=region, egress_type=egress_type, status=HealthStatus.HEALTHY.value)
        if not candidates and region:
            candidates = self.storage.list_nodes(region=region, status=HealthStatus.HEALTHY.value)
        if not candidates and egress_type:
            candidates = self.storage.list_nodes(egress_type=egress_type, status=HealthStatus.HEALTHY.value)
        if not candidates:
            candidates = self.storage.list_nodes(status=HealthStatus.HEALTHY.value)
        if not candidates:
            candidates = self.storage.list_nodes(region=region, egress_type=egress_type)
        if not candidates:
            candidates = self.storage.list_nodes()

        if not candidates:
            return None

        # 2. Filter out candidates whose exit_ip is currently in cooldown for target_service
        cooled_exit_ips = {
            c.exit_ip for c in self.storage.get_active_cooldowns(target_service=target_service)
            if c.exit_ip
        }

        clean_candidates = [
            c for c in candidates
            if not c.exit_ip or c.exit_ip not in cooled_exit_ips
        ]

        if not clean_candidates:
            # Every healthy exit is cooling down for this target service.
            # Refuse the allocation instead of silently reusing a burned exit
            # (callers translate None into HTTP 503).
            return None

        clean_candidates.sort(key=lambda x: x.name)

        # 3. Hash assignment
        hash_val = zlib.crc32(f"{account_id}:{target_service}".encode("utf-8")) & 0xFFFFFFFF
        selected_idx = hash_val % len(clean_candidates)
        selected_node = clean_candidates[selected_idx]

        # 4. Lock IP cooldown
        now_str = datetime.now().isoformat()
        ip_to_lock = selected_node.exit_ip or selected_node.host
        cooldown_record = self.storage.add_ip_cooldown(
            exit_ip=ip_to_lock,
            target_service=target_service,
            node_name=selected_node.name,
            cooldown_seconds=cooldown_seconds
        )

        # 5. Persist lease
        self.storage.set_lease(account_id, selected_node.name)

        # 6. Resolve relay hop
        relay_hop = self.resolve_relay_hop(selected_node)
        relay_hop_url = f"{relay_hop.host}:{relay_hop.port}" if relay_hop else None

        return RegistrationLease(
            account_id=account_id,
            target_service=target_service,
            node_name=selected_node.name,
            proxy_url=selected_node.proxy_url,
            exit_ip=selected_node.exit_ip,
            region=selected_node.region,
            egress_type=selected_node.egress_type,
            cooldown_seconds=cooldown_seconds,
            leased_at=now_str,
            cooled_until=cooldown_record.cooled_until,
            relay_mode=selected_node.relay_mode,
            relay_hop=relay_hop_url
        )

    def handle_battery_event(self, report: BatteryEventReport) -> Dict[str, Any]:
        """
        Handles battery degradation / 429 / 401 events reported by downstream battery consumers
        (e.g., grok2api, cpa-manager-plus). Performs automatic battery swapping (lease rebind).
        """
        event_id = self.storage.record_battery_event(report)
        new_lease = None
        svc = report.target_service or "default"

        if report.force_rebind or report.event_type in (
            BatteryEventType.RATE_LIMITED_429.value,
            BatteryEventType.BANNED_401.value,
            BatteryEventType.TURNSTILE_FAILED.value
        ):
            target_region = None
            target_egress = None
            previous_node_name = None

            # 1. Check account_allocations first
            alloc = self.storage.find_allocation_by_account(report.account_id, service=svc)
            if alloc:
                old_node = self.storage.get_node(alloc["node_name"])
                if old_node:
                    target_region = old_node.region
                    target_egress = old_node.egress_type
                    previous_node_name = old_node.name
            else:
                existing_lease = self.storage.get_lease(report.account_id)
                if existing_lease:
                    old_node = self.storage.get_node(existing_lease.node_name)
                    if old_node:
                        target_region = old_node.region
                        target_egress = old_node.egress_type
                        previous_node_name = old_node.name

            new_lease = self.acquire_for_account(
                account_id=report.account_id,
                region=target_region,
                egress_type=target_egress,
                force_rebind=True,
                service=svc,
                # A swap that recomputes the same node is a no-op; exclude it.
                exclude_node=previous_node_name
            )

            # 2. Rewrite ONLY the reporting service's binding. Touching sibling
            # services would contaminate unrelated pools with this exit.
            if new_lease:
                now_str = datetime.now().isoformat()
                self.storage.batch_save_allocations([(svc, report.account_id, new_lease.node_name, now_str)])

        lease_dict = None
        if new_lease:
            lease_dict = new_lease.model_dump() if hasattr(new_lease, "model_dump") else new_lease.dict()

        return {
            "event_id": event_id,
            "status": "processed",
            "battery_swapped": new_lease is not None,
            "new_lease": lease_dict
        }

