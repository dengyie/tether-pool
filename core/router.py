"""
core/router.py - Routing and proxy assignment engine with Circuit-Aware Lease Re-homing,
Anti-Sybil Cooldown for Registration Bots, and Relay Bridge Resolution
"""

import zlib
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
from core.models import (
    ProxyNode, LeaseInfo, EgressType, HealthStatus, Tier, TierPolicy,
    RegistrationLease, BatteryEventReport, BatteryEventType
)
from core.storage import Storage
from core.locks import BINDING_WRITE_LOCK


class Router:
    def __init__(self, storage: Storage):
        self.storage = storage

    def acquire_for_account(
        self,
        account_id: str,
        egress_type: Optional[str] = EgressType.WARP_IPV6.value,
        region: Optional[str] = None,
        tier: Optional[str] = None,
        tier_policy: Optional[str] = None,
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

        A caller-supplied region/egress/tier filter is a *per-call* constraint, not a reason to
        rewrite an existing binding: when a healthy binding exists but does not satisfy the
        filter, a constrained node is returned ephemerally and the stored binding is left
        untouched. Only an explicit `force_rebind` (or the absence of any binding) persists
        a new mapping, so a plain read can never silently re-point an account.
        """
        svc = service or "default"
        binding_conflict = False

        # Determine effective tier policy
        # Default is standard_first (protects self-hosted/premium IPs).
        if tier_policy:
            policy = tier_policy.lower()
        elif tier == Tier.PREMIUM.value:
            policy = TierPolicy.PREMIUM_FIRST.value
        elif tier == Tier.STANDARD.value:
            policy = TierPolicy.STANDARD_FIRST.value
        else:
            policy = TierPolicy.STANDARD_FIRST.value

        def _node_matches_constraints(node: ProxyNode) -> bool:
            if egress_type and node.egress_type != egress_type:
                return False
            if region and node.region != region:
                return False
            if policy == TierPolicy.PREMIUM_ONLY.value and node.tier != Tier.PREMIUM.value:
                return False
            if policy == TierPolicy.STANDARD_ONLY.value and node.tier != Tier.STANDARD.value:
                return False
            if tier and node.tier != tier:
                return False
            return True

        # 1. Check existing lease if not forcing rebind
        if not force_rebind:
            # 1a. Universal Allocation Engine bindings check first
            alloc = self.storage.find_allocation_by_account(account_id, service=svc)
            if alloc:
                node = self.storage.get_node(alloc["node_name"])
                if node and node.status == HealthStatus.HEALTHY.value and node.name != exclude_node:
                    if _node_matches_constraints(node):
                        return LeaseInfo(
                            account_id=account_id,
                            node_name=node.name,
                            proxy_url=node.proxy_url,
                            exit_ip=node.exit_ip,
                            region=node.region,
                            egress_type=node.egress_type,
                            assigned_at=alloc["allocated_at"],
                            tier=node.tier
                        )
                    # Healthy binding that only fails this call's filter -> keep it.
                    binding_conflict = True

            # 1b. Legacy lease table check
            existing = self.storage.get_lease(account_id)
            if existing:
                node = self.storage.get_node(existing.node_name)
                # Ensure existing node is HEALTHY (Closed)
                if node and node.status == HealthStatus.HEALTHY.value and node.name != exclude_node:
                    if _node_matches_constraints(node):
                        return existing
                    binding_conflict = True

        # 2. Get eligible candidates (tier-aware ladder search)
        candidates = self._get_candidates_for_tier_policy(
            region=region,
            egress_type=egress_type,
            policy=policy
        )

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

        # 4. Ephemeral cases do NOT pollute database: anonymous accounts, and accounts
        # whose stored binding only failed this call's constraint filter.
        if account_id.startswith("anon_") or binding_conflict:
            return LeaseInfo(
                account_id=account_id,
                node_name=selected_node.name,
                proxy_url=selected_node.proxy_url,
                exit_ip=selected_node.exit_ip,
                region=selected_node.region,
                egress_type=selected_node.egress_type,
                assigned_at=now_str,
                tier=selected_node.tier
            )

        # 5. Persist lease and synchronize with account_allocations
        lease = self.storage.set_lease(account_id, selected_node.name)
        self.storage.batch_save_allocations([(svc, account_id, selected_node.name, now_str)])

        return lease

    def _get_candidates_for_tier_policy(
        self,
        region: Optional[str],
        egress_type: Optional[str],
        policy: str
    ) -> List[ProxyNode]:
        def find_pool(p_tier: Optional[str], p_status: str, match_egress: bool, match_region: bool) -> List[ProxyNode]:
            r = region if match_region else None
            e = egress_type if match_egress else None
            return self.storage.list_nodes(region=r, egress_type=e, status=p_status, tier=p_tier)

        if policy == TierPolicy.PREMIUM_ONLY.value:
            tiers_order = [Tier.PREMIUM.value]
            allow_cross_tier = False
        elif policy == TierPolicy.STANDARD_ONLY.value:
            tiers_order = [Tier.STANDARD.value]
            allow_cross_tier = False
        elif policy == TierPolicy.PREMIUM_FIRST.value:
            tiers_order = [Tier.PREMIUM.value, Tier.STANDARD.value]
            allow_cross_tier = True
        else:  # STANDARD_FIRST (default)
            tiers_order = [Tier.STANDARD.value, Tier.PREMIUM.value]
            allow_cross_tier = True

        # 1. Healthy search
        for t in tiers_order:
            if region and egress_type:
                c = find_pool(t, HealthStatus.HEALTHY.value, match_egress=True, match_region=True)
                if c:
                    return c
            if region:
                c = find_pool(t, HealthStatus.HEALTHY.value, match_egress=False, match_region=True)
                if c:
                    return c
            if egress_type:
                c = find_pool(t, HealthStatus.HEALTHY.value, match_egress=True, match_region=False)
                if c:
                    return c
            c = find_pool(t, HealthStatus.HEALTHY.value, match_egress=False, match_region=False)
            if c:
                return c
            if not allow_cross_tier:
                break

        # 2. Degraded emergency search
        for t in tiers_order:
            if region and egress_type:
                c = find_pool(t, HealthStatus.DEGRADED.value, match_egress=True, match_region=True)
                if c:
                    return c
            if region:
                c = find_pool(t, HealthStatus.DEGRADED.value, match_egress=False, match_region=True)
                if c:
                    return c
            c = find_pool(t, HealthStatus.DEGRADED.value, match_egress=False, match_region=False)
            if c:
                return c
            if not allow_cross_tier:
                break

        # 3. Final fallback if cross-tier allowed (strictly exclude DEAD nodes to respect circuit breaker)
        if allow_cross_tier:
            c = [n for n in self.storage.list_nodes(region=region, egress_type=egress_type) if n.status != HealthStatus.DEAD.value]
            if c:
                return c
            return [n for n in self.storage.list_nodes() if n.status != HealthStatus.DEAD.value]

        return []

    def rehome_leases_for_node(self, broken_node_name: str) -> Dict[str, Any]:
        """
        Automatic Self-Healing:
        When a node's circuit breaker trips OPEN (dead), proactively rehome
        all attached account leases to healthy backup nodes in the same region.
        """
        with BINDING_WRITE_LOCK:
            return self._rehome_leases_for_node(broken_node_name)

    def _rehome_leases_for_node(self, broken_node_name: str) -> Dict[str, Any]:
        leases = self.storage.get_leases_for_node(broken_node_name)
        stale_allocations = self.storage.get_allocations_for_node(broken_node_name)
        if not leases and not stale_allocations:
            return {"rehomed_count": 0, "allocations_rehomed_count": 0, "details": []}

        broken_node = self.storage.get_node(broken_node_name)
        target_region = broken_node.region if broken_node else None
        target_egress = broken_node.egress_type if broken_node else None
        target_tier = broken_node.tier if broken_node else None

        # Find healthy candidates in the same region, prioritizing the same tier
        candidates = []
        if target_tier:
            candidates = self.storage.list_nodes(
                region=target_region,
                egress_type=target_egress,
                status=HealthStatus.HEALTHY.value,
                tier=target_tier
            )
            if not candidates and target_region:
                candidates = self.storage.list_nodes(
                    region=target_region,
                    status=HealthStatus.HEALTHY.value,
                    tier=target_tier
                )

        if not candidates:
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

            rehomed_details.append({
                "account_id": acc,
                "from_node": broken_node_name,
                "to_node": new_node.name,
                "new_proxy_url": new_node.proxy_url
            })

        # One event per rehome sweep, not one per account: the audit trail only needs to
        # record that the node was drained, and per-account rows would grow the event
        # table with the number of bound accounts.
        if rehomed_details:
            target_nodes = sorted({d["to_node"] for d in rehomed_details})
            self.storage.record_circuit_event(
                node_name=broken_node_name,
                from_state=HealthStatus.DEAD.value,
                to_state="rehomed",
                reason=f"auto_rehome_{len(rehomed_details)}_accounts_to_{len(target_nodes)}_nodes"
            )

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
        tier: Optional[str] = Tier.PREMIUM.value,
        cooldown_seconds: int = 1800
    ) -> Optional[RegistrationLease]:
        """
        Dedicated anti-sybil egress assignment for batch account registration bots.
        Ensures that allocated exit IPs have not been used for the same target service
        within the cooldown window. Defaults to premium tier to protect registrations.

        The choose-then-lock window is serialized: without it, two concurrent registrations
        both pass the cooldown filter and receive the same exit, which is exactly the reuse
        this endpoint exists to prevent.
        """
        with BINDING_WRITE_LOCK:
            return self._acquire_for_registration(
                account_id, target_service, region, egress_type, tier, cooldown_seconds
            )

    def _acquire_for_registration(
        self,
        account_id: str,
        target_service: str,
        region: Optional[str],
        egress_type: Optional[str],
        tier: Optional[str] = Tier.PREMIUM.value,
        cooldown_seconds: int = 1800
    ) -> Optional[RegistrationLease]:
        self.storage.clean_expired_cooldowns()

        # 1. Fetch eligible candidates (tier-aware)
        candidates = []
        if tier:
            candidates = self.storage.list_nodes(region=region, egress_type=egress_type, status=HealthStatus.HEALTHY.value, tier=tier)
            if not candidates and region:
                candidates = self.storage.list_nodes(region=region, status=HealthStatus.HEALTHY.value, tier=tier)
            if not candidates and egress_type:
                candidates = self.storage.list_nodes(egress_type=egress_type, status=HealthStatus.HEALTHY.value, tier=tier)
            if not candidates:
                candidates = self.storage.list_nodes(status=HealthStatus.HEALTHY.value, tier=tier)

        if not candidates:
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

        # 2. Only nodes with a known exit IP can be protected by an IP cooldown. Nodes
        # that have not been probed yet would bypass the filter entirely and lock their
        # shared mesh host, so they are excluded until a probe reveals their exit.
        candidates = [c for c in candidates if c.exit_ip]
        if not candidates:
            return None

        cooled_exit_ips = {
            c.exit_ip for c in self.storage.get_active_cooldowns(target_service=target_service)
            if c.exit_ip
        }

        clean_candidates = [c for c in candidates if c.exit_ip not in cooled_exit_ips]

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
        cooldown_record = self.storage.add_ip_cooldown(
            exit_ip=selected_node.exit_ip,
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
            relay_hop=relay_hop_url,
            tier=selected_node.tier
        )

    def handle_battery_event(self, report: BatteryEventReport) -> Dict[str, Any]:
        """
        Handles battery degradation / 429 / 401 events reported by downstream battery consumers
        (e.g., grok2api, cpa-manager-plus). Performs automatic battery swapping (lease rebind).
        """
        with BINDING_WRITE_LOCK:
            return self._handle_battery_event(report)

    def _handle_battery_event(self, report: BatteryEventReport) -> Dict[str, Any]:
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
            target_tier = None
            previous_node_name = None

            # 1. Check account_allocations first
            alloc = self.storage.find_allocation_by_account(report.account_id, service=svc)
            if alloc:
                old_node = self.storage.get_node(alloc["node_name"])
                if old_node:
                    target_region = old_node.region
                    target_egress = old_node.egress_type
                    target_tier = old_node.tier
                    previous_node_name = old_node.name
            else:
                existing_lease = self.storage.get_lease(report.account_id)
                if existing_lease:
                    old_node = self.storage.get_node(existing_lease.node_name)
                    if old_node:
                        target_region = old_node.region
                        target_egress = old_node.egress_type
                        target_tier = old_node.tier
                        previous_node_name = old_node.name

            new_lease = self.acquire_for_account(
                account_id=report.account_id,
                region=target_region,
                egress_type=target_egress,
                tier=target_tier,
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

