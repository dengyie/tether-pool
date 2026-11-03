"""
core/allocator.py - Universal Account Allocation & Distribution Engine
Provides balanced capacity-aware sharding, consistent hashing, and multi-protocol proxy allocation
for downstream account pools (e.g. grok2api 2,100+ accounts, cpa-manager-plus, gemini2api).
"""

import bisect
import hashlib
import heapq
import zlib
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple
from urllib.parse import urlparse, quote

from core.models import (
    ProxyNode, HealthStatus, EgressType,
    BatchAllocationRequest, BatchAllocationResponse,
    AccountAllocationItem, AllocationStrategy
)
from core.storage import Storage


class ConsistentHashRing:
    """
    Consistent Hash Ring with virtual nodes (replicas) for minimal disruption re-sharding.
    """
    def __init__(self, nodes: List[ProxyNode], replicas: int = 100):
        self.replicas = replicas
        self.ring: List[int] = []
        self.node_map: Dict[int, ProxyNode] = {}
        for node in nodes:
            self._add_node_hashes(node)
        self.ring.sort()

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)

    def _add_node_hashes(self, node: ProxyNode):
        for i in range(self.replicas):
            vkey = f"{node.name}#vn{i}"
            h = self._hash(vkey)
            self.ring.append(h)
            self.node_map[h] = node

    def add_node(self, node: ProxyNode):
        self._add_node_hashes(node)
        self.ring.sort()

    def get_node(self, key: str) -> Optional[ProxyNode]:
        if not self.ring:
            return None
        h = self._hash(key)
        idx = bisect.bisect_right(self.ring, h)
        if idx == len(self.ring):
            idx = 0
        return self.node_map[self.ring[idx]]


class AllocationEngine:
    def __init__(self, storage: Storage):
        self.storage = storage

    def _get_eligible_nodes(
        self,
        egress_preference: Optional[str] = None,
        region: Optional[str] = None
    ) -> List[ProxyNode]:
        """
        Retrieves healthy eligible proxy nodes.
        Prioritizes specified egress type and region, with graceful fallback ladders.
        """
        # 1. Strict healthy match
        nodes = self.storage.list_nodes(
            region=region,
            egress_type=egress_preference,
            status=HealthStatus.HEALTHY.value
        )
        if not nodes and region:
            nodes = self.storage.list_nodes(region=region, status=HealthStatus.HEALTHY.value)
        if not nodes and egress_preference:
            nodes = self.storage.list_nodes(egress_type=egress_preference, status=HealthStatus.HEALTHY.value)
        if not nodes:
            nodes = self.storage.list_nodes(status=HealthStatus.HEALTHY.value)
        if not nodes:
            # Emergency fallback: degraded nodes
            nodes = self.storage.list_nodes(status=HealthStatus.DEGRADED.value)
        if not nodes:
            nodes = self.storage.list_nodes()

        # Interleave nodes across regions for maximum geographical diversity
        region_buckets: Dict[str, List[ProxyNode]] = {}
        for n in nodes:
            region_buckets.setdefault(n.region, []).append(n)

        for bucket in region_buckets.values():
            bucket.sort(key=lambda x: x.name)

        interleaved: List[ProxyNode] = []
        max_len = max((len(b) for b in region_buckets.values()), default=0)
        sorted_regions = sorted(region_buckets.keys())

        for i in range(max_len):
            for r in sorted_regions:
                b = region_buckets[r]
                if i < len(b):
                    interleaved.append(b[i])

        return interleaved

    def _build_gateway_proxy_url(
        self,
        account_id: str,
        gateway_base: str,
        egress_type: Optional[str] = None,
        region: Optional[str] = None,
        service: Optional[str] = None
    ) -> str:
        parsed = urlparse(gateway_base)
        netloc = parsed.netloc or f"{parsed.hostname or '127.0.0.1'}:{parsed.port or 9101}"
        scheme = parsed.scheme or "http"

        # Build parametric proxy auth user
        # Format: account_id__type_<egress>__region_<region>__service_<service>
        parts = [quote(account_id, safe="")]
        if egress_type:
            parts.append(f"type_{egress_type}")
        if region:
            parts.append(f"region_{region}")
        if service and service != "default":
            parts.append(f"service_{service}")
        auth_user = "__".join(parts)

        return f"{scheme}://{auth_user}:tetherpool@{netloc}"

    def batch_allocate(
        self,
        request: BatchAllocationRequest,
        gateway_base: str = "http://127.0.0.1:9101"
    ) -> BatchAllocationResponse:
        """
        Batch-allocates a list of accounts (e.g. 2,100 grok2api accounts) across available nodes.
        Guarantees minimal variance (balanced spread) across all available IPv6/IPv4 exits.
        Enforces max_accounts_per_ip capacity constraints.
        """
        if not request.accounts:
            return BatchAllocationResponse(
                service=request.service,
                total_accounts=0,
                allocated_nodes_count=0,
                min_accounts_per_node=0,
                max_accounts_per_node=0,
                avg_accounts_per_node=0.0,
                allocations={}
            )

        nodes = self._get_eligible_nodes(
            egress_preference=request.egress_preference,
            region=request.region
        )
        if not nodes:
            raise RuntimeError("No egress nodes available in pool for allocation")

        now_str = datetime.now().isoformat()
        node_by_name = {n.name: n for n in nodes}
        allocations: Dict[str, AccountAllocationItem] = {}
        records_to_save: List[Tuple[str, str, str, str]] = []

        # 1. Reuse existing healthy allocations if force_rebalance is False
        existing_allocations: Dict[str, Dict[str, Any]] = {}
        if not request.force_rebalance:
            all_existing = self.storage.list_allocations_by_service(request.service)
            existing_allocations = {r["account_id"]: r for r in all_existing}

        accounts_to_assign: List[str] = []
        node_counts: Dict[str, int] = {n.name: 0 for n in nodes}

        for acc in request.accounts:
            existing = existing_allocations.get(acc)
            if existing and existing["node_name"] in node_by_name:
                n = node_by_name[existing["node_name"]]
                if n.status == HealthStatus.HEALTHY.value:
                    gw_url = self._build_gateway_proxy_url(acc, gateway_base, n.egress_type, n.region, request.service)
                    item = AccountAllocationItem(
                        account_id=acc,
                        node_name=n.name,
                        proxy_url=n.proxy_url,
                        gateway_proxy_url=gw_url,
                        exit_ip=n.exit_ip,
                        region=n.region,
                        egress_type=n.egress_type,
                        service=request.service,
                        allocated_at=existing["allocated_at"]
                    )
                    allocations[acc] = item
                    node_counts[n.name] += 1
                    continue
            accounts_to_assign.append(acc)

        # 2. Enforce pool capacity against max_accounts_per_ip safety limit
        max_cap = request.max_accounts_per_ip
        if max_cap > 0 and accounts_to_assign:
            total_capacity = len(nodes) * max_cap
            current_occupied = sum(node_counts.values())
            remaining_slots = total_capacity - current_occupied
            if len(accounts_to_assign) > remaining_slots:
                raise ValueError(
                    f"Pool capacity exceeded: requested {len(accounts_to_assign)} new assignments, "
                    f"but only {remaining_slots} slots available across {len(nodes)} nodes "
                    f"(max_accounts_per_ip={max_cap}, currently occupied={current_occupied})"
                )

        # 3. Allocate remaining accounts
        if accounts_to_assign:
            if request.strategy == AllocationStrategy.CONSISTENT_HASH.value:
                ring = ConsistentHashRing(nodes)
                for acc in accounts_to_assign:
                    chosen_node = ring.get_node(f"{request.service}:{acc}") or nodes[0]
                    gw_url = self._build_gateway_proxy_url(acc, gateway_base, chosen_node.egress_type, chosen_node.region, request.service)
                    item = AccountAllocationItem(
                        account_id=acc,
                        node_name=chosen_node.name,
                        proxy_url=chosen_node.proxy_url,
                        gateway_proxy_url=gw_url,
                        exit_ip=chosen_node.exit_ip,
                        region=chosen_node.region,
                        egress_type=chosen_node.egress_type,
                        service=request.service,
                        allocated_at=now_str
                    )
                    allocations[acc] = item
                    node_counts[chosen_node.name] += 1
                    records_to_save.append((request.service, acc, chosen_node.name, now_str))
            else:
                # Default: Balanced Round-Robin (Interleaved)
                # Use min-heap to fill lowest loaded nodes first, guaranteeing minimal variance
                heap = []
                for idx, node in enumerate(nodes):
                    heapq.heappush(heap, (node_counts[node.name], idx, node))

                for acc in accounts_to_assign:
                    cur_count, idx, chosen_node = heapq.heappop(heap)
                    gw_url = self._build_gateway_proxy_url(acc, gateway_base, chosen_node.egress_type, chosen_node.region, request.service)
                    item = AccountAllocationItem(
                        account_id=acc,
                        node_name=chosen_node.name,
                        proxy_url=chosen_node.proxy_url,
                        gateway_proxy_url=gw_url,
                        exit_ip=chosen_node.exit_ip,
                        region=chosen_node.region,
                        egress_type=chosen_node.egress_type,
                        service=request.service,
                        allocated_at=now_str
                    )
                    allocations[acc] = item
                    node_counts[chosen_node.name] += 1
                    records_to_save.append((request.service, acc, chosen_node.name, now_str))
                    heapq.heappush(heap, (cur_count + 1, idx, chosen_node))

        # 4. Persist to SQLite
        if records_to_save:
            self.storage.batch_save_allocations(records_to_save)

        # 4. Compute statistics
        used_counts = [count for count in node_counts.values() if count > 0]
        min_c = min(used_counts) if used_counts else 0
        max_c = max(used_counts) if used_counts else 0
        avg_c = round(len(allocations) / len(used_counts), 2) if used_counts else 0.0

        return BatchAllocationResponse(
            service=request.service,
            total_accounts=len(allocations),
            allocated_nodes_count=len(used_counts),
            min_accounts_per_node=min_c,
            max_accounts_per_node=max_c,
            avg_accounts_per_node=avg_c,
            allocations=allocations
        )

    def acquire_single(
        self,
        service: str,
        account_id: str,
        egress_preference: Optional[str] = None,
        region: Optional[str] = None,
        force_rebalance: bool = False,
        gateway_base: str = "http://127.0.0.1:9101"
    ) -> AccountAllocationItem:
        """
        On-demand acquisition for a single account.
        Checks existing allocation; if missing, picks least loaded eligible node.
        """
        if not force_rebalance:
            existing = self.storage.get_allocation(service, account_id)
            if existing:
                node = self.storage.get_node(existing["node_name"])
                if node and node.status == HealthStatus.HEALTHY.value:
                    gw_url = self._build_gateway_proxy_url(account_id, gateway_base, node.egress_type, node.region, service=service)
                    return AccountAllocationItem(
                        account_id=account_id,
                        node_name=node.name,
                        proxy_url=node.proxy_url,
                        gateway_proxy_url=gw_url,
                        exit_ip=node.exit_ip,
                        region=node.region,
                        egress_type=node.egress_type,
                        service=service,
                        allocated_at=existing["allocated_at"]
                    )

        nodes = self._get_eligible_nodes(egress_preference=egress_preference, region=region)
        if not nodes:
            raise RuntimeError("No eligible nodes available for allocation")

        # Pick node with least existing allocations in this service
        all_allocations = self.storage.list_allocations_by_service(service)
        load_counts: Dict[str, int] = {n.name: 0 for n in nodes}
        for a in all_allocations:
            if a["node_name"] in load_counts:
                load_counts[a["node_name"]] += 1

        least_loaded_node = min(nodes, key=lambda n: (load_counts[n.name], zlib.crc32(f"{service}:{account_id}:{n.name}".encode()) & 0xFFFFFFFF))

        now_str = datetime.now().isoformat()
        self.storage.batch_save_allocations([(service, account_id, least_loaded_node.name, now_str)])

        gw_url = self._build_gateway_proxy_url(account_id, gateway_base, least_loaded_node.egress_type, least_loaded_node.region, service=service)
        return AccountAllocationItem(
            account_id=account_id,
            node_name=least_loaded_node.name,
            proxy_url=least_loaded_node.proxy_url,
            gateway_proxy_url=gw_url,
            exit_ip=least_loaded_node.exit_ip,
            region=least_loaded_node.region,
            egress_type=least_loaded_node.egress_type,
            service=service,
            allocated_at=now_str
        )
