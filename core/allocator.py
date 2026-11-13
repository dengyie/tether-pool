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
    ProxyNode, HealthStatus, Tier, TierPolicy,
    BatchAllocationRequest, BatchAllocationResponse,
    AccountAllocationItem, AllocationStrategy
)
from core.storage import Storage
from core.locks import BINDING_WRITE_LOCK
from core.ban import node_isp, ip_in_excluded_nets, collect_ban_exclusions


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
        region: Optional[str] = None,
        tier: Optional[str] = None,
        tier_policy: Optional[str] = None,
        region_strict: bool = False
    ) -> List[ProxyNode]:
        """
        Retrieves healthy eligible proxy nodes.
        Prioritizes specified egress type, region, and tier policy, with graceful fallback ladders.

        region_strict=True hardens the region filter (same semantics as Router's
        `_get_candidates_for_tier_policy`): the final cross-region fallback rung is
        discarded, so an exhausted region yields an empty candidate set (caller
        answers 503) instead of a silent foreign-region node. 需求 §4.2: region
        是对凭据的硬承诺, 宁可 503 也不跨区违约。
        """
        policy = tier_policy.lower() if tier_policy else (TierPolicy.PREMIUM_FIRST.value if tier == Tier.PREMIUM.value else TierPolicy.STANDARD_FIRST.value)

        def find_nodes(p_tier: Optional[str], p_status: str) -> List[ProxyNode]:
            if region and egress_preference:
                n = self.storage.list_nodes(region=region, egress_type=egress_preference, status=p_status, tier=p_tier)
                if n:
                    return n
            if region:
                return self.storage.list_nodes(region=region, status=p_status, tier=p_tier)
            if egress_preference:
                n = self.storage.list_nodes(egress_type=egress_preference, status=p_status, tier=p_tier)
                if n:
                    return n
            return self.storage.list_nodes(status=p_status, tier=p_tier)

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

        nodes: List[ProxyNode] = []
        # 1. Healthy search
        for t in tiers_order:
            nodes = find_nodes(t, HealthStatus.HEALTHY.value)
            if nodes or not allow_cross_tier:
                break

        # 2. Degraded emergency search
        if not nodes:
            for t in tiers_order:
                nodes = find_nodes(t, HealthStatus.DEGRADED.value)
                if nodes or not allow_cross_tier:
                    break

        # 3. Last fallback (must strictly exclude DEAD nodes)
        if not nodes and allow_cross_tier:
            nodes = [n for n in self.storage.list_nodes(region=region, egress_type=egress_preference) if n.status != HealthStatus.DEAD.value]
            if not nodes:
                nodes = [n for n in self.storage.list_nodes() if n.status != HealthStatus.DEAD.value]

        # region_strict: 丢弃第三级跨区兜底带来的外区节点 (region 是硬承诺,
        # 候选剔空 -> 调用方 503, 与 Router region_strict 同语义)。
        if region_strict and region:
            nodes = [n for n in nodes if n.region == region]

        # Ensure no DEAD node ever passes through
        nodes = [n for n in nodes if n.status != HealthStatus.DEAD.value]

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
        service: Optional[str] = None,
        tier: Optional[str] = None
    ) -> str:
        parsed = urlparse(gateway_base)
        netloc = parsed.netloc or f"{parsed.hostname or '127.0.0.1'}:{parsed.port or 9101}"
        scheme = parsed.scheme or "http"

        # Build parametric proxy auth user
        # Format: account_id__type_<egress>__region_<region>__tier_<tier>__service_<service>
        parts = [quote(account_id, safe="")]
        if egress_type:
            parts.append(f"type_{egress_type}")
        if region:
            parts.append(f"region_{region}")
        if tier:
            parts.append(f"tier_{tier}")
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

        Occupancy is read, then bindings are written; the whole window is serialized so
        concurrent HTTP batches cannot both observe the same pre-write state and overfill
        an exit (`api/server.py` endpoints are sync `def` and run in a threadpool).
        """
        with BINDING_WRITE_LOCK:
            return self._batch_allocate(request, gateway_base)

    def _batch_allocate(
        self,
        request: BatchAllocationRequest,
        gateway_base: str = "http://127.0.0.1:9101"
    ) -> BatchAllocationResponse:
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
            region=request.region,
            tier=request.tier,
            tier_policy=request.tier_policy
        )
        if not nodes:
            raise RuntimeError("No egress nodes available in pool for allocation")

        now_str = datetime.now().isoformat()
        node_by_name = {n.name: n for n in nodes}
        allocations: Dict[str, AccountAllocationItem] = {}
        records_to_save: List[Tuple[str, str, str, str]] = []

        max_cap = request.max_accounts_per_ip

        def _exit_key(n: ProxyNode) -> str:
            # Capacity is bounded per real exit IP, not per listener/port:
            # several nodes may share one upstream egress.
            return n.exit_ip or f"node:{n.name}"

        def _has_room(n: ProxyNode) -> bool:
            return max_cap <= 0 or exit_counts.get(_exit_key(n), 0) < max_cap

        # Seed occupancy from ALL existing bindings of this service, so incremental
        # batches cannot silently overfill exits that are already in use.
        occupied_by_node = self.storage.count_allocations_per_node(request.service)
        occupied_by_exit = self.storage.count_allocations_per_exit(request.service)
        node_counts: Dict[str, int] = {n.name: occupied_by_node.get(n.name, 0) for n in nodes}
        exit_counts: Dict[str, int] = dict(occupied_by_exit)

        # 1. Reuse existing healthy allocations if force_rebalance is False
        existing_allocations: Dict[str, Dict[str, Any]] = {}
        if not request.force_rebalance:
            all_existing = self.storage.list_allocations_by_service(request.service)
            existing_allocations = {r["account_id"]: r for r in all_existing}

        accounts_to_assign: List[str] = []

        def _assign(acc: str, chosen_node: ProxyNode):
            gw_url = self._build_gateway_proxy_url(
                acc, gateway_base, chosen_node.egress_type, chosen_node.region, request.service, chosen_node.tier
            )
            allocations[acc] = AccountAllocationItem(
                account_id=acc,
                node_name=chosen_node.name,
                proxy_url=chosen_node.proxy_url,
                gateway_proxy_url=gw_url,
                exit_ip=chosen_node.exit_ip,
                region=chosen_node.region,
                egress_type=chosen_node.egress_type,
                service=request.service,
                allocated_at=now_str,
                tier=chosen_node.tier
            )
            records_to_save.append((request.service, acc, chosen_node.name, now_str))

        def _capacity_error(pending: int) -> ValueError:
            return ValueError(
                f"Pool capacity exceeded: unable to place {pending} remaining account(s) "
                f"for service '{request.service}' (max_accounts_per_ip={max_cap}, "
                f"nodes={len(nodes)}, occupied={sum(node_counts.values())})"
            )

        for acc in request.accounts:
            existing = existing_allocations.get(acc)
            if existing and existing["node_name"] in node_by_name:
                n = node_by_name[existing["node_name"]]
                if n.status == HealthStatus.HEALTHY.value:
                    gw_url = self._build_gateway_proxy_url(
                        acc, gateway_base, n.egress_type, n.region, request.service, n.tier
                    )
                    item = AccountAllocationItem(
                        account_id=acc,
                        node_name=n.name,
                        proxy_url=n.proxy_url,
                        gateway_proxy_url=gw_url,
                        exit_ip=n.exit_ip,
                        region=n.region,
                        egress_type=n.egress_type,
                        service=request.service,
                        allocated_at=existing["allocated_at"],
                        tier=n.tier
                    )
                    allocations[acc] = item
                    continue
            accounts_to_assign.append(acc)

        # 2. Allocate remaining accounts, honouring per-exit capacity for both strategies
        if accounts_to_assign:
            if request.strategy == AllocationStrategy.CONSISTENT_HASH.value:
                ring = ConsistentHashRing(nodes)
                for acc in accounts_to_assign:
                    chosen_node = ring.get_node(f"{request.service}:{acc}")
                    if chosen_node is None or not _has_room(chosen_node):
                        # Ring pick saturated: overflow to least-loaded exit with room
                        alternatives = sorted(
                            (n for n in nodes if _has_room(n)),
                            key=lambda n: (node_counts[n.name], n.name)
                        )
                        if not alternatives:
                            raise _capacity_error(len(accounts_to_assign))
                        chosen_node = alternatives[0]
                    _assign(acc, chosen_node)
                    node_counts[chosen_node.name] = node_counts.get(chosen_node.name, 0) + 1
                    key = _exit_key(chosen_node)
                    exit_counts[key] = exit_counts.get(key, 0) + 1
            else:
                # Default: Balanced Round-Robin (Interleaved)
                # Min-heap over real occupancy; skip exits that are already saturated.
                heap = [(node_counts[n.name], idx, n) for idx, n in enumerate(nodes)]
                heapq.heapify(heap)

                for acc in accounts_to_assign:
                    skipped = []
                    chosen = None
                    while heap:
                        item = heapq.heappop(heap)
                        if _has_room(item[2]):
                            chosen = item
                            break
                        skipped.append(item)
                    for item in skipped:
                        heapq.heappush(heap, item)
                    if chosen is None:
                        raise _capacity_error(len(accounts_to_assign))

                    cur_count, idx, chosen_node = chosen
                    _assign(acc, chosen_node)
                    node_counts[chosen_node.name] = cur_count + 1
                    key = _exit_key(chosen_node)
                    exit_counts[key] = exit_counts.get(key, 0) + 1
                    heapq.heappush(heap, (cur_count + 1, idx, chosen_node))

        # 3. Persist to SQLite
        if records_to_save:
            self.storage.batch_save_allocations(records_to_save)

        # 4. Compute statistics over the accounts covered by this call
        used_counts: Dict[str, int] = {}
        for item in allocations.values():
            used_counts[item.node_name] = used_counts.get(item.node_name, 0) + 1
        min_c = min(used_counts.values()) if used_counts else 0
        max_c = max(used_counts.values()) if used_counts else 0
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
        tier: Optional[str] = None,
        tier_policy: Optional[str] = None,
        force_rebalance: bool = False,
        gateway_base: str = "http://127.0.0.1:9101",
        # 封禁感知分配 (ban-aware allocation 2026-11-14): 与 Router 同语义,
        # opt-in 硬过滤, 候选耗尽 -> RuntimeError (调用方 503)。
        avoid_banned: bool = False,
        avoid_cidrs: Optional[List[str]] = None,
        avoid_same_isp: bool = False,
        region_strict: bool = False
    ) -> AccountAllocationItem:
        """
        On-demand acquisition for a single account.
        Checks existing allocation; if missing, picks least loaded eligible node.
        """
        with BINDING_WRITE_LOCK:
            return self._acquire_single(
                service, account_id, egress_preference, region, tier, tier_policy, force_rebalance, gateway_base,
                avoid_banned=avoid_banned, avoid_cidrs=avoid_cidrs, avoid_same_isp=avoid_same_isp,
                region_strict=region_strict
            )

    def _acquire_single(
        self,
        service: str,
        account_id: str,
        egress_preference: Optional[str] = None,
        region: Optional[str] = None,
        tier: Optional[str] = None,
        tier_policy: Optional[str] = None,
        force_rebalance: bool = False,
        gateway_base: str = "http://127.0.0.1:9101",
        avoid_banned: bool = False,
        avoid_cidrs: Optional[List[str]] = None,
        avoid_same_isp: bool = False,
        region_strict: bool = False
    ) -> AccountAllocationItem:
        # quarantine 集合组装: 节点名让 fast path 视为 mismatch, /24 与 isp 集在
        # 候选过滤生效。与 Router 共用 core/ban.collect_ban_exclusions (单一事实源)。
        ban_node_names, ban_excluded_nets, ban_excluded_isps = collect_ban_exclusions(
            self.storage, service,
            avoid_banned=avoid_banned,
            avoid_cidrs=avoid_cidrs,
            avoid_same_isp=avoid_same_isp,
        )

        def _ban_node_ok(n: ProxyNode) -> bool:
            if n.name in ban_node_names:
                return False
            if ip_in_excluded_nets(n.exit_ip, ban_excluded_nets):
                return False
            if avoid_same_isp and ban_excluded_isps:
                isp = node_isp(n)
                if isp and isp in ban_excluded_isps:
                    return False
            return True

        # fast path 是否仅因封禁排除被跳过: 是 -> 本次改道是 per-call 过滤结果,
        # 与 Router binding_conflict 同语义, 不得落库迁移绑定 (opt-in 语义)。
        ban_skipped_existing = False

        if not force_rebalance:
            existing = self.storage.get_allocation(service, account_id)
            if existing:
                node = self.storage.get_node(existing["node_name"])
                if node and node.status == HealthStatus.HEALTHY.value and not _ban_node_ok(node):
                    # 绑定节点健康但被本次 ban 过滤拒绝: 记旗, 候选选择照常,
                    # 结果临时返回、绑定原地保留 (与 Router._acquire_for_account 对齐)。
                    ban_skipped_existing = True
                elif node and node.status == HealthStatus.HEALTHY.value and _ban_node_ok(node):
                    matches = True
                    if egress_preference and node.egress_type != egress_preference:
                        matches = False
                    if region and node.region != region:
                        matches = False
                    if tier_policy == TierPolicy.PREMIUM_ONLY.value and node.tier != Tier.PREMIUM.value:
                        matches = False
                    elif tier_policy == TierPolicy.STANDARD_ONLY.value and node.tier != Tier.STANDARD.value:
                        matches = False
                    elif tier and node.tier != tier and not tier_policy:
                        matches = False

                    if matches:
                        gw_url = self._build_gateway_proxy_url(account_id, gateway_base, node.egress_type, node.region, service=service, tier=node.tier)
                        return AccountAllocationItem(
                            account_id=account_id,
                            node_name=node.name,
                            proxy_url=node.proxy_url,
                            gateway_proxy_url=gw_url,
                            exit_ip=node.exit_ip,
                            region=node.region,
                            egress_type=node.egress_type,
                            service=service,
                            allocated_at=existing["allocated_at"],
                            tier=node.tier
                        )

        nodes = self._get_eligible_nodes(
            egress_preference=egress_preference,
            region=region,
            tier=tier,
            tier_policy=tier_policy,
            region_strict=region_strict
        )
        # 封禁感知硬过滤 (需求 §5.2/§5.3): 被封节点名、同 /24、同 ISP 剔除。
        if ban_node_names or ban_excluded_nets or (avoid_same_isp and ban_excluded_isps):
            nodes = [n for n in nodes if _ban_node_ok(n)]
        if not nodes:
            raise RuntimeError("No eligible nodes available for allocation")

        # Pick node with least existing allocations in this service (single GROUP BY query)
        load_counts: Dict[str, int] = self.storage.count_allocations_per_node(service)

        least_loaded_node = min(
            nodes,
            key=lambda n: (
                load_counts.get(n.name, 0),
                zlib.crc32(f"{service}:{account_id}:{n.name}".encode()) & 0xFFFFFFFF,
            ),
        )

        now_str = datetime.now().isoformat()
        if not ban_skipped_existing:
            self.storage.batch_save_allocations([(service, account_id, least_loaded_node.name, now_str)])

        gw_url = self._build_gateway_proxy_url(account_id, gateway_base, least_loaded_node.egress_type, least_loaded_node.region, service=service, tier=least_loaded_node.tier)
        return AccountAllocationItem(
            account_id=account_id,
            node_name=least_loaded_node.name,
            proxy_url=least_loaded_node.proxy_url,
            gateway_proxy_url=gw_url,
            exit_ip=least_loaded_node.exit_ip,
            region=least_loaded_node.region,
            egress_type=least_loaded_node.egress_type,
            service=service,
            allocated_at=now_str,
            tier=least_loaded_node.tier,
            isp=node_isp(least_loaded_node) or None
        )
