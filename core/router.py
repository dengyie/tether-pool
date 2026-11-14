"""
core/router.py - Routing and proxy assignment engine with Circuit-Aware Lease Re-homing,
Anti-Sybil Cooldown for Registration Bots, and Relay Bridge Resolution
"""

import os
import zlib
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
from core.models import (
    ProxyNode, LeaseInfo, EgressType, HealthStatus, Tier, TierPolicy,
    RegistrationLease, BatteryEventReport, BatteryEventType, RelayMode
)
from core.ban import (
    node_isp,
    build_ban_excluded_networks,
    ip_in_excluded_nets,
    collect_ban_exclusions,
)
from core.storage import Storage
from core.locks import BINDING_WRITE_LOCK


class Router:
    def __init__(self, storage: Storage):
        self.storage = storage
        # hub 跳板扫描缓存 (数据流优化 2026-11-14): 276 个 relay_fallback 节点
        # 都没有显式 relay_hop_node, 旧实现每个请求最多扫 4 个 hub 区。跳板候选
        # 只随节点状态/配置变化, 10s TTL 对"仅直连失败才用到"的兜底路径完全够
        # 新鲜 —— 每请求 4 次 list_nodes 归零。
        self._hub_cache: Optional[List[ProxyNode]] = None
        self._hub_cache_ts: float = 0.0
        self._hub_cache_ttl: float = float(os.environ.get("IP_POOL_HUB_CACHE_TTL", "10"))

    def _hub_candidates(self) -> List[ProxyNode]:
        """Healthy clash_proxy hub nodes (hongkong/tokyo/singapore/us-west), 10s TTL cached."""
        import time
        now = time.time()
        if self._hub_cache is None or now - self._hub_cache_ts > self._hub_cache_ttl:
            candidates: List[ProxyNode] = []
            for hub_region in ["hongkong", "tokyo", "singapore", "us-west"]:
                candidates.extend(self.storage.list_nodes(
                    region=hub_region,
                    egress_type=EgressType.CLASH_PROXY.value,
                    status=HealthStatus.HEALTHY.value
                ))
            self._hub_cache = candidates
            self._hub_cache_ts = now
        return self._hub_cache

    def acquire_for_account(
        self,
        account_id: str,
        egress_type: Optional[str] = EgressType.WARP_IPV6.value,
        region: Optional[str] = None,
        tier: Optional[str] = None,
        tier_policy: Optional[str] = None,
        force_rebind: bool = False,
        service: Optional[str] = None,
        exclude_node: Optional[str] = None,
        exclude_nodes: Optional[set] = None,
        region_strict: bool = False,
        bind_persist: bool = False,
        # 软排除语义 (2026-11-14 机场目标级故障兜底): 绑定节点被 exclude_nodes
        # 排除时, 返回临时 lease 改道候选节点但不落库 —— 绑定原地保留, 其他
        # 目标类继续走原节点, 上游修复后下一请求自动回到绑定节点。仅对
        # force_rebind=False 的 sticky 排除场景生效。
        exclude_transient: bool = False,
        # 封禁感知分配 (ban-aware allocation 2026-11-14): avoid_banned 并入该
        # service 的 quarantine 集 (节点名 + exit_ip /24); avoid_cidrs 显式网段;
        # avoid_same_isp 跨 ISP 选点 (仅对带 isp=<name> tag 的节点生效)。
        # 硬过滤: 候选剔空 -> None (503), 绝不放松约束硬塞 (需求 §4.2)。
        avoid_banned: bool = False,
        avoid_cidrs: Optional[List[str]] = None,
        avoid_same_isp: bool = False
    ) -> Optional[LeaseInfo]:
        with BINDING_WRITE_LOCK:
            return self._acquire_for_account(
                account_id=account_id,
                egress_type=egress_type,
                region=region,
                tier=tier,
                tier_policy=tier_policy,
                force_rebind=force_rebind,
                service=service,
                exclude_node=exclude_node,
                exclude_nodes=exclude_nodes,
                region_strict=region_strict,
                bind_persist=bind_persist,
                exclude_transient=exclude_transient,
                avoid_banned=avoid_banned,
                avoid_cidrs=avoid_cidrs,
                avoid_same_isp=avoid_same_isp
            )

    def _acquire_for_account(
        self,
        account_id: str,
        egress_type: Optional[str] = EgressType.WARP_IPV6.value,
        region: Optional[str] = None,
        tier: Optional[str] = None,
        tier_policy: Optional[str] = None,
        force_rebind: bool = False,
        service: Optional[str] = None,
        exclude_node: Optional[str] = None,
        exclude_nodes: Optional[set] = None,
        region_strict: bool = False,
        bind_persist: bool = False,
        exclude_transient: bool = False,
        avoid_banned: bool = False,
        avoid_cidrs: Optional[List[str]] = None,
        avoid_same_isp: bool = False
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

        `region_strict=True` hardens the region filter: the tier ladder stops relaxing
        `region` (region+egress → region → give up), so a nonexistent region yields None
        (the gateway answers 503 pool_exhausted) instead of silently falling back abroad.

        `bind_persist=True` opts the caller out of the ephemeral-binding-conflict rule:
        the selected node is persisted even though the stored binding only failed this
        call's filter (reason `constraint_rebind`), letting a credential declare itself
        the canonical binding for the account.

        Whenever a bind is persisted, the call's constraint set is stored alongside it
        (bound_* columns) so auto_rehome can keep the binding inside its promise.
        """
        svc = service or "default"
        binding_conflict = False
        # R1: the gateway retries with a *cumulative* exclusion set, so a request can never
        # cycle back onto a node that already failed for it. `exclude_node` is kept as the
        # single-node spelling used by the battery/rebind paths.
        excluded: set = set(exclude_nodes or ())
        if exclude_node:
            excluded.add(exclude_node)

        # 封禁感知分配: quarantine 集合组装 (仅在调用方显式 opt-in 时生效 ——
        # 用户拍板 2026-11-14: 不做全局自动排除)。节点名放独立集合 (不并入
        # excluded —— 网关 R1 累积重试依赖 excluded 触发持久重绑, ban 过滤是
        # per-call 临时性, 两者语义必须分开); /24 与 isp 集在候选过滤阶段生效。
        # 组装逻辑与 Allocator 共用 core/ban.collect_ban_exclusions (单一事实源)。
        ban_excluded_names, ban_excluded_nets, ban_excluded_isps = collect_ban_exclusions(
            self.storage, svc,
            avoid_banned=avoid_banned,
            avoid_cidrs=avoid_cidrs,
            avoid_same_isp=avoid_same_isp,
        )

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

        def _call_constraints() -> Dict[str, Any]:
            """The constraint set this call carries, in the persisted bound_* spelling."""
            return {
                "region": region or "",
                "region_strict": bool(region_strict),
                "egress_type": egress_type or "",
                "tier": tier or "",
                "tier_policy": tier_policy or "",
            }

        def _node_matches_constraints(node: ProxyNode) -> bool:
            if egress_type and node.egress_type != egress_type:
                return False
            if region and node.region != region:
                return False
            if policy == TierPolicy.PREMIUM_ONLY.value and node.tier != Tier.PREMIUM.value:
                return False
            if policy == TierPolicy.STANDARD_ONLY.value and node.tier != Tier.STANDARD.value:
                return False
            return True

        def _ban_rejects(node: ProxyNode) -> bool:
            """完整 ban 谓词 (名字 ∨ /24 ∨ isp), 与候选过滤 _ban_filtered 同一语义。
            fast path 守卫必须用这个 —— 只查名字会让同 /24 / 同 ISP 的健康粘性绑定
            绕过过滤直接返回 (review P1: 与 Allocator._ban_node_ok 语义对齐)。"""
            if node.name in ban_excluded_names:
                return True
            if ip_in_excluded_nets(node.exit_ip, ban_excluded_nets):
                return True
            if avoid_same_isp and ban_excluded_isps:
                isp = node_isp(node)
                if isp and isp in ban_excluded_isps:
                    return True
            return False

        # 1. Check existing lease if not forcing rebind
        alloc = self.storage.find_allocation_by_account(account_id, service=svc)
        steer_transient = False
        steer_sticky_node: Optional[str] = None
        if not force_rebind:
            # 1a. Universal Allocation Engine bindings check first
            if alloc:
                node = self.storage.get_node(alloc["node_name"])
                # 封禁排除先于一切: 绑定节点被完整 ban 谓词 (名字 ∨ /24 ∨ isp) 拒绝时
                # 绝不走 fast path 返回 (先于 healthy/excluded 分支判断)。
                if (
                    node
                    and node.status == HealthStatus.HEALTHY.value
                    and _ban_rejects(node)
                ):
                    binding_conflict = True
                elif (
                    node
                    and node.status == HealthStatus.HEALTHY.value
                    and node.name not in excluded
                ):
                    if _node_matches_constraints(node):
                        # Constraint backfill: a constrained call whose stored constraint
                        # set differs (e.g. bindings created before bound_* columns or a
                        # credential region change) persists the new set so future rehomes
                        # honour it. Unconstrained calls never strip stored constraints —
                        # a plain request must not weaken a pinned binding.
                        call_c = _call_constraints()
                        if any(call_c.values()) and (
                            alloc.get("bound_region") or "",
                            bool(alloc.get("bound_region_strict")),
                            alloc.get("bound_egress_type") or "",
                            alloc.get("bound_tier") or "",
                            alloc.get("bound_tier_policy") or "",
                        ) != (call_c["region"], call_c["region_strict"], call_c["egress_type"], call_c["tier"], call_c["tier_policy"]):
                            try:
                                self.storage.update_allocation_constraints(svc, account_id, call_c)
                            except Exception as c_err:
                                print(f"[-] Constraint backfill failed for {account_id}: {c_err}")
                        return LeaseInfo(
                            account_id=account_id,
                            node_name=node.name,
                            proxy_url=node.proxy_url,
                            exit_ip=node.exit_ip,
                            region=node.region,
                            egress_type=node.egress_type,
                            assigned_at=alloc["allocated_at"],
                            tier=node.tier,
                            host=node.host,
                            port=node.port,
                            relay_mode=node.relay_mode,
                            relay_hop_node=node.relay_hop_node
                        )
                    # Healthy binding that only fails this call's filter -> keep it.
                    binding_conflict = True
                elif (
                    node
                    and node.status == HealthStatus.HEALTHY.value
                    and node.name in excluded
                    and exclude_transient
                ):
                    # 软排除 (目标级兜底): 绑定节点本身健康, 只是被本请求的
                    # 目标类健康集排除 —— 记旗, 候选选择照常进行, 但最后不落库
                    # (见 step 5 前的拦截)。绑定原地保留。
                    steer_transient = True
                    steer_sticky_node = node.name

            # 1b. Legacy lease table check — only for accounts that have NO universal
            # allocation at all. `batch_save_allocations` now maintains this mirror in the
            # same transaction as the authoritative allocation, so a lease row can belong
            # to a *different* service. Adopting it here would leak that service's binding
            # into the requested one and skip persisting the new allocation.
            existing = (
                None if self.storage.find_allocation_by_account(account_id)
                else self.storage.get_lease(account_id)
            )
            if existing:
                node = self.storage.get_node(existing.node_name)
                # 封禁排除先于一切 (同 1a): 完整 ban 谓词拒绝的绑定绝不走 fast path。
                if node and node.status == HealthStatus.HEALTHY.value and _ban_rejects(node):
                    binding_conflict = True
                # Ensure existing node is HEALTHY (Closed)
                elif node and node.status == HealthStatus.HEALTHY.value and node.name not in excluded:
                    if _node_matches_constraints(node):
                        return existing
                    binding_conflict = True
                elif (
                    node
                    and node.status == HealthStatus.HEALTHY.value
                    and node.name in excluded
                    and exclude_transient
                ):
                    # 软排除对 legacy 镜像账户同语义: 临时改道, lease 行不动。
                    steer_transient = True
                    steer_sticky_node = node.name

        # A forced rebind must actually change nodes; the deterministic CRC32 hash below
        # would otherwise re-select the current sticky node (the documented
        # `__rebind_true` no-op fixed in production on 2026-11-05). The binding is only
        # consulted here, so the steady-state path keeps its single query.
        if force_rebind:
            current_node_name = alloc["node_name"] if alloc and alloc.get("node_name") else None
            if current_node_name is None:
                current_lease = self.storage.get_lease(account_id)
                if current_lease and not self.storage.find_allocation_by_account(account_id):
                    # Pre-migration accounts bound only through the legacy lease mirror.
                    current_node_name = current_lease.node_name
            if current_node_name:
                excluded.add(current_node_name)

        # 2. Get eligible candidates (tier-aware ladder search)
        candidates = self._get_candidates_for_tier_policy(
            region=region,
            egress_type=egress_type,
            policy=policy,
            region_strict=region_strict
        )

        if excluded:
            filtered = [c for c in candidates if c.name not in excluded]
            # Honour the exclusions even when they consume the whole pool: the tier ladder
            # has already widened scope, and landing back on a node that just failed for
            # this request is strictly worse than returning "no candidate" (-> 503).
            candidates = filtered

        # 封禁感知硬过滤 (需求 §5.2/§5.3): 被封节点名、同 /24、同 ISP 剔除。
        # 候选剔空 -> 走下方降级分支 (软排除降级会回绑定节点, 与目标级兜底同形)。
        if ban_excluded_names or ban_excluded_nets or (avoid_same_isp and ban_excluded_isps):
            def _ban_filtered(c: ProxyNode) -> bool:
                if c.name in ban_excluded_names:
                    return False
                if ip_in_excluded_nets(c.exit_ip, ban_excluded_nets):
                    return False
                if avoid_same_isp and ban_excluded_isps:
                    isp = node_isp(c)
                    if isp and isp in ban_excluded_isps:
                        return False
                return True
            candidates = [c for c in candidates if _ban_filtered(c)]

        if not candidates:
            # 软排除降级 (目标级兜底): 绑定节点健康、只是被本请求的目标类健康集
            # 排除, 且候选被排除空 —— 宁可返回绑定节点 (该类流量可能失败, 但绝不
            # None/503; 绑定节点对其他目标类仍然可用, 探活恢复后自然无感)。
            # 降级必须先过本调用的约束过滤: region/egress/tier 钉定是对凭据的
            # 承诺, 宁可 503 也不跨区违约 (与 binding_conflict 路径同语义)。
            if steer_transient and steer_sticky_node:
                bound = self.storage.get_node(steer_sticky_node)
                if (
                    bound
                    and bound.status == HealthStatus.HEALTHY.value
                    and _node_matches_constraints(bound)
                ):
                    return LeaseInfo(
                        account_id=account_id,
                        node_name=bound.name,
                        proxy_url=bound.proxy_url,
                        exit_ip=bound.exit_ip,
                        region=bound.region,
                        egress_type=bound.egress_type,
                        # legacy 镜像路径下 alloc 为 None, 直接取当前时间
                        assigned_at=(alloc or {}).get("allocated_at") or datetime.now().isoformat(),
                        tier=bound.tier,
                        host=bound.host,
                        port=bound.port,
                        relay_mode=bound.relay_mode,
                        relay_hop_node=bound.relay_hop_node
                    )
            return None

        # Sort stably by node name
        candidates.sort(key=lambda x: x.name)

        # 3. Deterministic CRC32 hash, weighted by node weight
        selected_node = self._weighted_deterministic_pick(candidates, account_id)

        now_str = datetime.now().isoformat()

        # 4. Ephemeral cases do NOT pollute database: anonymous accounts, and accounts
        #    whose stored binding only failed this call's constraint filter. The
        #    `bind_persist_true` credential opts out of the latter: it declares this
        #    call's binding canonical, so the selected node IS persisted (with reason
        #    `constraint_rebind`). A persistence failure never fails the request — the
        #    caller still gets its egress.
        ephemeral_lease = LeaseInfo(
            account_id=account_id,
            node_name=selected_node.name,
            proxy_url=selected_node.proxy_url,
            exit_ip=selected_node.exit_ip,
            region=selected_node.region,
            egress_type=selected_node.egress_type,
            assigned_at=now_str,
            tier=selected_node.tier,
            host=selected_node.host,
            port=selected_node.port,
            relay_mode=selected_node.relay_mode,
            relay_hop_node=selected_node.relay_hop_node
        )
        if account_id.startswith("anon_"):
            return ephemeral_lease

        if binding_conflict:
            if not bind_persist:
                return ephemeral_lease
            try:
                self.storage.set_lease(account_id, selected_node.name, rebind_reason="constraint_rebind")
                self.storage.batch_save_allocations(
                    [(svc, account_id, selected_node.name, now_str)],
                    reason="constraint_rebind",
                    constraints_by_account={(svc, account_id): _call_constraints()},
                )
            except Exception as persist_err:
                print(f"[-] constraint_rebind persistence failed for {account_id}: {persist_err}")
            return ephemeral_lease

        # 软排除拦截 (目标级兜底): 改道结果只作为本请求的临时 lease 返回,
        # 不写 set_lease / account_allocations —— 绑定原地保留, 上游恢复后
        # 下一请求自动回到原绑定节点。
        if steer_transient:
            ephemeral_lease.steered = True
            return ephemeral_lease

        # 5. Persist lease and synchronize with account_allocations (constraint set rides
        #    along so auto_rehome stays inside the credential's promise).
        lease = self.storage.set_lease(account_id, selected_node.name)
        self.storage.batch_save_allocations(
            [(svc, account_id, selected_node.name, now_str)],
            constraints_by_account={(svc, account_id): _call_constraints()},
        )
        # set_lease 由 lease 表构造, 不带拨号字段 —— 从 selected_node 补齐,
        # 让新绑定/R2 重绑路径同样免掉网关的重复 get_node。
        lease.host = selected_node.host
        lease.port = selected_node.port
        lease.relay_mode = selected_node.relay_mode
        lease.relay_hop_node = selected_node.relay_hop_node

        return lease

    def _get_candidates_for_tier_policy(
        self,
        region: Optional[str],
        egress_type: Optional[str],
        policy: str,
        region_strict: bool = False
    ) -> List[ProxyNode]:
        def find_pool(p_tier: Optional[str], p_status: str, match_egress: bool, match_region: bool) -> List[ProxyNode]:
            r = region if match_region else None
            e = egress_type if match_egress else None
            nodes = self.storage.list_nodes(region=r, egress_type=e, status=p_status, tier=p_tier)
            if egress_type is None:
                # 计量型 vendor 家宽 (residential 标签) 是显式 opt-in 资源:
                # 未声明 egress_type 的请求 (匿名/无类型) 不得默认落在其上,
                # 否则未配置的消费方会烧掉按 GB 计费的家宽流量 (2026-11-12 实测
                # Mac 池匿名流量 45.2% 落家宽)。显式 type=clash_proxy 不受影响;
                # 阶梯最后的全池兜底 (availability first) 也不受影响。
                nodes = [n for n in nodes if "residential" not in (n.tags or [])]
            return nodes

        # region_strict: the credential's region is a hard promise. The ladder stops
        # relaxing `region` (region+egress → region → give up) so a nonexistent/empty
        # region surfaces as "no candidates" (gateway -> 503 pool_exhausted) instead of
        # a silent cross-region fallback. Without a region constraint strict is moot.
        # NOTE: strict 与 *_only 组合时两者都是硬边界 — `*_only` 限定 tier、
        # strict 限定 region, 交集为空即拒绝 (不会为凑 *_only 而跨区, 也不会为凑
        # region 而破坏 *_only)。语义上等价于 AND, 无隐式优先级。
        allow_region_relax = not (region_strict and region)

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
            if allow_region_relax:
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
            if allow_region_relax:
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
            if allow_region_relax:
                return [n for n in self.storage.list_nodes() if n.status != HealthStatus.DEAD.value]
            c_region = [n for n in self.storage.list_nodes(region=region) if n.status != HealthStatus.DEAD.value]
            if c_region:
                return c_region

        return []

    def rehome_leases_for_node(self, broken_node_name: str) -> Dict[str, Any]:
        """
        Automatic Self-Healing:
        When a node's circuit breaker trips OPEN (dead), proactively rehome
        all attached account leases to healthy backup nodes in the same region.
        """
        with BINDING_WRITE_LOCK:
            return self._rehome_leases_for_node(broken_node_name)

    def _find_rehome_candidates(
        self,
        target_region: Optional[str],
        target_egress: Optional[str],
        target_tier: Optional[str],
        exclude_names: set
    ) -> List[ProxyNode]:
        """
        Narrows the pool for a rebind the same way the allocation ladder does, from the
        most sticky (same region + egress + tier) to the least (any healthy node ANYWHERE
        is better than none). Only HEALTHY nodes qualify; DEAD / cooling nodes never enter
        the sequence (R3 acceptance: after a mesh-nl kill the pool still has aws-tokyo /
        huawei-sg / india visible, and never re-proposes a dead listener).
        """
        # Order matters: `egress_type` is preserved before region (R2 acceptance requires a
        # rehomed warp_ipv6 account to stay on warp_ipv6), and region before tier. A
        # same-region non-WARP node is therefore never chosen over a cross-region WARP one.
        ladder = [
            (target_region, target_egress, target_tier),
            (target_region, target_egress, None),
            (None, target_egress, target_tier),
            (None, target_egress, None),
            (target_region, None, target_tier),
            (target_region, None, None),
            (None, None, target_tier),
            (None, None, None),
        ]
        for region, egress, tier in ladder:
            cands = self.storage.list_nodes(
                region=region, egress_type=egress,
                status=HealthStatus.HEALTHY.value, tier=tier
            )
            cands = [c for c in cands if c.name not in exclude_names]
            if cands:
                return cands
        return []

    def _find_rehome_candidates_for_account(
        self,
        constraints: Dict[str, Any],
        fallback_region: Optional[str],
        fallback_egress: Optional[str],
        fallback_tier: Optional[str],
        exclude_names: set
    ) -> Tuple[Optional[List[ProxyNode]], Optional[str]]:
        """
        Rehome candidate set for ONE account under its persisted constraint set.

        Returns (candidates, None) when candidates exist, or (None, blocked_region) when
        the account is rehome-blocked (its constraint set has no healthy candidate at
        all — the binding stays where it is rather than silently breaking the promise).

        Semantics per constraint dimension (mirrors the acquire ladder):
        * `region` — HARD: candidates never leave the bound region. `region_strict`
          changes acquire-time relaxation only; for rehome a bound region is always
          enforced (that is the caller's "美国出口" promise).
        * `egress_type` / `tier` — preferred inside the region: the (region+egress) or
          (region+tier) subset is tried first, relaxed to the whole region when empty
          (the acquire ladder relaxes egress the same way).
        * `tier_policy` `*_only` — HARD: the region candidate set must also match the
          policy tier (a standard_only account must not be parked on premium nodes).
        * no bound region — unconstrained: identical to the availability-first ladder.
        """
        bound_region = (constraints.get("region") or "").strip() or None
        bound_egress = (constraints.get("egress_type") or "").strip() or None
        bound_tier = (constraints.get("tier") or "").strip() or None
        bound_policy = (constraints.get("tier_policy") or "").strip().lower() or None

        if not bound_region:
            # No region promise on record: keep the existing availability-first ladder.
            cands = self._find_rehome_candidates(
                fallback_region, fallback_egress, fallback_tier, exclude_names
            )
            return (cands or None), None

        def _pool(egress: Optional[str], tier: Optional[str]) -> List[ProxyNode]:
            nodes = self.storage.list_nodes(
                region=bound_region, egress_type=egress,
                status=HealthStatus.HEALTHY.value, tier=tier
            )
            return [n for n in nodes if n.name not in exclude_names]

        if bound_policy == TierPolicy.PREMIUM_ONLY.value:
            hard_tiers = [Tier.PREMIUM.value]
        elif bound_policy == TierPolicy.STANDARD_ONLY.value:
            hard_tiers = [Tier.STANDARD.value]
        else:
            hard_tiers = None

        # Tier preferences (non-*_only policies): try the bound tier first, then relax
        # within the region — mirroring the acquire ladder's tier ordering.
        tier_pref: List[Optional[str]]
        if hard_tiers:
            tier_pref = hard_tiers  # policy is absolute; no relaxation at all
        elif bound_tier:
            tier_pref = [bound_tier, None]
        else:
            tier_pref = [None]

        for t in tier_pref:
            # Preferred egress first, then any egress inside the region.
            if bound_egress:
                cands = _pool(bound_egress, t)
                if cands:
                    return cands, None
            cands = _pool(None, t)
            if cands:
                return cands, None

        return None, bound_region

    @staticmethod
    def _constraints_from_alloc(alloc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Reads the persisted bound_* set out of an allocation row (all-or-nothing)."""
        if not alloc:
            return {}
        return {
            "region": alloc.get("bound_region") or "",
            "region_strict": bool(alloc.get("bound_region_strict")),
            "egress_type": alloc.get("bound_egress_type") or "",
            "tier": alloc.get("bound_tier") or "",
            "tier_policy": alloc.get("bound_tier_policy") or "",
        }

    def _record_rehome_blocked(self, svc: str, bound_region: Optional[str]) -> None:
        try:
            self.storage.incr_metric(
                "ip_pool_rehome_blocked_total",
                {"service": svc, "region": bound_region or "unknown"}
            )
        except Exception as metric_err:
            print(f"[-] Failed to record rehome_blocked metric: {metric_err}")

    def _rehome_leases_for_node(self, broken_node_name: str) -> Dict[str, Any]:
        leases = self.storage.get_leases_for_node(broken_node_name)
        stale_allocations = self.storage.get_allocations_for_node(broken_node_name)
        if not leases and not stale_allocations:
            return {"rehomed_count": 0, "allocations_rehomed_count": 0, "details": []}

        broken_node = self.storage.get_node(broken_node_name)
        target_region = broken_node.region if broken_node else None
        target_egress = broken_node.egress_type if broken_node else None
        target_tier = broken_node.tier if broken_node else None

        # Migration units are (service, account_id) pairs: one account_id can be bound
        # in several service namespaces with DIFFERENT constraint sets, and each
        # binding must be rehomed under its own promise (never aggregated by
        # account_id — that would apply one service's region pin to all of them).
        units: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for a in stale_allocations:
            units[(a["service"], a["account_id"])] = {"constraints": self._constraints_from_alloc(a)}
        allocated_account_ids = {acc for (_, acc) in units}
        # Both the legacy lease table and the universal allocation table can point at
        # the dead node; accounts present only in the lease table predate the
        # universal table and are adopted into `default` with no constraints on
        # record (availability-first, same invariant migrate_legacy_leases keeps).
        for lease in leases:
            if lease.account_id not in allocated_account_ids:
                units.setdefault(("default", lease.account_id), {"constraints": {}})
        lease_accounts = {lease.account_id for lease in leases}

        now_str = datetime.now().isoformat()

        # The unconstrained candidate ladder does not depend on the account — compute
        # it ONCE for every no-region unit (empty or region-less constraint sets both
        # resolve to exactly this ladder inside _find_rehome_candidates_for_account).
        # It stays inside the BINDING_WRITE_LOCK, so the hoist also shortens the
        # critical section proportionally to the account count.
        base_candidates = self._find_rehome_candidates(
            target_region, target_egress, target_tier,
            exclude_names={broken_node_name}
        )

        new_node_by_unit: Dict[Tuple[str, str], ProxyNode] = {}
        blocked_units: Dict[Tuple[str, str], str] = {}
        for key, info in units.items():
            svc_acc, acc = key
            c = info["constraints"]
            if c.get("region"):
                cands, blocked_region = self._find_rehome_candidates_for_account(
                    c, target_region, target_egress, target_tier,
                    exclude_names={broken_node_name}
                )
            else:
                # No region promise on record: shared availability-first ladder.
                cands, blocked_region = (base_candidates or None), None
            if not cands:
                blocked_units[key] = blocked_region or "none_healthy"
                self._record_rehome_blocked(svc_acc, blocked_units[key])
                continue
            new_node_by_unit[key] = self._weighted_deterministic_pick(cands, acc)

        rehomed_details = []
        for key in sorted(units):
            svc_acc, acc = key
            if key in new_node_by_unit:
                new_node = new_node_by_unit[key]
                rehomed_details.append({
                    "account_id": acc,
                    "service": svc_acc,
                    "from_node": broken_node_name,
                    "to_node": new_node.name,
                    "new_proxy_url": new_node.proxy_url
                })
            else:
                rehomed_details.append({
                    "account_id": acc,
                    "service": svc_acc,
                    "from_node": broken_node_name,
                    "to_node": None,
                    "blocked": True,
                    "blocked_region": blocked_units[key]
                })

        # One event per rehome sweep, not one per account: the audit trail only needs to
        # record that the node was drained, and per-account rows would grow the event
        # table with the number of bound accounts.
        if rehomed_details:
            moved = [d for d in rehomed_details if d.get("to_node")]
            target_nodes = sorted({d["to_node"] for d in moved})
            reason_txt = (
                f"auto_rehome_{len(moved)}_accounts_to_{len(target_nodes)}_nodes"
                + (f"_{len(blocked_units)}_rehome_blocked" if blocked_units else "")
            )
            self.storage.record_circuit_event(
                node_name=broken_node_name,
                from_state=HealthStatus.DEAD.value,
                to_state="rehomed",
                reason=reason_txt
            )

        # Rewrite universal allocations still pointing at the dead node (service preserved;
        # bound_* constraint columns preserved — do NOT pass constraints here). Adopted
        # lease-only units ride in the same transaction, which mirrors leases too.
        allocation_records: List[Tuple[str, str, str, str]] = [
            (svc_acc, acc, new_node_by_unit[(svc_acc, acc)].name, now_str)
            for svc_acc, acc in sorted(units)
            if (svc_acc, acc) in new_node_by_unit
        ]
        if allocation_records:
            self.storage.batch_save_allocations(allocation_records, reason="auto_rehome")

        rehomed_account_ids = {acc for (_, acc) in new_node_by_unit}
        return {
            "rehomed_count": len(new_node_by_unit),
            "leases_rehomed_count": len(lease_accounts & rehomed_account_ids),
            "allocations_rehomed_count": len(allocation_records),
            "rehome_blocked_count": len(blocked_units),
            "details": rehomed_details
        }

    def rehome_leases_for_domain(self, fault_domain: str, affected_nodes: List[ProxyNode]) -> Dict[str, Any]:
        """
        R3: ONE batch rehome for every account bound anywhere inside a failed domain,
        instead of one rehome sweep per listener. Called once per domain trip by the
        failure-domain isolator; all 511 grok2api accounts bound to a dead host migrate
        in a single batch (one epoch advance per service, one audit event).
        """
        with BINDING_WRITE_LOCK:
            return self._rehome_leases_for_domain(fault_domain, affected_nodes)

    def _rehome_leases_for_domain(self, fault_domain: str, affected_nodes: List[ProxyNode]) -> Dict[str, Any]:
        broken_names = {n.name for n in affected_nodes}
        if not broken_names:
            return {"rehomed_count": 0, "allocations_rehomed_count": 0, "details": []}

        # Union of leases + allocations across every listener in the domain. Migration
        # units are (service, account_id) pairs — one account_id can be bound in
        # several service namespaces with DIFFERENT constraint sets, and each binding
        # must be rehomed under its own promise (never aggregated by account_id).
        # Accounts that only exist in the legacy lease table are adopted into the
        # `default` namespace (same invariant `migrate_legacy_leases` maintains: a
        # lease implies an allocation), so the whole domain move is still ONE batch.
        lease_accounts: Dict[str, str] = {}
        stale_allocs: Dict[Tuple[str, str], str] = {}
        units: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for node_name in sorted(broken_names):
            for lease in self.storage.get_leases_for_node(node_name):
                lease_accounts.setdefault(lease.account_id, lease.node_name)
            for a in self.storage.get_allocations_for_node(node_name):
                stale_allocs[(a["service"], a["account_id"])] = a["node_name"]
                units.setdefault((a["service"], a["account_id"]), {"constraints": {}})
                c = self._constraints_from_alloc(a)
                if c.get("region"):
                    units[(a["service"], a["account_id"])]["constraints"] = c
        allocated_account_ids = {acc for (_, acc) in stale_allocs}
        for acc in lease_accounts:
            if acc not in allocated_account_ids:
                units.setdefault(("default", acc), {"constraints": {}})

        if not units:
            return {"rehomed_count": 0, "allocations_rehomed_count": 0, "details": []}

        anchor = self.storage.get_node(next(iter(sorted(broken_names)))) or affected_nodes[0]
        now_str = datetime.now().isoformat()

        # Unconstrained candidate ladder does not depend on the account — compute it
        # ONCE for every no-region unit (also shortens the BINDING_WRITE_LOCK section).
        base_candidates = self._find_rehome_candidates(
            anchor.region, anchor.egress_type, anchor.tier,
            exclude_names=broken_names
        )

        new_node_by_unit: Dict[Tuple[str, str], ProxyNode] = {}
        blocked_units: Dict[Tuple[str, str], str] = {}
        for key, info in units.items():
            svc_acc, acc = key
            c = info["constraints"]
            if c.get("region"):
                cands, blocked_region = self._find_rehome_candidates_for_account(
                    c, anchor.region, anchor.egress_type, anchor.tier,
                    exclude_names=broken_names
                )
            else:
                cands, blocked_region = (base_candidates or None), None
            if not cands:
                blocked_units[key] = blocked_region or "none_healthy"
                self._record_rehome_blocked(svc_acc, blocked_units[key])
                continue
            new_node_by_unit[key] = self._weighted_deterministic_pick(cands, acc)

        # Single transaction: allocation rows, legacy lease mirror, epoch advance and
        # change-log rows all commit together (no per-account set_lease round trips).
        # bound_* constraint columns are preserved (no constraints_by_account here).
        allocation_records: List[Tuple[str, str, str, str]] = [
            (svc, acc, new_node_by_unit[(svc, acc)].name, now_str)
            for (svc, acc) in sorted(units)
            if (svc, acc) in new_node_by_unit
        ]
        if allocation_records:
            self.storage.batch_save_allocations(allocation_records, reason="domain_rehome")

        rehomed_account_ids = {acc for (_, acc) in new_node_by_unit}
        rehomed_details = []
        for (svc_acc, acc) in sorted(units):
            base_detail = {"account_id": acc, "service": svc_acc,
                           "from_node": "domain:" + fault_domain}
            if (svc_acc, acc) in new_node_by_unit:
                base_detail["to_node"] = new_node_by_unit[(svc_acc, acc)].name
            else:
                base_detail.update({"to_node": None, "blocked": True,
                                    "blocked_region": blocked_units[(svc_acc, acc)]})
            rehomed_details.append(base_detail)

        self.storage.record_circuit_event(
            node_name=fault_domain,
            from_state=HealthStatus.DEAD.value,
            to_state="rehomed",
            reason=(
                f"domain_rehome_{len(new_node_by_unit)}_accounts_from_{len(broken_names)}_listeners"
                + (f"_{len(blocked_units)}_rehome_blocked" if blocked_units else "")
            )
        )
        self.storage.incr_metric("ip_pool_rehome_total", {"service": "domain", "trigger": "circuit_dead"})

        return {
            "rehomed_count": len(new_node_by_unit),
            "rehome_blocked_count": len(blocked_units),
            "leases_rehomed_count": len(set(lease_accounts) & rehomed_account_ids),
            "allocations_rehomed_count": len(allocation_records),
            "details": rehomed_details
        }

    def rehome_account_binding(
        self,
        account_id: str,
        failed_node_name: str,
        service: Optional[str] = None,
        trigger: str = "retry_exhausted",
        allow_cross_region: bool = True,
    ) -> Dict[str, Any]:
        """
        R2: persist a SINGLE account's binding onto a healthy node when the request-time
        retry budget is exhausted. Idempotent + deduplicated per account: a migrating
        request wins the BINDING_WRITE_LOCK; the other N concurrent requests observe the
        account already off the failed node and become no-ops, so exactly one migration
        happens for the whole burst.

        Stickyness: same region + egress_type + tier first; when no same-domain healthy
        node exists and `allow_cross_region`, falls back across regions and marks the
        transition `cross_region_rebind=True` (audited, metric-labeled).
        """
        with BINDING_WRITE_LOCK:
            svc = service or "default"

            # Dedup: another request already migrated this account, or the binding was
            # never on the failed node → nothing to do.
            current = self.storage.find_allocation_by_account(account_id, service=svc)
            current_node_name = current["node_name"] if current else None
            if current_node_name and current_node_name != failed_node_name:
                return {"rehomed": False, "reason": "already_rehomed", "node": current_node_name}

            failed = self.storage.get_node(failed_node_name)
            if not failed:
                return {"rehomed": False, "reason": "failed_node_missing"}

            # Constraint-aware target selection: the account's persisted bound_* set
            # (declared by its credential at bind time) overrides the failed node's own
            # attributes. A region-pinned account whose region has no healthy candidate
            # is NOT migrated abroad — it is left on the failed node and reported
            # `rehome_blocked` (the next acquire answers 503 pool_exhausted under
            # region_strict, which is the caller's own promise being honoured).
            alloc_c = self._constraints_from_alloc(
                self.storage.find_allocation_by_account(account_id, service=svc)
            )
            # R3 in both branches: a sibling listener of the host that just failed it
            # is the same outage, not a fallback — constraint-pinned or not.
            domain_peers = {
                n.name for n in self.storage.list_nodes_by_domain(failed.effective_fault_domain)
            }
            if alloc_c.get("region"):
                candidates, blocked_region = self._find_rehome_candidates_for_account(
                    alloc_c, failed.region, failed.egress_type, failed.tier,
                    exclude_names={failed_node_name} | domain_peers
                )
                if not candidates:
                    self._record_rehome_blocked(svc, blocked_region)
                    return {
                        "rehomed": False, "reason": "rehome_blocked",
                        "blocked_region": blocked_region,
                        "account_id": account_id, "service": svc,
                    }
            else:
                # Never re-park an account on a sibling listener of the host that just failed
                # it: in the incident all 25 ports of mesh-nl died together, so "another node
                # with the same host" is not a fallback, it is the same outage.
                candidates = self._find_rehome_candidates(
                    failed.region, failed.egress_type, failed.tier,
                    exclude_names={failed_node_name} | domain_peers
                )
                if not candidates:
                    return {"rehomed": False, "reason": "no_healthy_target"}

            candidates.sort(key=lambda x: x.name)
            selected = self._weighted_deterministic_pick(candidates, account_id)

            cross_region = bool(
                failed.region
                and selected.region
                and selected.region != failed.region
            )
            moved = self.storage.move_account_binding(
                service=svc,
                account_id=account_id,
                new_node_name=selected.name,
                reason=trigger,
                cross_region_rebind=cross_region,
            )
            if moved.get("moved"):
                self.storage.incr_metric(
                    "ip_pool_rehome_total",
                    {"service": svc, "trigger": trigger}
                )
            return {
                "rehomed": moved.get("moved", False),
                "reason": trigger,
                "from_node": failed_node_name,
                "to_node": selected.name,
                "cross_region_rebind": cross_region,
                "new_proxy_url": selected.proxy_url,
                "epoch": moved.get("epoch", 0),
            }

    @staticmethod
    def _weighted_deterministic_pick(candidates: List[ProxyNode], key: str) -> ProxyNode:
        """
        Deterministic weighted sticky pick over a name-sorted candidate list.

        Each node owns `weight` units of the CRC32 hash space (contiguous
        intervals, walked cumulatively); the same (key, candidate set, weights)
        triple always maps to the same node. With the default uniform weight
        this yields a uniform share — the mapping formula differs from the
        legacy `crc32 % len`, which only ever affected *new* bindings: existing
        stickiness lives in the allocation table, not in the hash.

        Non-uniform weights let ops steer traffic share (e.g. down-weight
        metered residential bridges relative to flat-rate WARP exits) without
        touching the routing code. Weights are clamped to >=1 so a bad
        nodes.yaml value can never zero out a node or divide by zero.

        Raises ValueError on an empty candidate list (all current call sites
        guard before calling; this keeps a future caller from hitting an
        opaque ZeroDivisionError).
        """
        if not candidates:
            raise ValueError("_weighted_deterministic_pick: empty candidate list")
        weights = [max(1, int(c.weight or 0)) for c in candidates]
        point = (zlib.crc32(key.encode("utf-8")) & 0xFFFFFFFF) % sum(weights)
        for c, w in zip(candidates, weights):
            point -= w
            if point < 0:
                return c
        return candidates[-1]

    def resolve_relay_hop(self, node: ProxyNode) -> Optional[ProxyNode]:
        """
        Resolves the designated relay transit hop for a node.
        If node.relay_hop_node is set, returns that node.
        Otherwise falls back to an available Hong Kong, Tokyo, or US bridge node.
        """
        # direct_only 节点 (环回桥, 教训 #3) 永远不取中继跳板: 提前返回, 省掉
        # 热路径上最多 4 个 hub 区 storage 查询 (否则每个请求都白扫后丢弃)。
        if node.relay_mode == RelayMode.DIRECT_ONLY.value:
            return None

        if node.relay_hop_node:
            hop = self.storage.get_node(node.relay_hop_node)
            if hop and hop.status == HealthStatus.HEALTHY.value:
                return hop

        # Fallback candidate pool: healthy clash_proxy nodes in key transit hubs
        # (10s TTL 缓存 —— 见 _hub_candidates; hop 只在直连拨号失败时才真正使用)
        valid = [h for h in self._hub_candidates() if h.name != node.name]
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

        # 3. Hash assignment (weighted — registration share follows node weight too)
        selected_node = self._weighted_deterministic_pick(
            clean_candidates, f"{account_id}:{target_service}")

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

