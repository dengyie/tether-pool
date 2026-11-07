"""
api/server.py - FastAPI service for the IP Pool System
"""

import os
import yaml
import asyncio
from contextlib import asynccontextmanager
from typing import Optional, List, Dict
from fastapi import FastAPI, Query, BackgroundTasks, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, HTMLResponse, RedirectResponse, JSONResponse

from core.models import (
    ProxyNode, PoolStats, LeaseInfo, ProbeResult, CircuitEvent,
    RegistrationLease, IpCooldownRecord, BatteryEventReport,
    BatchAllocationRequest, BatchAllocationResponse, AccountAllocationItem,
    AllocationOverviewResponse, NodeCreateRequest, NodeUpdateRequest,
    HealthStatus
)
from core.storage import Storage
from core.prober import Prober
from core.router import Router
from core.metrics import MetricsCollector
from core.allocator import AllocationEngine
from core.quality import IPQualityEvaluator
from core.locks import BINDING_WRITE_LOCK
from adapters.cpa_adapter import CpaAdapter

CONFIG_FILE = os.environ.get("IP_POOL_CONFIG", os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "nodes.yaml"))
DASHBOARD_HTML = os.path.join(os.path.dirname(os.path.dirname(__file__)), "web", "templates", "dashboard.html")

# Optional bearer-token auth for the control plane. When set, /health stays public and
# every other endpoint requires the token, provided as `Authorization: Bearer <token>`,
# a `?token=` query parameter, or the `ip_pool_token` cookie set by /dashboard.
API_TOKEN = os.environ.get("IP_POOL_API_TOKEN", "").strip()
AUTH_COOKIE_NAME = "ip_pool_token"
PROBE_INTERVAL_SECONDS = int(os.environ.get("IP_POOL_PROBE_INTERVAL", "300"))
PROBE_HISTORY_KEEP_DAYS = int(os.environ.get("IP_POOL_PROBE_HISTORY_KEEP_DAYS", "7"))
# Config files below this node count are treated as incomplete (e.g. a scratch
# override), so startup never acts on the registry based on them.
MIN_CONFIG_NODES_FOR_PRUNE = 10
# Deleting nodes missing from config also deletes their bindings. Default is to mark them
# DEAD and let self-healing re-home; set to 1 to accept the destructive cleanup.
PRUNE_MISSING_NODES = os.environ.get("IP_POOL_PRUNE_MISSING_NODES", "").strip() in ("1", "true", "yes")

storage = Storage()
router = Router(storage)
prober = Prober(storage, router=router)
allocator = AllocationEngine(storage)
metrics_collector = MetricsCollector(storage)
cpa_adapter = CpaAdapter(storage)

# Single-flight guard so overlapping sweeps (startup + manual trigger) cannot
# both hammer the pool at once.
_probe_lock = asyncio.Lock()


def load_initial_nodes():
    if not os.path.exists(CONFIG_FILE):
        print(f"[-] Config file not found: {CONFIG_FILE}")
        return 0
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
        if not data or not isinstance(data, dict):
            print(f"[-] Config file {CONFIG_FILE} parsed empty; skipping node load")
            return 0
        nodes_data = data.get("nodes", []) or []
        for item in nodes_data:
            configured_tier = item.get("tier")
            score, init_tier, _ = IPQualityEvaluator.evaluate_node(
                tags=item.get("tags", []),
                egress_type=item.get("egress_type"),
                status="unprobed",
                configured_tier=configured_tier
            )
            node = ProxyNode(
                name=item["name"],
                host=item["host"],
                port=item["port"],
                protocol=item.get("protocol", "socks5h"),
                region=item["region"],
                egress_type=item["egress_type"],
                tags=item.get("tags", []),
                weight=item.get("weight", 100),
                relay_mode=item.get("relay_mode", "relay_fallback"),
                relay_hop_node=item.get("relay_hop_node"),
                # Fault-domain identity may be declared in the authority config; when
                # omitted, ProxyNode derives it from host (see effective_fault_domain).
                fault_domain=item.get("fault_domain", "") or "",
                provider_region=item.get("provider_region", "") or "",
                tier=init_tier,
                configured_tier=configured_tier,
                quality_score=score
            )
            storage.upsert_node(node)
    print(f"[+] Loaded/upserted {len(nodes_data)} nodes into storage.")

    # Legacy `leases` rows without an allocation predate the universal layer; backfill
    # them so capacity accounting, monitoring and re-homing share one source of truth.
    migrated = storage.migrate_legacy_leases()
    if migrated:
        print(f"[+] Migrated {migrated} legacy lease(s) into account_allocations.")

    # Nodes absent from the authority config are marked DEAD so the existing self-healing
    # path can re-home their bindings. Hard deletion is destructive (it cascades to
    # account_allocations and leases) and therefore requires an explicit opt-in.
    if len(nodes_data) >= MIN_CONFIG_NODES_FOR_PRUNE:
        keep_names = [item["name"] for item in nodes_data]
        if PRUNE_MISSING_NODES:
            pruned = storage.prune_nodes_except(keep_names)
            if pruned:
                print(f"[!] Deleted {len(pruned)} nodes absent from {os.path.basename(CONFIG_FILE)} "
                      f"(IP_POOL_PRUNE_MISSING_NODES=1)")
        else:
            stale = storage.mark_nodes_missing_as_dead(keep_names)
            if stale:
                print(f"[+] Marked {len(stale)} nodes absent from {os.path.basename(CONFIG_FILE)} as DEAD "
                      f"(bindings preserved for self-healing)")
    return len(nodes_data)


async def background_probe_loop():
    while True:
        try:
            if _probe_lock.locked():
                print("[*] Probe sweep already in progress; skipping this tick.")
            else:
                async with _probe_lock:
                    print("[*] Starting scheduled full pool probe...")
                    results = await prober.probe_all(concurrency=25)
                    healthy = sum(1 for r in results if r.success)
                    print(f"[+] Scheduled probe finished: {healthy}/{len(results)} nodes healthy.")
                    pruned = storage.prune_event_tables(keep_days=PROBE_HISTORY_KEEP_DAYS)
                    pruned_total = {k: v for k, v in pruned.items() if v}
                    if pruned_total:
                        detail = ", ".join(f"{k}={v}" for k, v in pruned_total.items())
                        print(f"[+] Pruned stale event rows (>{PROBE_HISTORY_KEEP_DAYS}d): {detail}")
        except Exception as e:
            print(f"[-] Scheduled probe error: {e}")
        await asyncio.sleep(PROBE_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_initial_nodes()
    # Initial probe in background on startup
    probe_task = asyncio.create_task(background_probe_loop())
    yield
    # Await the cancellation so in-flight probe sockets are closed before shutdown
    # instead of being torn down mid-await during interpreter exit.
    probe_task.cancel()
    await asyncio.gather(probe_task, return_exceptions=True)


app = FastAPI(
    title="IP Pool System",
    description="Unified Global Egress IP Pool System with Dual-Stack Health Probing & Sticky Routing",
    version="1.0.0",
    lifespan=lifespan
)


def _provided_token(request: Request) -> str:
    """Token from the Authorization header, cookie, or query parameter."""
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[len("Bearer "):].strip()
    cookie = request.cookies.get(AUTH_COOKIE_NAME, "")
    if cookie:
        return cookie
    return request.query_params.get("token", "").strip()


@app.middleware("http")
async def bearer_auth_middleware(request: Request, call_next):
    if API_TOKEN and request.url.path != "/health":
        token = _provided_token(request)
        if token != API_TOKEN:
            # Browsers cannot attach an Authorization header to a navigation request,
            # so entering the dashboard is a two-step handshake: ?token=... is exchanged
            # for an HttpOnly cookie, after which the page's same-origin fetches carry it.
            if request.url.path in ("/", "/dashboard") and request.method in ("GET", "HEAD"):
                return HTMLResponse(
                    "<html><body style='font-family:system-ui;padding:2rem'>"
                    "<h3>IP Pool control plane</h3>"
                    "<p>Access requires a token. Open <code>/dashboard?token=&lt;YOUR_TOKEN&gt;</code>.</p>"
                    "</body></html>",
                    status_code=401,
                )
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)
        if request.query_params.get("token") and request.url.path == "/dashboard":
            response = RedirectResponse(url="/dashboard", status_code=302)
            response.set_cookie(
                AUTH_COOKIE_NAME, token, httponly=True, samesite="lax", path="/"
            )
            return response
    return await call_next(request)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def index():
    return RedirectResponse(url="/dashboard")


@app.api_route("/dashboard", methods=["GET", "HEAD"], response_class=HTMLResponse)
def get_dashboard():
    if os.path.exists(DASHBOARD_HTML):
        with open(DASHBOARD_HTML, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Dashboard file not found</h1>", status_code=404)


@app.api_route("/health", methods=["GET", "HEAD"])
def health():
    return {"status": "ok", "service": "ip-pool"}


@app.get("/v1/stats", response_model=PoolStats)
def get_stats():
    return storage.get_stats()


@app.get("/v1/nodes", response_model=List[ProxyNode])
def list_nodes(
    region: Optional[str] = None,
    egress_type: Optional[str] = None,
    status: Optional[str] = None,
    tier: Optional[str] = Query(None, description="Optional quality tier filter: premium or standard")
):
    return storage.list_nodes(region=region, egress_type=egress_type, status=status, tier=tier)


@app.get("/v1/nodes/{name}", response_model=ProxyNode)
def get_node(name: str):
    """Retrieves a single proxy node by its unique identifier."""
    node = storage.get_node(name)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node '{name}' not found")
    return node


@app.post("/v1/nodes", response_model=ProxyNode)
def create_or_upsert_node(req: NodeCreateRequest):
    """
    Directly creates or updates a proxy node in the pool via API.
    Evaluates quality scores and configures quality tiers dynamically.
    Tags node with 'api_managed' to prevent pruning or demotion during static config reloads.
    """
    configured_tier = req.configured_tier or req.tier
    tags = list(req.tags)
    if "api_managed" not in tags:
        tags.append("api_managed")

    score, init_tier, _ = IPQualityEvaluator.evaluate_node(
        tags=tags,
        egress_type=req.egress_type,
        status="unprobed",
        configured_tier=configured_tier
    )
    final_tier = req.tier or init_tier
    node = ProxyNode(
        name=req.name,
        host=req.host,
        port=req.port,
        protocol=req.protocol,
        region=req.region,
        egress_type=req.egress_type,
        tags=tags,
        weight=req.weight,
        relay_mode=req.relay_mode,
        relay_hop_node=req.relay_hop_node,
        tier=final_tier,
        configured_tier=configured_tier,
        quality_score=score,
        fault_domain=req.fault_domain,
        provider_region=req.provider_region
    )
    storage.upsert_node(node)
    saved = storage.get_node(req.name)
    if not saved:
        raise HTTPException(status_code=500, detail="Failed to save node")
    return saved


@app.patch("/v1/nodes/{name}", response_model=ProxyNode)
def update_node(name: str, req: NodeUpdateRequest):
    """
    Updates specific attributes of an existing proxy node (weight, tags, tier, status, relay).
    If status is set to dead, automatically triggers rehoming of active account bindings.
    """
    existing = storage.get_node(name)
    if not existing:
        raise HTTPException(status_code=404, detail=f"Node '{name}' not found")

    updates = req.model_dump(exclude_unset=True)
    if not updates:
        return existing

    if "tier" in updates and "configured_tier" not in updates:
        updates["configured_tier"] = updates["tier"]

    updated_node = storage.update_node(name, updates)
    if not updated_node:
        raise HTTPException(status_code=404, detail=f"Node '{name}' not found")

    if updates.get("status") == HealthStatus.DEAD.value:
        router.rehome_leases_for_node(name)

    return updated_node


@app.delete("/v1/nodes/{name}")
def delete_node(
    name: str,
    rehome: bool = Query(True, description="Automatically rehome attached accounts before deleting")
):
    """
    Deletes a proxy node from the pool.
    Optionally auto-rehomes any bound accounts before removal.
    Uses BINDING_WRITE_LOCK and pre-marks node as DEAD so concurrent acquisitions cannot target it.
    """
    with BINDING_WRITE_LOCK:
        existing = storage.get_node(name)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Node '{name}' not found")

        # Pre-mark node as DEAD so no concurrent requests can select it during rehoming
        storage.update_node(name, {"status": HealthStatus.DEAD.value, "circuit_state": HealthStatus.DEAD.value})

        rehome_result = None
        if rehome:
            rehome_result = router.rehome_leases_for_node(name)

        deleted = storage.delete_node(name, prune_bindings=True)
        return {
            "deleted": deleted,
            "node_name": name,
            "rehome_result": rehome_result
        }


@app.post("/v1/config/reload")
def reload_config():
    """
    Hot-reloads node topology from disk without requiring a systemd service restart.
    """
    try:
        count = load_initial_nodes()
        return {
            "reloaded": True,
            "config_file": CONFIG_FILE,
            "nodes_loaded": count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to reload config: {str(e)}")



def _lease_from_allocation(account_id: str, alloc: dict, st: Optional[Storage] = None) -> Optional[LeaseInfo]:
    target_storage = st or storage
    node = target_storage.get_node(alloc["node_name"])
    if not node:
        return None
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


@app.get("/v1/acquire", response_model=LeaseInfo)
def get_bound_lease(
    account_id: str = Query(..., description="Unique account ID / email"),
    service: Optional[str] = Query(None, description="Service namespace (defaults to 'default')")
):
    """
    Reads the account's current binding without creating one.

    Acquiring is a state change and lives on POST; a GET must stay safe so that
    prefetches, retries and link previews cannot silently bind accounts or consume
    capacity.
    """
    svc = service or "default"
    alloc = storage.find_allocation_by_account(account_id, service=svc)
    lease = _lease_from_allocation(account_id, alloc) if alloc else None
    if not lease:
        legacy = storage.get_lease(account_id)
        if legacy:
            lease = legacy
    if not lease:
        raise HTTPException(status_code=404, detail=f"No binding for account '{account_id}' in service '{svc}'")
    return lease


@app.post("/v1/acquire", response_model=LeaseInfo)
def acquire_node(
    account_id: str = Query(..., description="Unique account ID / email"),
    egress_type: Optional[str] = Query(None, description="Optional egress type constraint (warp_ipv6, native_ipv4, native_ipv6)"),
    region: Optional[str] = Query(None, description="Optional target region (tokyo, netherlands, singapore, india)"),
    tier: Optional[str] = Query(None, description="Optional quality tier constraint (premium or standard)"),
    tier_policy: Optional[str] = Query(None, description="Optional tier routing policy (standard_first, premium_first, premium_only, standard_only)"),
    force_rebind: bool = Query(False, description="Force re-allocating a new node"),
    service: Optional[str] = Query(None, description="Service namespace (defaults to 'default')")
):
    """
    Acquires (or returns) the account's sticky binding.

    `egress_type`/`region`/`tier` are optional constraints for this call: when the account
    already has a healthy binding that does not match them, the constrained node is
    returned without overwriting the stored binding. Use `force_rebind=true` to
    deliberately re-point the account.
    """
    lease = router.acquire_for_account(
        account_id=account_id,
        egress_type=egress_type,
        region=region,
        tier=tier,
        tier_policy=tier_policy,
        force_rebind=force_rebind,
        service=service
    )
    if not lease:
        raise HTTPException(status_code=503, detail="No available proxy node matching criteria")
    return lease


@app.post("/v1/probe")
async def trigger_probe(background_tasks: BackgroundTasks):
    async def _run_probe():
        if _probe_lock.locked():
            return
        async with _probe_lock:
            await prober.probe_all(concurrency=25)
    background_tasks.add_task(_run_probe)
    return {"message": "Full pool probe triggered in background"}


@app.post("/v1/probe/test_node", response_model=ProbeResult)
async def test_single_node(node_name: str = Query(..., description="Node name to test")):
    """Immediately test a single node over EasyTier and return live egress result"""
    node = storage.get_node(node_name)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node {node_name} not found")
    res = await prober.probe_node(node, force=True)
    return res


@app.post("/v1/probe/target", response_model=ProbeResult)
async def test_target_service(
    node_name: str = Query(..., description="Node name to test"),
    target_url: str = Query(..., description="Target service URL to probe (e.g. https://x.com/i/flow/signup)")
):
    """Probe a specific business target URL through a proxy node to test anti-bot evasion"""
    node = storage.get_node(node_name)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node {node_name} not found")
    res = await prober.probe_target_service(node, target_url)
    return res


@app.get("/v1/bindings/epoch")
def get_bindings_epoch(service: str = Query("default", min_length=1)):
    """
    R5(b): current monotonic binding epoch for a service.

    Downstreams poll this cheaply; when `epoch` differs from the value they last
    reconciled they pull `GET /v1/export/grok2api?since_epoch=N` for the delta.
    This replaces "import once and trust it forever", the pattern that let 511
    grok2api accounts keep dialling mesh-nl after the host was gone.
    """
    info = storage.get_binding_epoch(service)
    if info is None:
        return {"service": service, "epoch": 0, "changed_at": None, "last_change_reason": None}
    return info


@app.get("/v1/export/grok2api")
def export_grok2api(
    since_epoch: Optional[int] = Query(
        None, ge=0,
        description="When set, return only the bindings that moved after this epoch"
    )
):
    """Export healthy nodes formatted for grok2api egress node injection.

    With `since_epoch=N` the response carries only accounts whose binding moved since epoch
    N (mapped to their *current* node, not the intermediate hop) plus the current epoch, so
    a downstream can reconcile incrementally instead of doing a one-time static import.
    """
    if since_epoch is not None:
        service = "grok2api"
        changes = storage.list_binding_changes_since(service, since_epoch)
        current = storage.get_binding_epoch(service) or {
            "epoch": 0, "changed_at": None, "last_change_reason": None
        }
        assignments = {}
        for change in changes:
            acc = change.account_id
            if acc in assignments:
                continue
            alloc = storage.get_allocation(service, acc)
            node = storage.get_node(alloc["node_name"]) if alloc else None
            if not node:
                continue
            assignments[acc] = {
                "name": node.name,
                "url": f"socks5://{node.host}:{node.port}",
                "region": node.region,
                "egress_type": node.egress_type,
                "exit_ip": node.exit_ip or ""
            }
        return {
            "count": len(assignments),
            "nodes": list(assignments.values()),
            "assignments": assignments,
            "epoch": current["epoch"],
            "since_epoch": since_epoch,
            "changed_at": current.get("changed_at"),
            "last_change_reason": current.get("last_change_reason"),
            "reason_breakdown": _binding_reason_breakdown(changes),
        }

    nodes = storage.list_nodes(status="healthy")
    if not nodes:
        nodes = storage.list_nodes()  # fallback

    egress_nodes = []
    for n in nodes:
        egress_nodes.append({
            "name": n.name,
            "url": f"socks5://{n.host}:{n.port}",
            "region": n.region,
            "egress_type": n.egress_type,
            "exit_ip": n.exit_ip or ""
        })
    return {"count": len(egress_nodes), "nodes": egress_nodes}


def _binding_reason_breakdown(changes) -> Dict[str, int]:
    breakdown: Dict[str, int] = {}
    for change in changes:
        breakdown[change.reason] = breakdown.get(change.reason, 0) + 1
    return breakdown


@app.get("/v1/export/cpa")
def export_cpa(
    region: Optional[str] = Query(None, description="Optional region filter"),
    egress_type: Optional[str] = Query(None, description="Optional egress type filter")
):
    """Export native and WARP nodes for CPA configuration via CpaAdapter"""
    return cpa_adapter.generate_cpa_config(region=region, egress_type=egress_type)


@app.get("/metrics", response_class=PlainTextResponse)
def get_metrics():
    """Prometheus OpenMetrics exporter with low-cardinality labels"""
    return PlainTextResponse(
        content=metrics_collector.generate_metrics(),
        media_type="text/plain; version=0.0.4; charset=utf-8"
    )


@app.get("/v1/circuit/events", response_model=List[CircuitEvent])
def list_circuit_events(limit: int = Query(50, ge=1, le=200)):
    """Retrieve recent circuit breaker transition and self-healing events"""
    return storage.list_circuit_events(limit=limit)


@app.post("/v1/circuit/reset")
def reset_node_circuit(node_name: str = Query(..., description="Node name to reset")):
    """Operator manual reset of a node back to healthy state"""
    success = storage.reset_node_circuit(node_name)
    if not success:
        raise HTTPException(status_code=404, detail=f"Node {node_name} not found")
    return {"message": f"Node {node_name} circuit reset to healthy"}


@app.post("/v1/circuit/rehome")
def trigger_node_rehome(node_name: str = Query(..., description="Node name whose leases should be migrated")):
    """Trigger manual lease re-homing for a node"""
    result = router.rehome_leases_for_node(node_name)
    return result


# --- Registration Anti-Sybil Endpoints ---

@app.get("/v1/acquire_for_registration", response_model=RegistrationLease)
def acquire_for_registration(
    account_id: str = Query(..., description="Unique registration account ID / email"),
    target_service: str = Query(..., description="Target service identifier (e.g., grok_reg, openai_signup)"),
    cooldown: int = Query(1800, description="IP cooldown seconds after registration"),
    region: Optional[str] = Query(None, description="Optional target region preference"),
    egress_type: Optional[str] = Query(None, description="Optional preferred egress type (clash_proxy, native_ipv4, warp_ipv6)"),
    tier: Optional[str] = Query("premium", description="Optional quality tier preference (defaults to premium for registration protection)")
):
    """
    Dedicated anti-sybil egress lease assignment for batch registration bots.
    Ensures exit IP has not been used for target_service within the cooldown period.
    """
    lease = router.acquire_for_registration(
        account_id=account_id,
        target_service=target_service,
        region=region,
        egress_type=egress_type,
        tier=tier,
        cooldown_seconds=cooldown
    )
    if not lease:
        raise HTTPException(status_code=503, detail="No clean proxy node available for registration (all candidates cooled)")
    return lease


@app.get("/v1/cooldowns", response_model=List[IpCooldownRecord])
def list_cooldowns(target_service: Optional[str] = Query(None, description="Filter by target service")):
    """List active IP cooldown locks for anti-sybil tracking"""
    return storage.get_active_cooldowns(target_service=target_service)


@app.post("/v1/release_cooldown")
def release_cooldown(
    exit_ip: str = Query(..., description="Exit IP to release"),
    target_service: str = Query(..., description="Target service")
):
    """Early unlock of an IP from cooldown (e.g. registration failed before form submission)"""
    success = storage.release_ip_cooldown(exit_ip=exit_ip, target_service=target_service)
    return {"success": success, "exit_ip": exit_ip, "target_service": target_service}


# --- Battery Lifecycle & Hot Swapping Endpoints ---

@app.post("/v1/battery/event")
def report_battery_event(report: BatteryEventReport):
    """
    Downstream battery consumers (e.g., grok2api, cpa-manager-plus) report 429/401/Turnstile failures.
    Triggers automatic battery swapping (lease re-homing to a fresh healthy node).
    """
    result = router.handle_battery_event(report)
    return result


@app.get("/v1/battery/events")
def list_battery_events(
    account_id: Optional[str] = Query(None, description="Filter by account ID"),
    limit: int = Query(50, ge=1, le=200)
):
    """List recent battery lifecycle events"""
    return storage.list_battery_events(account_id=account_id, limit=limit)


# --- Universal Account Allocation & Distribution Endpoints ---

@app.post("/v1/allocation/batch", response_model=BatchAllocationResponse)
def batch_allocate_accounts(request: BatchAllocationRequest):
    """
    Universal batch account allocation interface (e.g. grok2api 2,100+ accounts).
    Evenly distributes accounts across pool exits (e.g. 100 WARP IPv6 nodes) with minimal variance.
    Generates both direct SOCKS5 and transparent gateway URLs.
    """
    try:
        return allocator.batch_allocate(request)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/v1/allocation/acquire", response_model=AccountAllocationItem)
def acquire_account_allocation(
    service: str = Query(..., description="Service identifier, e.g. grok2api"),
    account_id: str = Query(..., description="Unique account ID or email"),
    egress_preference: Optional[str] = Query("warp_ipv6", description="Preferred egress type"),
    region: Optional[str] = Query(None, description="Optional region preference"),
    tier: Optional[str] = Query(None, description="Optional tier filter: premium or standard"),
    tier_policy: Optional[str] = Query(None, description="Optional tier policy: standard_first, premium_first, premium_only, standard_only"),
    force_rebalance: bool = Query(False, description="Force reallocating to a different node")
):
    """
    On-demand allocation for a single account with load-aware capacity distribution.
    """
    try:
        return allocator.acquire_single(
            service=service,
            account_id=account_id,
            egress_preference=egress_preference,
            region=region,
            tier=tier,
            tier_policy=tier_policy,
            force_rebalance=force_rebalance
        )
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/v1/allocation/status", response_model=AllocationOverviewResponse)
def get_allocation_overview():
    """
    Statistical overview of account distributions and node capacity utilization.
    """
    return storage.get_allocations_overview()


@app.get("/v1/allocation/accounts")
def list_service_allocations(service: str = Query(..., description="Service identifier")):
    """
    List all active account allocations for a given service.
    """
    return storage.list_allocations_by_service(service)


@app.post("/v1/allocation/release")
def release_account_allocation(
    service: str = Query(..., description="Service identifier"),
    account_id: str = Query(..., description="Account ID to release")
):
    """
    Release or unbind an account allocation.
    """
    success = storage.delete_allocation(service, account_id)
    return {"success": success, "service": service, "account_id": account_id}



