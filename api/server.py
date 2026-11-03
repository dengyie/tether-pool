"""
api/server.py - FastAPI service for the IP Pool System
"""

import os
import yaml
import asyncio
from contextlib import asynccontextmanager
from typing import Optional, List
from fastapi import FastAPI, Query, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, HTMLResponse, RedirectResponse

from core.models import ProxyNode, PoolStats, LeaseInfo, ProbeResult, CircuitEvent
from core.storage import Storage
from core.prober import Prober
from core.router import Router
from core.metrics import MetricsCollector
from adapters.cpa_adapter import CpaAdapter

CONFIG_FILE = os.environ.get("IP_POOL_CONFIG", os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "nodes.yaml"))
DASHBOARD_HTML = os.path.join(os.path.dirname(os.path.dirname(__file__)), "web", "templates", "dashboard.html")

storage = Storage()
router = Router(storage)
prober = Prober(storage, router=router)
metrics_collector = MetricsCollector(storage)
cpa_adapter = CpaAdapter(storage)


def load_initial_nodes():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            nodes_data = data.get("nodes", [])
            for item in nodes_data:
                node = ProxyNode(
                    name=item["name"],
                    host=item["host"],
                    port=item["port"],
                    protocol=item.get("protocol", "socks5h"),
                    region=item["region"],
                    egress_type=item["egress_type"],
                    tags=item.get("tags", []),
                    weight=item.get("weight", 100)
                )
                storage.upsert_node(node)
        print(f"[+] Loaded/upserted {len(nodes_data)} nodes into storage.")


async def background_probe_loop():
    while True:
        try:
            print("[*] Starting scheduled full pool probe...")
            results = await prober.probe_all(concurrency=25)
            healthy = sum(1 for r in results if r.success)
            print(f"[+] Scheduled probe finished: {healthy}/{len(results)} nodes healthy.")
        except Exception as e:
            print(f"[-] Scheduled probe error: {e}")
        await asyncio.sleep(300)  # Every 5 minutes


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_initial_nodes()
    # Initial probe in background on startup
    probe_task = asyncio.create_task(background_probe_loop())
    yield
    probe_task.cancel()


app = FastAPI(
    title="IP Pool System",
    description="Unified Global Egress IP Pool System with Dual-Stack Health Probing & Sticky Routing",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
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
    status: Optional[str] = None
):
    return storage.list_nodes(region=region, egress_type=egress_type, status=status)


@app.get("/v1/acquire", response_model=LeaseInfo)
def acquire_node(
    account_id: str = Query(..., description="Unique account ID / email"),
    egress_type: Optional[str] = Query("warp_ipv6", description="Preferred egress type (warp_ipv6, native_ipv4, native_ipv6)"),
    region: Optional[str] = Query(None, description="Optional target region (tokyo, netherlands, singapore, india)"),
    force_rebind: bool = Query(False, description="Force re-allocating a new node")
):
    lease = router.acquire_for_account(
        account_id=account_id,
        egress_type=egress_type,
        region=region,
        force_rebind=force_rebind
    )
    if not lease:
        raise HTTPException(status_code=503, detail="No available proxy node matching criteria")
    return lease


@app.post("/v1/probe")
async def trigger_probe(background_tasks: BackgroundTasks):
    async def _run_probe():
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


@app.get("/v1/export/grok2api")
def export_grok2api():
    """Export healthy nodes formatted for grok2api egress node injection"""
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

