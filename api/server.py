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

from core.models import ProxyNode, PoolStats, LeaseInfo, ProbeResult
from core.storage import Storage
from core.prober import Prober
from core.router import Router

CONFIG_FILE = os.environ.get("IP_POOL_CONFIG", os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "nodes.yaml"))

storage = Storage()
prober = Prober(storage)
router = Router(storage)


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


@app.get("/health")
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
def export_cpa():
    """Export native and WARP nodes for CPA configuration"""
    nodes = storage.list_nodes(status="healthy")
    return {
        "proxies": [
            {
                "id": n.name,
                "proxy_url": f"socks5://{n.host}:{n.port}",
                "region": n.region,
                "type": n.egress_type,
                "exit_ip": n.exit_ip or ""
            }
            for n in nodes
        ]
    }
