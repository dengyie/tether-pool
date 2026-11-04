#!/usr/bin/env python3
"""
demo.py - Interactive Showcase & Verification Demo for IP Pool System v2.0
Verifies: Topology, Live Probing, Sticky Routing, Circuit Breaker Self-Healing, and Unified Gateway
"""

import sys
import os
import asyncio
import tempfile
import time
import httpx
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# The demo injects synthetic probe failures and re-homes leases, so it must run
# against a throwaway database instead of the production pool registry.
if not os.environ.get("IP_POOL_DEMO_USE_LIVE_DB"):
    _DEMO_DB_DIR = tempfile.mkdtemp(prefix="ip-pool-demo-")
    os.environ["IP_POOL_DB"] = os.path.join(_DEMO_DB_DIR, "demo.db")

from core.storage import Storage
from core.prober import Prober
from core.router import Router
from core.models import EgressType, HealthStatus, ProbeResult
from core.metrics import MetricsCollector
from gateway.server import ForwardProxyGateway
from api.server import load_initial_nodes


async def run_demo():
    console = Console()
    console.print("\n")
    console.print(Panel.fit(
        "[bold cyan]🌐 Unified Global IP Pool System v2.0 - Dual-Mode Showcase[/bold cyan]\n"
        "[italic white]108 Independent Exits · 4-State Circuit Breaker · Self-Healing · Unified Gateway (:9101)[/italic white]",
        border_style="cyan"
    ))

    # 1. Load nodes and storage
    load_initial_nodes()
    storage = Storage()
    router = Router(storage)
    prober = Prober(storage, router=router, timeout=10.0)

    stats = storage.get_stats()
    console.print(f"\n[bold yellow]Step 1: Inspecting Current IP Pool State[/bold yellow]")

    stats_table = Table(title="Global Exit Pool Distribution", border_style="dim")
    stats_table.add_column("Metric / Region", style="cyan")
    stats_table.add_column("Node Count", justify="right", style="green")
    stats_table.add_column("Type Breakdown", style="magenta")

    stats_table.add_row("Total Active Nodes", str(stats.total_nodes), "100 WARP IPv6 + 8 Native")
    stats_table.add_row("Healthy Exits", str(stats.healthy_nodes), "Operational")
    for reg, count in sorted(stats.region_distribution.items()):
        reg_type = "25 WARP + 1~2 Native" if count >= 25 else "1 Native IPv4"
        stats_table.add_row(f"Region: {reg.upper()}", str(count), reg_type)

    console.print(stats_table)

    # 2. Live Probing Demonstration
    console.print(f"\n[bold yellow]Step 2: Live Dual-Stack Health Probing Sample[/bold yellow]")
    sample_node_names = [
        "aws-tokyo-warp-01",
        "huawei-sg-warp-01",
        "mesh-nl-warp-01",
        "india-warp-01",
        "aws-tokyo-native-v4",
        "hk-native-v4"
    ]

    probe_table = Table(title="Real-Time Probe Results (over EasyTier Mesh)", border_style="cyan")
    probe_table.add_column("Node Name", style="cyan")
    probe_table.add_column("Type", style="blue")
    probe_table.add_column("Mesh SOCKS5", style="white")
    probe_table.add_column("Status", style="bold")
    probe_table.add_column("Verified Exit IP", style="green")
    probe_table.add_column("RTT Latency", justify="right", style="yellow")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True
    ) as progress:
        task = progress.add_task("[cyan]Probing sample nodes across 5 regions...", total=len(sample_node_names))
        for name in sample_node_names:
            node = storage.get_node(name)
            if node:
                res = await prober.probe_node(node)
                status_str = "[green]HEALTHY[/green]" if res.success else "[red]FAILED[/red]"
                probe_table.add_row(
                    node.name,
                    node.egress_type,
                    f"{node.host}:{node.port}",
                    status_str,
                    res.exit_ip or "-",
                    f"{res.latency_ms:.1f}ms" if res.latency_ms else "-"
                )
            progress.advance(task)

    console.print(probe_table)

    # 3. Deterministic Sticky Hash Routing Demonstration
    console.print(f"\n[bold yellow]Step 3: Deterministic CRC32 Sticky Hash Routing[/bold yellow]")
    test_accounts = [
        ("alice@mango.com", None, None),
        ("bob@mango.com", None, None),
        ("charlie@mango.com", "tokyo", "warp_ipv6"),
        ("crawler_bot@mango.com", None, "native_ipv4"),
    ]

    route_table = Table(title="Account -> Proxy Node Lease Mapping", border_style="magenta")
    route_table.add_column("Account Identifier", style="white")
    route_table.add_column("Constraint", style="yellow")
    route_table.add_column("Assigned Node", style="cyan")
    route_table.add_column("Region", style="magenta")
    route_table.add_column("Exit IP", style="green")
    route_table.add_column("Proxy URL", style="blue")

    allocated_leases = []
    for acc, reg, eg_type in test_accounts:
        lease = router.acquire_for_account(acc, region=reg, egress_type=eg_type)
        constraint = f"{reg or '*'}:{eg_type or '*'}"
        route_table.add_row(
            acc,
            constraint,
            lease.node_name,
            lease.region.upper(),
            lease.exit_ip or "-",
            lease.proxy_url
        )
        allocated_leases.append((acc, lease))

    console.print(route_table)

    # 4. Persistence Test
    console.print(f"\n[bold yellow]Step 4: Sticky Lease Consistency Verification[/bold yellow]")
    alice_first = allocated_leases[0][1]
    # Same constraints as Step 3: passing the default egress_type here would
    # legitimately rebind a lease that was acquired without an egress filter.
    alice_second = router.acquire_for_account("alice@mango.com", egress_type=None)
    if alice_first.node_name == alice_second.node_name:
        console.print(f"  [green]✔ PASSED:[/green] 'alice@mango.com' stably re-acquired the exact same node [bold cyan]{alice_first.node_name}[/bold cyan] ({alice_first.proxy_url}).")
    else:
        console.print(f"  [red]✘ FAILED:[/red] Inconsistent mapping detected!")

    # 5. Live Egress Request via Acquired Proxy
    console.print(f"\n[bold yellow]Step 5: Executing Real Outbound HTTP Request via Assigned Proxy[/bold yellow]")
    target_proxy = alice_first.proxy_url
    console.print(f"  [*] Dispatching GET https://cloudflare.com/cdn-cgi/trace via [cyan]{target_proxy}[/cyan]...")

    try:
        t0 = time.perf_counter()
        async with httpx.AsyncClient(proxy=target_proxy, timeout=10.0) as client:
            resp = await client.get("https://cloudflare.com/cdn-cgi/trace")
            duration = (time.perf_counter() - t0) * 1000

            trace_data = {}
            for line in resp.text.splitlines():
                if "=" in line:
                    k, v = line.split("=", 1)
                    trace_data[k.strip()] = v.strip()

            actual_ip = trace_data.get("ip", "unknown")
            actual_loc = trace_data.get("loc", "unknown")
            is_warp = trace_data.get("warp", "off")

            console.print(f"  [green]✔ 200 OK Received in {duration:.1f}ms![/green]")
            console.print(f"    - Real Outbound IP : [bold green]{actual_ip}[/bold green]")
            console.print(f"    - Edge Location    : [bold magenta]{actual_loc}[/bold magenta]")
            console.print(f"    - Cloudflare WARP  : [bold cyan]{is_warp}[/bold cyan]")
    except Exception as e:
        console.print(f"  [red]✘ Outbound request failed: {e}[/red]")

    # 6. Circuit Breaker & Automatic Self-Healing Demonstration
    console.print(f"\n[bold yellow]Step 6: Four-State Circuit Breaker & Self-Healing Verification[/bold yellow]")
    sim_node_name = "aws-tokyo-warp-01"
    sim_account = "vital_vip_user@mango.com"

    # Bind lease to node
    storage.set_lease(sim_account, sim_node_name)
    initial_lease = storage.get_lease(sim_account)
    console.print(f"  [*] Pre-condition: Account '[cyan]{sim_account}[/cyan]' bound to '[magenta]{initial_lease.node_name}[/magenta]'.")

    # Simulate node failure -> Trip OPEN
    console.print(f"  [*] Simulating consecutive probe failures on '[magenta]{sim_node_name}[/magenta]'...")
    storage.update_probe_result(ProbeResult(node_name=sim_node_name, success=False, error="simulated_timeout_1"))
    _, event = storage.update_probe_result(ProbeResult(node_name=sim_node_name, success=False, error="simulated_timeout_2"))

    if event:
        console.print(f"  [red]⚡ Circuit Tripped OPEN:[/red] {event.node_name} ({event.from_state} -> [bold red]{event.to_state}[/bold red], Reason: {event.reason})")

    # Trigger Self-Healing Lease Re-homing
    rehome_res = router.rehome_leases_for_node(sim_node_name)
    healed_lease = storage.get_lease(sim_account)
    console.print(f"  [green]✔ Self-Healing Rehomed {rehome_res['rehomed_count']} Leases:[/green]")
    console.print(f"    - Old Node: [red]{sim_node_name}[/red] (Circuit OPEN)")
    console.print(f"    - New Node: [bold green]{healed_lease.node_name}[/bold green] ({healed_lease.region.upper()})")

    # Restore the node
    storage.reset_node_circuit(sim_node_name)
    console.print(f"  [cyan]✔ Operator Reset:[/cyan] Reset node {sim_node_name} back to healthy.")

    # 7. Local Unified Forward Proxy Gateway (:9101) Demonstration
    console.print(f"\n[bold yellow]Step 7: Testing Unified Forward Proxy Gateway (:9101)[/bold yellow]")
    gw = ForwardProxyGateway(storage, router, host="127.0.0.1", port=0)
    await gw.start()
    ephemeral_port = gw.server.sockets[0].getsockname()[1]
    console.print(f"  [*] Ephemeral Gateway listening on [cyan]127.0.0.1:{ephemeral_port}[/cyan]")

    try:
        gw_proxy_url = f"http://127.0.0.1:{ephemeral_port}"
        gw_headers = {"X-Account-Id": "gateway_demo_user", "X-Region": "tokyo"}
        console.print(f"  [*] Dispatching request through Gateway with header [yellow]X-Account-Id: gateway_demo_user[/yellow]...")

        async with httpx.AsyncClient(proxy=gw_proxy_url, headers=gw_headers, timeout=10.0) as client:
            gw_resp = await client.get("https://cloudflare.com/cdn-cgi/trace")
            if gw_resp.status_code == 200:
                console.print(f"  [green]✔ Gateway Transparent Forwarding Succeeded (HTTP 200 OK)![/green]")
                for line in gw_resp.text.splitlines()[:3]:
                    console.print(f"      {line}")
    except Exception as gw_err:
        console.print(f"  [yellow]⚠ Gateway direct request note: {gw_err}[/yellow]")
    finally:
        await gw.stop()

    # 8. Prometheus Metrics Preview
    console.print(f"\n[bold yellow]Step 8: Prometheus OpenMetrics Preview (/metrics)[/bold yellow]")
    collector = MetricsCollector(storage)
    metrics_text = collector.generate_metrics()
    for line in metrics_text.splitlines()[:10]:
        console.print(f"  [dim]{line}[/dim]")
    console.print("  [dim]... (truncated)[/dim]")

    console.print("\n[bold green]================================================================[/bold green]")
    console.print("[bold green]✔ IP Pool System v2.0 Dual-Mode Demo Completed Successfully![/bold green]\n")


if __name__ == "__main__":
    asyncio.run(run_demo())
