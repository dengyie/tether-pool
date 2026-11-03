#!/usr/bin/env python3
"""
scripts/probe_all.py - CLI tool to trigger a full probe and display summary table
"""

import sys
import os
import asyncio
from rich.console import Console
from rich.table import Table

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.storage import Storage
from core.prober import Prober
from api.server import load_initial_nodes


async def main():
    console = Console()
    console.print("\n[bold cyan]=== IP Pool - Global Dual-Stack Health Prober ===[/bold cyan]")

    load_initial_nodes()
    storage = Storage()
    prober = Prober(storage, timeout=12.0)

    console.print("[yellow][*] Probing 108 nodes across Tokyo, Netherlands, Singapore, India, Hong Kong, and US-West...[/yellow]")
    results = await prober.probe_all(concurrency=15)

    # Build rich table
    table = Table(title="Global Proxy Node Pool Status")
    table.add_column("Node Name", style="cyan", no_wrap=True)
    table.add_column("Region", style="magenta")
    table.add_column("Type", style="blue")
    table.add_column("Host:Port", style="white")
    table.add_column("Status", style="bold")
    table.add_column("Exit IP", style="green")
    table.add_column("Latency", justify="right", style="yellow")

    healthy = 0
    degraded = 0
    dead = 0

    nodes = storage.list_nodes()
    for n in nodes:
        status_str = f"[green]HEALTHY[/green]" if n.status == "healthy" else (f"[yellow]DEGRADED[/yellow]" if n.status == "degraded" else f"[red]DEAD[/red]")
        if n.status == "healthy":
            healthy += 1
        elif n.status == "degraded":
            degraded += 1
        else:
            dead += 1

        table.add_row(
            n.name,
            n.region,
            n.egress_type,
            f"{n.host}:{n.port}",
            status_str,
            n.exit_ip or "-",
            f"{n.latency_ms}ms" if n.latency_ms else "-"
        )

    console.print(table)
    console.print(f"\n[bold green]Summary: Total {len(nodes)} | Healthy: {healthy} | Degraded: {degraded} | Dead: {dead}[/bold green]\n")


if __name__ == "__main__":
    asyncio.run(main())
