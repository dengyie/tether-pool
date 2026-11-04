"""
adapters/cpa_adapter.py - CPA (Claude / OpenAI Proxy) Configuration Exporter & Synchronizer
Generates proxy definitions and credential-level sticky routing for CPA.
"""

import os
import sys
import yaml
from typing import Dict, Any, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.storage import Storage
from core.models import HealthStatus


class CpaAdapter:
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage or Storage()

    def generate_cpa_config(
        self,
        region: Optional[str] = None,
        egress_type: Optional[str] = None,
        only_healthy: bool = True
    ) -> Dict[str, Any]:
        """
        Generates CPA-compatible proxy definitions.
        Format:
        proxies:
          - id: aws-tokyo-warp-01
            url: socks5://10.144.144.3:10881
            region: tokyo
            type: warp_ipv6
            weight: 100
        """
        status_filter = HealthStatus.HEALTHY.value if only_healthy else None
        nodes = self.storage.list_nodes(region=region, egress_type=egress_type, status=status_filter)
        if not nodes and only_healthy:
            nodes = self.storage.list_nodes(region=region, egress_type=egress_type)

        cpa_proxies = []
        for n in nodes:
            cpa_proxies.append({
                "id": n.name,
                "url": f"socks5://{n.host}:{n.port}",
                "region": n.region,
                "type": n.egress_type,
                "exit_ip": n.exit_ip or "",
                "weight": n.weight
            })

        return {
            "version": "1.0",
            "proxy_count": len(cpa_proxies),
            "proxies": cpa_proxies
        }

    def export_to_file(self, filepath: str, region: Optional[str] = None) -> str:
        """Exports CPA configuration to a target YAML file."""
        config = self.generate_cpa_config(region=region)
        os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            yaml.dump(config, f, allow_unicode=True, sort_keys=False)
        return filepath


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Export IP Pool nodes to CPA configuration format")
    parser.add_argument("--output", "-o", default="data/cpa_proxies.yaml", help="Output YAML file path")
    parser.add_argument("--region", "-r", default=None, help="Filter by region (tokyo, singapore, netherlands, india)")
    args = parser.parse_args()

    adapter = CpaAdapter()
    path = adapter.export_to_file(args.output, region=args.region)
    print(f"[+] CPA proxy configuration exported successfully to: {path}")


if __name__ == "__main__":
    main()
