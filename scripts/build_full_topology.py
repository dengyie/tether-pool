#!/usr/bin/env python3
"""
build_full_topology.py - Generates the complete 479-node topology for TetherPool / IP-Pool.

Nodes:
1. Native dedicated exits (8 nodes, tier: premium)
2. Tokyo WARP IPv6 (40 nodes, 10881~10920, tier: standard)
3. Singapore WARP IPv6 (80 nodes, 10881~10960, tier: standard)
4. India WARP IPv6 (50 nodes, 10881~10930, tier: standard)
5. Netherlands WARP IPv6 (150 nodes, 10881~11030, tier: standard)
6. Cue Sandbox US WARP IPv6 (100 nodes, 10881~10980, tier: standard)
7. Preserved Clash / 3X-UI bridge proxies (51 nodes, tier: standard)

Total: 8 + 40 + 80 + 50 + 150 + 100 + 51 = 479 nodes.
"""

import os
import yaml

OUTPUT_PATH = "config/nodes.yaml"

def generate_topology():
    with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
        existing = yaml.safe_load(f)

    settings = existing.get("settings", {
        "probe_interval_seconds": 300,
        "probe_timeout_seconds": 8,
        "probe_target_v4": "https://api.ipify.org",
        "probe_target_v6": "https://api64.ipify.org",
        "service_probe_url": "https://x.com",
    })

    # Preserve clash nodes
    clash_nodes = [n for n in existing.get("nodes", []) if n.get("host") == "127.0.0.1"]

    nodes = []

    # 1. Native Exits (8 nodes, Premium)
    native_specs = [
        {"name": "aws-tokyo-native-v4", "host": "10.144.144.3", "port": 10801, "region": "tokyo", "type": "native_ipv4", "tags": ["self_hosted", "native", "aws", "tokyo"]},
        {"name": "aws-tokyo-native-v6", "host": "10.144.144.3", "port": 10802, "region": "tokyo", "type": "native_ipv6", "tags": ["self_hosted", "native", "aws", "tokyo"]},
        {"name": "huawei-sg-native-v4", "host": "10.144.144.13", "port": 10801, "region": "singapore", "type": "native_ipv4", "tags": ["self_hosted", "native", "huawei", "singapore"]},
        {"name": "leaseweb-nl-native-v4", "host": "10.144.144.12", "port": 10801, "region": "netherlands", "type": "native_ipv4", "tags": ["self_hosted", "native", "leaseweb", "netherlands"]},
        {"name": "leaseweb-nl-native-v6", "host": "10.144.144.12", "port": 10802, "region": "netherlands", "type": "native_ipv6", "tags": ["self_hosted", "native", "leaseweb", "netherlands"]},
        {"name": "azure-india-native-v4", "host": "10.144.144.5", "port": 10801, "region": "india", "type": "native_ipv4", "tags": ["self_hosted", "native", "azure", "india"]},
        {"name": "azure-hk-native-v4", "host": "10.144.144.2", "port": 10801, "region": "hongkong", "type": "native_ipv4", "tags": ["self_hosted", "native", "azure", "hongkong"]},
        {"name": "gcp-uswest-native-v4", "host": "10.144.144.4", "port": 10801, "region": "us-west", "type": "native_ipv4", "tags": ["self_hosted", "native", "gcp", "us-west"]},
    ]
    for spec in native_specs:
        nodes.append({
            "name": spec["name"],
            "host": spec["host"],
            "port": spec["port"],
            "protocol": "socks5h",
            "region": spec["region"],
            "egress_type": spec["type"],
            "tags": spec["tags"],
            "weight": 100,
            "tier": "premium",
        })

    # 2. WARP IPv6 Pools (Standard)
    warp_groups = [
        {
            "prefix": "aws-tokyo-warp",
            "host": "10.144.144.3",
            "region": "tokyo",
            "count": 40,
            "tags": ["warp", "ipv6", "tokyo"],
        },
        {
            "prefix": "huawei-sg-warp",
            "host": "10.144.144.13",
            "region": "singapore",
            "count": 80,
            "tags": ["warp", "ipv6", "singapore"],
        },
        {
            "prefix": "azure-india-warp",
            "host": "10.144.144.5",
            "region": "india",
            "count": 50,
            "tags": ["warp", "ipv6", "india"],
        },
        {
            "prefix": "leaseweb-nl-warp",
            "host": "10.144.144.12",
            "region": "netherlands",
            "count": 150,
            "tags": ["warp", "ipv6", "netherlands"],
        },
        {
            "prefix": "cue-us-warp",
            "host": "10.144.144.81",
            "region": "us-central",
            "count": 100,
            "tags": ["warp", "ipv6", "us-central"],
        },
    ]

    for grp in warp_groups:
        for i in range(1, grp["count"] + 1):
            port = 10880 + i
            if port >= 11010:
                # Avoid conflict with EasyTier mesh listener on 11010
                port += 1
            nodes.append({
                "name": f"{grp['prefix']}-{i:02d}" if grp["count"] < 100 else f"{grp['prefix']}-{i:03d}",
                "host": grp["host"],
                "port": port,
                "protocol": "socks5h",
                "region": grp["region"],
                "egress_type": "warp_ipv6",
                "tags": list(grp["tags"]),
                "weight": 100,
                "tier": "standard",
            })

    # 3. Append Clash Nodes (51)
    for cn in clash_nodes:
        if "tier" not in cn:
            cn["tier"] = "standard"
        nodes.append(cn)

    full_doc = {
        "version": "1.0",
        "settings": settings,
        "nodes": nodes,
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        yaml.dump(full_doc, f, allow_unicode=True, sort_keys=False, indent=2)

    print(f"Generated {len(nodes)} nodes in {OUTPUT_PATH}:")
    print(f"  - Native Premium: {len(native_specs)}")
    print(f"  - WARP Standard : {sum(g['count'] for g in warp_groups)}")
    print(f"  - Clash Standard: {len(clash_nodes)}")
    print(f"  - Total         : {len(nodes)}")

if __name__ == "__main__":
    generate_topology()
