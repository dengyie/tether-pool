"""
core/clash_importer.py - Importer & Mihomo Bridge Generator for Clash / 3X-UI Proxies

Extracts proxies from Clash YAML profiles (e.g., mango-clash from 3x-ui.mangoqwq.com),
maps them to local SOCKS5 bridge listeners in Mihomo Merge.yaml (127.0.0.1:11001+),
and generates TetherPool-compliant ProxyNode definitions for multi-region routing.
"""

from __future__ import annotations

import os
import re
import yaml
import json
import urllib.request
import urllib.error
from typing import List, Dict, Any, Optional, Tuple

from core.models import ProxyNode, EgressType, HealthStatus

DEFAULT_CLASH_VERGE_RUNTIME = os.path.expanduser(
    "~/Library/Application Support/io.github.clash-verge-rev.clash-verge-rev/clash-verge.yaml"
)
DEFAULT_RAW_PROFILE = os.path.expanduser(
    "~/Library/Application Support/io.github.clash-verge-rev.clash-verge-rev/profiles/RBqg7ki8l5g7.yaml"
)
DEFAULT_PROFILE_PATH = (
    DEFAULT_CLASH_VERGE_RUNTIME
    if os.path.exists(DEFAULT_CLASH_VERGE_RUNTIME)
    else DEFAULT_RAW_PROFILE
)
DEFAULT_MERGE_PATH = os.path.expanduser(
    "~/Library/Application Support/io.github.clash-verge-rev.clash-verge-rev/profiles/Merge.yaml"
)
DEFAULT_BASE_PORT = 11001
DEFAULT_HOST = "127.0.0.1"

# Emoji flag to region mapping
FLAG_TO_REGION = {
    "🇭🇰": "hongkong",
    "🇯🇵": "tokyo",
    "🇸🇬": "singapore",
    "🇺🇸": "us-west",
    "🇩🇪": "germany",
    "🇳🇱": "netherlands",
    "🇮🇳": "india",
    "🇫🇷": "france",
    "🇮🇹": "italy",
    "🇹🇼": "taiwan",
    "🇨🇳": "china",
    "🇷🇺": "russia",
    "🇦🇺": "australia",
    "🇬🇧": "united-kingdom",
    "🇰🇷": "korea",
    "🇻🇳": "vietnam",
    "🇨🇭": "switzerland",
}

# Keyword to region heuristic mapping
KEYWORD_TO_REGION = [
    (r"(?i)(hongkong|\bhk\b|hkg|香港)", "hongkong"),
    (r"(?i)(tokyo|japan|\bjp\b|osaka|日本)", "tokyo"),
    (r"(?i)(singapore|\bsg\b|新加坡)", "singapore"),
    (r"(?i)(\bus\b|united states|america|las|nyc|美国|googlevps)", "us-west"),
    (r"(?i)(germany|\bde\b|frankfurt|德国)", "germany"),
    (r"(?i)(netherlands|\bnl\b|amsterdam|kerkrade|deluxhost|荷兰)", "netherlands"),
    (r"(?i)(india|\bin\b|mumbai|chennai|印度)", "india"),
    (r"(?i)(france|\bfr\b|katabump|法国)", "france"),
    (r"(?i)(italy|\bit\b|botkeep|milan|意大利)", "italy"),
    (r"(?i)(taiwan|\btw\b|hinet|彰化|台湾)", "taiwan"),
    (r"(?i)(china|\bcn\b|shanghai|guangzhou|hangzhou|huawei-sh|tencent-sh|tencent-gz|tebi|pxed|home-win|上海|广州|杭州|家宽)", "china"),
    (r"(?i)(russia|\bru\b|novosibirsk|justhost|俄罗斯)", "russia"),
    (r"(?i)(australia|\bau\b|sydney|澳大利亚)", "australia"),
    (r"(?i)(britain|\buk\b|london|英国)", "united-kingdom"),
    (r"(?i)(korea|\bkr\b|seoul|韩国)", "korea"),
    (r"(?i)(vietnam|\bvn\b|hanoi|tothost|越南)", "vietnam"),
    (r"(?i)(switzerland|\bch\b|brn|buyvm-brn|瑞士)", "switzerland"),
]


def detect_region(name: str, server: str = "") -> str:
    """Detects region code from proxy display name, country flag, or server hostname."""
    if not name:
        return "global"

    # 1. Match country flag emojis
    for flag, region in FLAG_TO_REGION.items():
        if flag in name:
            return region

    # 2. Match text keywords in name
    for pattern, region in KEYWORD_TO_REGION:
        if re.search(pattern, name):
            return region

    # 3. Match server domain/hostname
    if server:
        for pattern, region in KEYWORD_TO_REGION:
            if re.search(pattern, server):
                return region

    return "global"


def sanitize_slug(name: str) -> str:
    """Produces a clean alphanumeric slug from a raw proxy display name."""
    # Strip bracketed self-built tags like [自建] or [share] or [tomorin]
    clean = re.sub(r"\[.*?\]", "", name)
    # Strip emoji flags and symbols
    clean = re.sub(r"[\U0001F1E6-\U0001F1FF]+", "", clean)
    clean = re.sub(r"[\U0001F300-\U0001FAFF]+", "", clean)
    clean = re.sub(r"[丨|/\\()_#@~`!$%^&*=+]", "-", clean)
    # Replace non-ascii and punctuation with hyphen
    clean = re.sub(r"[^a-zA-Z0-9\-]+", "-", clean)
    clean = re.sub(r"-+", "-", clean).strip("-").lower()
    return clean or "node"


class ClashImporter:
    """
    Parses Clash proxy definitions and coordinates Mihomo listeners bridge
    and TetherPool node registration.
    """

    def __init__(
        self,
        profile_path: str = DEFAULT_PROFILE_PATH,
        merge_path: str = DEFAULT_MERGE_PATH,
        base_port: int = DEFAULT_BASE_PORT,
        host: str = DEFAULT_HOST
    ):
        self.profile_path = profile_path
        self.merge_path = merge_path
        self.base_port = base_port
        self.host = host

    def parse_profile(self, custom_path: Optional[str] = None) -> List[Dict[str, Any]]:
        """Parses the Clash YAML file and returns the raw proxies list."""
        target_path = custom_path or self.profile_path
        if not os.path.exists(target_path):
            raise FileNotFoundError(f"Clash profile not found at: {target_path}")

        with open(target_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if not isinstance(data, dict) or "proxies" not in data:
            raise ValueError(f"Invalid Clash profile: missing 'proxies' key in {target_path}")

        proxies = data.get("proxies", [])
        return [p for p in proxies if isinstance(p, dict) and p.get("name")]

    def build_tether_nodes(
        self,
        proxies: Optional[List[Dict[str, Any]]] = None
    ) -> List[ProxyNode]:
        """
        Converts Clash proxies into TetherPool ProxyNode models with allocated local bridge ports.
        """
        if proxies is None:
            proxies = self.parse_profile()

        nodes: List[ProxyNode] = []
        used_names = set()

        for idx, p in enumerate(proxies):
            raw_name = str(p.get("name", "")).strip()
            server = str(p.get("server", "")).strip()
            proto = str(p.get("type", "unknown")).lower()
            region = detect_region(raw_name, server)

            # Generate unique node name
            slug = sanitize_slug(raw_name)
            node_name = f"clash-{idx+1:03d}-{slug}"
            if node_name in used_names:
                node_name = f"clash-{idx+1:03d}-{slug}-{idx+1}"
            used_names.add(node_name)

            allocated_port = self.base_port + idx

            tags = ["clash", "bridge", proto]
            if "[自建]" in raw_name:
                tags.append("self_hosted")
            if "[share]" in raw_name:
                tags.append("share")
            if "[tomorin]" in raw_name:
                tags.append("tomorin")

            node = ProxyNode(
                name=node_name,
                host=self.host,
                port=allocated_port,
                protocol="socks5h",
                region=region,
                egress_type=EgressType.CLASH_PROXY.value,
                tags=tags,
                weight=100,
                status=HealthStatus.UNPROBED.value
            )
            nodes.append(node)

        return nodes

    def generate_merge_listeners(
        self,
        proxies: Optional[List[Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        """
        Generates Mihomo `listeners:` configuration list for Clash Verge Merge.yaml.
        Each listener binds a local SOCKS5 port to a specific proxy outbound.
        """
        if proxies is None:
            proxies = self.parse_profile()

        listeners = []
        for idx, p in enumerate(proxies):
            proxy_name = p.get("name")
            port = self.base_port + idx
            listener = {
                "name": f"tether-socks-{idx+1:03d}",
                "type": "socks",
                "port": port,
                "listen": self.host,
                "proxy": proxy_name
            }
            listeners.append(listener)
        return listeners

    def apply_to_merge_yaml(
        self,
        listeners: Optional[List[Dict[str, Any]]] = None,
        custom_merge_path: Optional[str] = None
    ) -> bool:
        """
        Updates Merge.yaml by injecting or replacing the `listeners:` section.
        Preserves existing `profile:` and other configuration keys.
        """
        target_path = custom_merge_path or self.merge_path
        if listeners is None:
            listeners = self.generate_merge_listeners()

        existing_data: Dict[str, Any] = {}
        if os.path.exists(target_path):
            try:
                with open(target_path, "r", encoding="utf-8") as f:
                    content = yaml.safe_load(f)
                    if isinstance(content, dict):
                        existing_data = content
            except Exception:
                existing_data = {}

        # Set listeners
        existing_data["listeners"] = listeners

        # Atomic write back
        tmp_path = f"{target_path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            yaml.dump(existing_data, f, allow_unicode=True, sort_keys=False)
        os.replace(tmp_path, target_path)
        return True

    def merge_into_nodes_yaml(
        self,
        nodes_yaml_path: str,
        new_nodes: List[ProxyNode]
    ) -> int:
        """
        Merges new ProxyNodes into TetherPool's config/nodes.yaml.
        Removes any prior clash-prefixed nodes to avoid duplicate accumulation,
        then appends the new set of nodes. Returns total count after merge.
        """
        if not os.path.exists(nodes_yaml_path):
            raise FileNotFoundError(f"nodes.yaml not found at {nodes_yaml_path}")

        with open(nodes_yaml_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

        existing_nodes = config.get("nodes", [])

        # Filter out existing clash-prefixed nodes
        kept_nodes = [n for n in existing_nodes if not str(n.get("name", "")).startswith("clash-")]

        # Serialize new nodes to dict
        for n in new_nodes:
            kept_nodes.append({
                "name": n.name,
                "host": n.host,
                "port": n.port,
                "protocol": n.protocol,
                "region": n.region,
                "egress_type": n.egress_type,
                "tags": n.tags,
                "weight": n.weight
            })

        config["nodes"] = kept_nodes

        tmp_path = f"{nodes_yaml_path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f, allow_unicode=True, sort_keys=False)
        os.replace(tmp_path, nodes_yaml_path)

        return len(kept_nodes)

    @staticmethod
    def reload_clash_verge(
        controller_url: str = "http://127.0.0.1:9097",
        secret: str = "set-your-secret"
    ) -> bool:
        """Triggers hot-reload on the local running Mihomo controller."""
        req_url = f"{controller_url.rstrip('/')}/configs?force=true"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {secret}"
        }
        req = urllib.request.Request(req_url, data=b"{}", headers=headers, method="PUT")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status in (200, 204)
        except Exception:
            return False


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="TetherPool Clash / 3X-UI Importer & Mihomo Bridge")
    parser.add_argument("--profile", default=DEFAULT_PROFILE_PATH, help="Path to Clash profile YAML")
    parser.add_argument("--merge", default=DEFAULT_MERGE_PATH, help="Path to Merge.yaml")
    parser.add_argument("--nodes-yaml", default="config/nodes.yaml", help="Path to TetherPool nodes.yaml")
    parser.add_argument("--base-port", type=int, default=DEFAULT_BASE_PORT, help="Starting port for listeners")
    parser.add_argument("--apply", action="store_true", help="Apply listeners to Merge.yaml and update nodes.yaml")
    parser.add_argument("--reload", action="store_true", help="Trigger Mihomo controller hot-reload")

    args = parser.parse_args()

    importer = ClashImporter(
        profile_path=args.profile,
        merge_path=args.merge,
        base_port=args.base_port
    )

    try:
        proxies = importer.parse_profile()
        print(f"[*] Successfully loaded {len(proxies)} proxies from {args.profile}")
    except Exception as e:
        print(f"[!] Failed to parse profile: {e}", file=sys.stderr)
        sys.exit(1)

    nodes = importer.build_tether_nodes(proxies)
    listeners = importer.generate_merge_listeners(proxies)

    regions_count: Dict[str, int] = {}
    for n in nodes:
        regions_count[n.region] = regions_count.get(n.region, 0) + 1

    print("[*] Node Region Distribution:")
    for reg, cnt in sorted(regions_count.items(), key=lambda x: -x[1]):
        print(f"    - {reg:15s}: {cnt:3d} nodes")

    print(f"[*] Port allocation range: {args.base_port} ~ {args.base_port + len(nodes) - 1}")

    if args.apply:
        print(f"[*] Updating Merge.yaml at: {args.merge} ...")
        importer.apply_to_merge_yaml(listeners)
        print("[+] Merge.yaml listeners successfully injected.")

        print(f"[*] Merging into nodes.yaml at: {args.nodes_yaml} ...")
        total = importer.merge_into_nodes_yaml(args.nodes_yaml, nodes)
        print(f"[+] Total pool nodes in nodes.yaml is now: {total}")

        if args.reload:
            print("[*] Triggering Mihomo controller reload...")
            ok = importer.reload_clash_verge()
            if ok:
                print("[+] Mihomo reloaded successfully.")
            else:
                print("[!] Note: Mihomo hot-reload returned non-200 (Merge.yaml saved; restart or toggle Merge in Verge UI if needed).")
    else:
        print("[*] Dry run complete. Pass --apply to inject listeners and update nodes.yaml.")

