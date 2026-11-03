"""
tests/test_clash_importer.py - Unit tests for Clash / 3X-UI proxy importer & Mihomo listeners bridge
"""

import os
import tempfile
import yaml
import pytest
from core.models import EgressType, HealthStatus
from core.clash_importer import (
    detect_region,
    sanitize_slug,
    ClashImporter,
    DEFAULT_BASE_PORT,
)


def test_detect_region_by_flag():
    assert detect_region("🇭🇰 [自建] 香港 Azure") == "hongkong"
    assert detect_region("🇯🇵 [自建] 日本 AWS 东京") == "tokyo"
    assert detect_region("🇫🇷 [自建] 法国 OVH") == "france"
    assert detect_region("🇮🇹 [自建] 意大利 Botkeep") == "italy"
    assert detect_region("🇩🇪 德国 01") == "germany"
    assert detect_region("🇨🇳 [自建] 上海华为云") == "china"
    assert detect_region("🇹🇼 台湾 Hinet") == "taiwan"
    assert detect_region("🇷🇺 俄罗斯 JustHost") == "russia"
    assert detect_region("🇦🇺 悉尼 Australia") == "australia"
    assert detect_region("🇻🇳 越南 Hanoi") == "vietnam"
    assert detect_region("🇨🇭 瑞士 BuyVM") == "switzerland"


def test_detect_region_by_keyword_and_server():
    assert detect_region("vless-india-reality", "in-mumbai.azure.com") == "india"
    assert detect_region("node-01", "us-nyc.example.com") == "us-west"
    assert detect_region("random-unknown-node") == "global"


def test_sanitize_slug():
    slug = sanitize_slug("[自建] 🇭🇰 Azure 香港 Reality")
    assert "自建" not in slug
    assert "🇭🇰" not in slug
    assert slug == "azure-reality"

    slug2 = sanitize_slug("🇺🇸 US-West (Silicon Valley) #123")
    assert slug2 == "us-west-silicon-valley-123"


SAMPLE_CLASH_YAML = """
port: 7890
mode: rule
proxies:
  - name: "🇭🇰 [自建] HK-Azure-VLESS"
    type: vless
    server: hk.example.com
    port: 443
    uuid: 11111111-2222-3333-4444-555555555555
    network: tcp
    tls: true
  - name: "🇯🇵 [自建] JP-AWS-TUIC"
    type: tuic
    server: jp.example.com
    port: 8443
    uuid: 22222222-3333-4444-5555-666666666666
    password: secretpassword
  - name: "🇫🇷 FR-OVH-Hysteria2"
    type: hysteria2
    server: fr.example.com
    port: 443
    auth: hypassword
"""


def test_clash_importer_parse_and_build_nodes():
    with tempfile.NamedTemporaryFile("w+", suffix=".yaml", delete=False) as f:
        f.write(SAMPLE_CLASH_YAML)
        profile_path = f.name

    try:
        importer = ClashImporter(profile_path=profile_path, base_port=12000)
        proxies = importer.parse_profile()
        assert len(proxies) == 3

        nodes = importer.build_tether_nodes(proxies)
        assert len(nodes) == 3

        # Node 1: HK Azure
        assert nodes[0].name.startswith("clash-001-")
        assert nodes[0].port == 12000
        assert nodes[0].host == "127.0.0.1"
        assert nodes[0].protocol == "socks5h"
        assert nodes[0].region == "hongkong"
        assert nodes[0].egress_type == EgressType.CLASH_PROXY.value
        assert "self_hosted" in nodes[0].tags
        assert "vless" in nodes[0].tags

        # Node 2: JP TUIC
        assert nodes[1].name.startswith("clash-002-")
        assert nodes[1].port == 12001
        assert nodes[1].region == "tokyo"
        assert "tuic" in nodes[1].tags

        # Node 3: FR Hysteria2
        assert nodes[2].name.startswith("clash-003-")
        assert nodes[2].port == 12002
        assert nodes[2].region == "france"
        assert "hysteria2" in nodes[2].tags
    finally:
        os.remove(profile_path)


def test_clash_importer_generate_and_apply_merge_listeners():
    with tempfile.NamedTemporaryFile("w+", suffix=".yaml", delete=False) as f_prof, \
         tempfile.NamedTemporaryFile("w+", suffix=".yaml", delete=False) as f_merge:
        f_prof.write(SAMPLE_CLASH_YAML)
        prof_path = f_prof.name
        f_merge.write("profile:\n  store-selected: true\n")
        merge_path = f_merge.name

    try:
        importer = ClashImporter(
            profile_path=prof_path,
            merge_path=merge_path,
            base_port=13000
        )
        listeners = importer.generate_merge_listeners()
        assert len(listeners) == 3
        assert listeners[0]["port"] == 13000
        assert listeners[0]["type"] == "socks"
        assert listeners[0]["proxy"] == "🇭🇰 [自建] HK-Azure-VLESS"
        assert listeners[1]["port"] == 13001
        assert listeners[2]["port"] == 13002

        success = importer.apply_to_merge_yaml(listeners)
        assert success is True

        with open(merge_path, "r", encoding="utf-8") as f:
            updated_merge = yaml.safe_load(f)

        assert "profile" in updated_merge
        assert "listeners" in updated_merge
        assert len(updated_merge["listeners"]) == 3
        assert updated_merge["listeners"][0]["port"] == 13000
    finally:
        os.remove(prof_path)
        os.remove(merge_path)


def test_clash_importer_merge_into_nodes_yaml():
    with tempfile.NamedTemporaryFile("w+", suffix=".yaml", delete=False) as f_nodes, \
         tempfile.NamedTemporaryFile("w+", suffix=".yaml", delete=False) as f_prof:
        initial_config = {
            "version": "1.0",
            "nodes": [
                {
                    "name": "easytier-node-001",
                    "host": "10.144.144.1",
                    "port": 1080,
                    "protocol": "socks5h",
                    "region": "hongkong",
                    "egress_type": "warp_ipv6",
                    "tags": ["warp"]
                },
                {
                    "name": "clash-old-dummy",
                    "host": "127.0.0.1",
                    "port": 9999,
                    "protocol": "socks5h",
                    "region": "tokyo",
                    "egress_type": "clash_proxy",
                    "tags": ["clash"]
                }
            ]
        }
        yaml.dump(initial_config, f_nodes)
        nodes_path = f_nodes.name
        f_prof.write(SAMPLE_CLASH_YAML)
        prof_path = f_prof.name

    try:
        importer = ClashImporter(profile_path=prof_path, base_port=14000)
        new_nodes = importer.build_tether_nodes()
        total_count = importer.merge_into_nodes_yaml(nodes_path, new_nodes)

        # 1 original easytier node + 3 new clash nodes = 4 (clash-old-dummy was replaced)
        assert total_count == 4

        with open(nodes_path, "r", encoding="utf-8") as f:
            final_cfg = yaml.safe_load(f)

        nodes = final_cfg["nodes"]
        assert len(nodes) == 4
        assert nodes[0]["name"] == "easytier-node-001"
        assert not any(n["name"] == "clash-old-dummy" for n in nodes)
        assert any(n["name"].startswith("clash-001-") for n in nodes)
    finally:
        os.remove(nodes_path)
        os.remove(prof_path)
