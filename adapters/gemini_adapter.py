"""
adapters/gemini_adapter.py - Gemini Web Reverse Proxy Egress Adapter
Enforces regional affinity (Tokyo, Singapore, US-West) and session persistence for Gemini API/Web.
"""

import os
import sys
from typing import Optional, Dict, Any
from urllib.parse import quote

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.storage import Storage
from core.router import Router
from core.models import EgressType

GEMINI_PREFERRED_REGIONS = ["tokyo", "singapore", "us-west", "netherlands"]


class GeminiAdapter:
    def __init__(self, storage: Optional[Storage] = None, router: Optional[Router] = None):
        self.storage = storage or Storage()
        self.router = router or Router(self.storage)

    def acquire_proxy_for_account(
        self,
        account_id: str,
        preferred_region: Optional[str] = "tokyo",
        force_rebind: bool = False
    ) -> Dict[str, Any]:
        """
        Acquires a sticky proxy for a Gemini Web account with regional fallback chain.
        """
        regions_to_try = [preferred_region] if preferred_region else []
        for r in GEMINI_PREFERRED_REGIONS:
            if r not in regions_to_try:
                regions_to_try.append(r)

        lease = None
        for r in regions_to_try:
            lease = self.router.acquire_for_account(
                account_id=account_id,
                region=r,
                egress_type=EgressType.WARP_IPV6.value,
                force_rebind=force_rebind
            )
            if lease:
                break

        if not lease:
            # Fallback to any available node
            lease = self.router.acquire_for_account(account_id=account_id, force_rebind=force_rebind)

        if not lease:
            raise RuntimeError(f"No available egress node found for Gemini account {account_id}")

        return {
            "account_id": lease.account_id,
            "node_name": lease.node_name,
            "proxy_url": lease.proxy_url,
            "region": lease.region,
            "exit_ip": lease.exit_ip or "",
            "egress_type": lease.egress_type
        }

    def get_unified_gateway_config(self, account_id: str, preferred_region: str = "tokyo") -> Dict[str, Any]:
        """
        Returns a ready-to-use local unified gateway (:9101) proxy config.

        Credentials are exposed as proxy username/password because routing
        headers are never sent inside an HTTPS CONNECT tunnel; only
        Proxy-Authorization is, and the gateway decodes it into routing tags.
        """
        username = (
            f"{quote(account_id, safe='')}"
            f"__type_{EgressType.WARP_IPV6.value}"
            f"__region_{preferred_region}"
        )
        return {
            "gateway_url": "http://127.0.0.1:9101",
            "proxy": {
                "server": "http://127.0.0.1:9101",
                "username": username,
                "password": "tetherpool",
            },
        }
