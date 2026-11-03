"""
adapters/playwright_pool.py - Playwright & Web Scraper Cluster Proxy Manager
Provides one-line proxy injection for Playwright Python, httpx, and scraping bots.
"""

import os
import sys
import uuid
from typing import Optional, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.storage import Storage
from core.router import Router
from core.models import EgressType


class PlaywrightProxyManager:
    """
    Adapter for headless browser automation (Playwright, Puppeteer, Selenium, httpx).
    """

    def __init__(self, storage: Optional[Storage] = None, router: Optional[Router] = None):
        self.storage = storage or Storage()
        self.router = router or Router(self.storage)

    def get_playwright_proxy(
        self,
        account_id: Optional[str] = None,
        region: Optional[str] = None,
        egress_type: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Returns a dictionary formatted for Playwright `browser.new_context(proxy=...)`.
        Example return: {'server': 'socks5://10.144.144.3:10881'}
        """
        acc_id = account_id or f"bot_{uuid.uuid4().hex[:8]}"
        lease = self.router.acquire_for_account(
            account_id=acc_id,
            region=region,
            egress_type=egress_type or EgressType.WARP_IPV6.value
        )
        if not lease:
            raise RuntimeError("No available proxy found in IP Pool for Playwright context")

        # Playwright supports 'socks5://host:port'
        node = self.storage.get_node(lease.node_name)
        return {
            "server": f"socks5://{node.host}:{node.port}",
            "username": "",
            "password": ""
        }

    def get_unified_gateway_proxy(
        self,
        account_id: Optional[str] = None,
        region: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Returns unified gateway (:9101) configuration for Playwright.
        """
        acc_id = account_id or f"bot_{uuid.uuid4().hex[:8]}"
        extra_headers = {"X-Account-Id": acc_id}
        if region:
            extra_headers["X-Region"] = region

        return {
            "proxy": {"server": "http://127.0.0.1:9101"},
            "extra_http_headers": extra_headers
        }
