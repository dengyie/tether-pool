"""
adapters/tetherpool_client.py - Official TetherPool Integration Client SDK
Zero-dependency, production-ready Python client for external projects (grok2api, cpa-manager-plus, gemini2api).
Supports batch account allocation, zero-config parametric gateway URLs, and battery health reporting.
"""

import json
import urllib.request
import urllib.error
from urllib.parse import urlparse, urlencode, quote
from typing import List, Dict, Optional, Any


class TetherPoolClient:
    """
    Standard TetherPool Integration Client.
    Usage Example:
        client = TetherPoolClient()
        # 1. Batch allocate 2,100 accounts evenly across IPv6 pool
        res = client.batch_allocate(account_ids, service="grok2api", egress_preference="warp_ipv6")

        # 2. On-demand proxy for an individual account
        proxy = client.get_proxy_for_account("user@corp.com", service="grok2api")

        # 3. Transparent Gateway URL (zero API calls)
        gw_url = client.get_gateway_proxy_url("user@corp.com", egress_type="warp_ipv6", region="tokyo")

        # 4. Report 429 rate limit to trigger auto battery hot-swap
        client.report_battery_error("user@corp.com", error_type="rate_limited_429", service="grok2api")
    """

    def __init__(
        self,
        api_base: str = "http://127.0.0.1:9100",
        gateway_base: str = "http://127.0.0.1:9101",
        timeout: float = 15.0
    ):
        self.api_base = api_base.rstrip("/")
        self.gateway_base = gateway_base.rstrip("/")
        self.timeout = timeout

    def _http_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        url = f"{self.api_base}{path}"
        if params:
            url += f"?{urlencode(params)}"

        headers = {"Accept": "application/json"}
        req_body = None
        if data is not None:
            headers["Content-Type"] = "application/json"
            req_body = json.dumps(data).encode("utf-8")

        req = urllib.request.Request(url, data=req_body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                resp_data = resp.read().decode("utf-8")
                return json.loads(resp_data) if resp_data else {}
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8")
            try:
                detail = json.loads(err_body).get("detail", err_body)
            except Exception:
                detail = err_body
            raise RuntimeError(f"TetherPool API Error [{e.code}]: {detail}") from e
        except Exception as e:
            raise RuntimeError(f"Failed to connect to TetherPool API at {url}: {e}") from e

    # --- Allocation & Egress Management ---

    def batch_allocate(
        self,
        accounts: List[str],
        service: str = "default",
        egress_preference: str = "warp_ipv6",
        region: Optional[str] = None,
        max_accounts_per_ip: int = 50,
        force_rebalance: bool = False,
        strategy: str = "balanced_round_robin"
    ) -> Dict[str, Any]:
        """
        Batch-allocates accounts across available IP exits with minimal variance.
        """
        payload = {
            "service": service,
            "accounts": accounts,
            "strategy": strategy,
            "egress_preference": egress_preference,
            "region": region,
            "max_accounts_per_ip": max_accounts_per_ip,
            "force_rebalance": force_rebalance
        }
        return self._http_request("POST", "/v1/allocation/batch", data=payload)

    def get_proxy_for_account(
        self,
        account_id: str,
        service: str = "default",
        egress_preference: str = "warp_ipv6",
        region: Optional[str] = None,
        force_rebalance: bool = False
    ) -> Dict[str, Any]:
        """
        Retrieves or allocates a dedicated sticky proxy for a single account.
        """
        params = {
            "service": service,
            "account_id": account_id,
            "egress_preference": egress_preference,
            "force_rebalance": force_rebalance
        }
        if region:
            params["region"] = region
        return self._http_request("POST", "/v1/allocation/acquire", params=params)

    def release_account(self, account_id: str, service: str = "default") -> bool:
        """
        Releases an account's IP allocation.
        """
        res = self._http_request("POST", "/v1/allocation/release", params={"service": service, "account_id": account_id})
        return res.get("success", False)

    def get_status(self) -> Dict[str, Any]:
        """
        Retrieves cluster-wide allocation metrics and load per node.
        """
        return self._http_request("GET", "/v1/allocation/status")

    def list_service_accounts(self, service: str) -> List[Dict[str, Any]]:
        """
        Lists all assigned accounts for a given service.
        """
        return self._http_request("GET", "/v1/allocation/accounts", params={"service": service})

    # --- Zero-Config Gateway URL Generation ---

    def get_gateway_proxy_url(
        self,
        account_id: str,
        egress_type: Optional[str] = "warp_ipv6",
        region: Optional[str] = None,
        service: Optional[str] = None
    ) -> str:
        """
        Generates a transparent gateway proxy URL with parametric authentication.
        Downstream services can use this single URL in requests/httpx/Playwright without API pre-registration!
        Example: http://user_001__type_warp_ipv6__region_tokyo__service_grok2api:tetherpool@127.0.0.1:9101
        """
        parsed = urlparse(self.gateway_base)
        netloc = parsed.netloc or f"{parsed.hostname or '127.0.0.1'}:{parsed.port or 9101}"
        scheme = parsed.scheme or "http"

        parts = [quote(account_id, safe="")]
        if egress_type:
            parts.append(f"type_{egress_type}")
        if region:
            parts.append(f"region_{region}")
        if service and service != "default":
            parts.append(f"service_{service}")
        auth_user = "__".join(parts)

        return f"{scheme}://{auth_user}:tetherpool@{netloc}"

    # --- Battery Lifecycle & Self-Healing ---

    def report_battery_error(
        self,
        account_id: str,
        error_type: str = "rate_limited_429",
        service: Optional[str] = None,
        node_name: Optional[str] = None,
        force_rebind: bool = True
    ) -> Dict[str, Any]:
        """
        Reports upstream rate-limiting (429) or ban (401) to trigger automatic hot-swapping
        to a fresh healthy node in the same region.
        """
        payload = {
            "account_id": account_id,
            "node_name": node_name,
            "event_type": error_type,
            "target_service": service,
            "force_rebind": force_rebind
        }
        return self._http_request("POST", "/v1/battery/event", data=payload)
