"""
core/prober.py - Asynchronous dual-stack health prober with Circuit Breaker Integration
"""

import asyncio
import time
import httpx
from typing import List, Optional, Any
from core.models import ProxyNode, ProbeResult, EgressType, HealthStatus
from core.storage import Storage
from core.circuit import CircuitBreaker

DEFAULT_TIMEOUT = 12.0
PROBE_URL_V4 = "https://api.ipify.org"
PROBE_URL_CF_TRACE = "https://cloudflare.com/cdn-cgi/trace"


def parse_cf_trace(text: str) -> dict:
    data = {}
    for line in text.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            data[k.strip()] = v.strip()
    return data


class Prober:
    def __init__(self, storage: Storage, router: Optional[Any] = None, timeout: float = DEFAULT_TIMEOUT):
        self.storage = storage
        self.router = router
        self.timeout = timeout

    async def probe_node(self, node: ProxyNode, retries: int = 1, force: bool = False) -> ProbeResult:
        # Check circuit breaker cooldown if node is DEAD
        is_canary = node.status == HealthStatus.HALF_OPEN.value
        if not force and node.status == HealthStatus.DEAD.value:
            if not CircuitBreaker.is_cooldown_expired(node):
                # Cooldown still active, skip probing to respect isolation
                return ProbeResult(
                    node_name=node.name,
                    success=False,
                    latency_ms=0.0,
                    error="circuit_open_in_cooldown"
                )
            else:
                # Cooldown expired, mark as HALF_OPEN for trial canary probe.
                # A canary is a single-trial probe by definition (the circuit
                # reopens on failure); burning a full retry (~timeout+0.5s
                # more per canary per sweep) adds no information.
                is_canary = True
                node.status = HealthStatus.HALF_OPEN.value
                self.storage.update_node_status(node.name, HealthStatus.HALF_OPEN.value)
                self.storage.record_circuit_event(
                    node_name=node.name,
                    from_state=HealthStatus.DEAD.value,
                    to_state=HealthStatus.HALF_OPEN.value,
                    reason="cooldown_expired_canary_probe"
                )

        proxy_url = f"socks5h://{node.host}:{node.port}"

        # Choose primary probe URL
        if node.egress_type == EgressType.NATIVE_IPV4.value:
            target_url = PROBE_URL_V4
            is_cf_trace = False
        else:
            target_url = PROBE_URL_CF_TRACE
            is_cf_trace = True

        for attempt in range(retries + 1):
            start_t = time.perf_counter()
            try:
                async def _req():
                    async with httpx.AsyncClient(proxy=proxy_url, timeout=self.timeout) as client:
                        return await client.get(target_url)

                resp = await asyncio.wait_for(_req(), timeout=self.timeout + 1.0)
                latency = round((time.perf_counter() - start_t) * 1000, 1)

                if resp.status_code == 200:
                    text = resp.text.strip()
                    if is_cf_trace:
                        parsed = parse_cf_trace(text)
                        exit_ip = parsed.get("ip", "")
                        if node.egress_type == EgressType.WARP_IPV6.value:
                            if not exit_ip or ":" not in exit_ip:
                                raise ValueError(f"Expected IPv6 in WARP trace, got: {exit_ip}")
                        else:
                            if not exit_ip:
                                raise ValueError(f"Expected valid IP in trace, got: {exit_ip}")
                    else:
                        exit_ip = text

                    res = ProbeResult(
                        node_name=node.name,
                        success=True,
                        exit_ip=exit_ip,
                        latency_ms=latency
                    )
                    _, event = self.storage.update_probe_result(res)
                    return res
                else:
                    raise RuntimeError(f"HTTP status {resp.status_code}")

            except Exception as e:
                latency = round((time.perf_counter() - start_t) * 1000, 1)
                err_msg = str(e).strip()
                if not err_msg or isinstance(e, asyncio.TimeoutError):
                    err_msg = f"Timeout (>{self.timeout}s)"

                if attempt < retries and not is_canary:
                    await asyncio.sleep(0.5)
                    continue

                res = ProbeResult(
                    node_name=node.name,
                    success=False,
                    latency_ms=latency,
                    error=err_msg[:120]
                )
                _, event = self.storage.update_probe_result(res)

                # Automatic Self-Healing: If circuit tripped OPEN (DEAD), rehome attached leases
                if event and event.to_state == HealthStatus.DEAD.value and self.router:
                    try:
                        rehome_res = self.router.rehome_leases_for_node(node.name)
                        if rehome_res.get("rehomed_count", 0) > 0:
                            print(f"[Self-Healing] Node {node.name} tripped OPEN. Rehomed {rehome_res['rehomed_count']} leases.")
                    except Exception as rehome_err:
                        print(f"[-] Error rehoming leases for {node.name}: {rehome_err}")

                return res

    async def probe_all(self, concurrency: int = 25, force: bool = False) -> List[ProbeResult]:
        nodes = self.storage.list_nodes()
        sem = asyncio.Semaphore(concurrency)

        async def _worker(node: ProxyNode) -> ProbeResult:
            async with sem:
                try:
                    return await self.probe_node(node, retries=1, force=force)
                except Exception as exc:
                    return ProbeResult(
                        node_name=node.name,
                        success=False,
                        error=f"worker_exception: {exc}"[:120]
                    )

        tasks = [_worker(node) for node in nodes]
        results = await asyncio.gather(*tasks)
        return results

    async def probe_target_service(self, node: ProxyNode, target_url: str) -> ProbeResult:
        """
        Target-specific business probe (e.g. testing Cloudflare Turnstile, X signup, or OpenAI Auth).
        Differentiates between general network reachability and domain-level anti-bot blockages.
        """
        proxy_url = f"socks5h://{node.host}:{node.port}"
        start_t = time.perf_counter()
        try:
            async with httpx.AsyncClient(proxy=proxy_url, timeout=self.timeout, follow_redirects=True) as client:
                resp = await client.get(
                    target_url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
                    }
                )
                latency = round((time.perf_counter() - start_t) * 1000, 1)

                # Check for Cloudflare / Anti-Bot blockages
                if resp.status_code in (403, 503) and ("challenge-running" in resp.text or "Just a moment" in resp.text or "cf-mitigated" in resp.headers):
                    return ProbeResult(
                        node_name=node.name,
                        success=False,
                        latency_ms=latency,
                        error=f"Blocked by Cloudflare/WAF (HTTP {resp.status_code})"
                    )
                if resp.status_code >= 400 and resp.status_code not in (404, 401):
                    return ProbeResult(
                        node_name=node.name,
                        success=False,
                        latency_ms=latency,
                        error=f"Target returned HTTP {resp.status_code}"
                    )
                return ProbeResult(
                    node_name=node.name,
                    success=True,
                    exit_ip=node.exit_ip,
                    latency_ms=latency
                )
        except Exception as e:
            latency = round((time.perf_counter() - start_t) * 1000, 1)
            return ProbeResult(
                node_name=node.name,
                success=False,
                latency_ms=latency,
                error=str(e)[:120]
            )

