"""
core/prober.py - Asynchronous dual-stack health prober for IP Pool
"""

import asyncio
import time
import httpx
from typing import List, Optional
from core.models import ProxyNode, ProbeResult, EgressType
from core.storage import Storage

DEFAULT_TIMEOUT = 12.0
PROBE_URL_V4 = "https://api.ipify.org"
PROBE_URL_CF_TRACE = "https://cloudflare.com/cdn-cgi/trace"
PROBE_URL_V6 = "https://api64.ipify.org"


def parse_cf_trace(text: str) -> dict:
    data = {}
    for line in text.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            data[k.strip()] = v.strip()
    return data


class Prober:
    def __init__(self, storage: Storage, timeout: float = DEFAULT_TIMEOUT):
        self.storage = storage
        self.timeout = timeout

    async def probe_node(self, node: ProxyNode, retries: int = 1) -> ProbeResult:
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
                        if not exit_ip or ":" not in exit_ip:
                            raise ValueError(f"Expected IPv6 in trace, got: {exit_ip}")
                    else:
                        exit_ip = text

                    res = ProbeResult(
                        node_name=node.name,
                        success=True,
                        exit_ip=exit_ip,
                        latency_ms=latency
                    )
                    self.storage.update_probe_result(res)
                    return res
                else:
                    raise RuntimeError(f"HTTP status {resp.status_code}")

            except Exception as e:
                latency = round((time.perf_counter() - start_t) * 1000, 1)
                err_msg = str(e).strip()
                if not err_msg or isinstance(e, asyncio.TimeoutError):
                    err_msg = f"Timeout (>{self.timeout}s)"

                if attempt < retries:
                    await asyncio.sleep(0.5)
                    continue

                res = ProbeResult(
                    node_name=node.name,
                    success=False,
                    latency_ms=latency,
                    error=err_msg[:120]
                )
                self.storage.update_probe_result(res)
                return res

    async def probe_all(self, concurrency: int = 15) -> List[ProbeResult]:
        nodes = self.storage.list_nodes()
        sem = asyncio.Semaphore(concurrency)

        async def _worker(node: ProxyNode) -> ProbeResult:
            async with sem:
                return await self.probe_node(node)

        tasks = [_worker(node) for node in nodes]
        results = await asyncio.gather(*tasks)
        return results

