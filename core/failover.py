"""
core/failover.py - Failure-domain isolation (R3) and fast host-unreachability detection (R4).

The 2026-11-07 incident had a single physical failure: `mesh-nl` went offline and took
its 25 WARP IPv6 listeners with it. The pool treated those 25 ports as 25 independent
nodes, so it produced 25 separate DEAD transitions, 25 rehome sweeps and 511 accounts
stuck on dead exits for over 20 seconds per request.

This module makes "one machine went away" a single, first-class event:

* `FailureDomainIsolator` collapses per-node signals into one domain-level decision, trips
  every listener in the domain in one storage sweep, and drains their bindings in one
  batch. Trip policy is configurable (`any` / `quorum` / `all`) because a single listener
  failing is not always a host failure — but the default is `any`, matching the incident.
* `EasytierPeerMonitor` turns `easytier-cli peer` output into sub-5s host availability.
  A peer disappearing from the mesh is authoritative and immediate: it bypasses SOCKS
  handshake timeouts entirely, which is what made detection take 15s+.

Both are deliberately storage-only: no asyncio task scheduling lives here, so they are
trivially unit-testable. `gateway/server.py` owns the background loop.
"""

import json
import os
import re
import subprocess
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from core.models import CircuitEvent, HealthStatus, ProxyNode
from core.storage import Storage

# How many nodes in a domain must show an unreachable signal before the whole domain is
# considered down. "any" matches observed reality: one listener failing on a host whose
# peer has vanished means the host is gone, not that one port is broken.
DEFAULT_DOMAIN_TRIP = os.environ.get("FAILURE_DOMAIN_TRIP", "any").strip().lower()
QUORUM_RATIO = float(os.environ.get("FAILURE_DOMAIN_QUORUM_RATIO", "0.5"))

# A domain stays "recovering" for this long after its trip so that the rehome path does
# not re-isolate listeners that are already known dead.
DOMAIN_COOLDOWN_SEC = float(os.environ.get("FAILURE_DOMAIN_COOLDOWN_SEC", "60"))


class FailureDomainIsolator:
    """
    Converts an unreachable signal on one listener into one domain-level isolation sweep.

    Thread-safety: isolation runs under BINDING_WRITE_LOCK because it rewrites account
    bindings; the lock is re-entrant so it can call into Router safely.
    """

    def __init__(
        self,
        storage: Storage,
        router: Optional[Any] = None,
        trip_policy: str = DEFAULT_DOMAIN_TRIP
    ):
        self.storage = storage
        self.router = router
        policy = (trip_policy or DEFAULT_DOMAIN_TRIP).strip().lower()
        if policy not in ("any", "quorum", "all"):
            policy = "any"
        self.trip_policy = policy
        # domain -> monotonic timestamp of last trip; prevents re-entrant sweeps within
        # the same failure while the first one is still draining bindings.
        self._recent_trips: Dict[str, float] = {}
        self._lock = threading.RLock()

    # -- policy ----------------------------------------------------------------

    def _should_trip(self, domain: str, unreachable_in_domain: int) -> bool:
        counts = self.storage.domain_status_counts(domain)
        total = sum(counts.values())
        if total == 0:
            return False
        if self.trip_policy == "all":
            return unreachable_in_domain >= total
        if self.trip_policy == "quorum":
            threshold = max(1, int(total * QUORUM_RATIO))
            # `domain_status_counts` is status-keyed; unreachable listeners may not be DEAD
            # yet, so compare against the whole domain population.
            return unreachable_in_domain >= threshold
        return unreachable_in_domain >= 1

    # -- public API ------------------------------------------------------------

    def report_unreachable(
        self,
        node_name: str,
        reason: str = "connect_unreachable",
        confirmed_dead: bool = True,
        whole_domain: bool = False,
    ) -> Dict[str, Any]:
        """
        Reports a *connection-layer* failure (not a target-site business error) for a node.

        `confirmed_dead` expresses whether this signal is authoritative on its own:

        * `True` — the source already knows the host is gone (EasyTier peer disappeared,
          or the node's own circuit breaker has already tripped DEAD). The domain trips
          immediately, subject to the configured policy.
        * `False` — a single request-time connect failure. One failed dial is not a host
          failure; it only counts once the node's circuit breaker has met its own failure
          threshold (R3: "达到阈值后"). The caller passes `confirmed_dead` after consulting
          the breaker outcome.

        `whole_domain` marks a signal that is authoritative for *every* listener (peer
        disappearance), so the `all`/`quorum` policies see the entire host as unreachable
        instead of waiting for each port to be persisted DEAD independently.

        Returns a dict describing what happened:
          {"action": "isolated"|"trip_pending"|"already_isolated"|"unknown_node",
           "domain": str, "affected_nodes": [...], "rehomed_count": int, "event": ...}
        """
        node = self.storage.get_node(node_name)
        if not node:
            return {"action": "unknown_node", "node_name": node_name}

        domain = node.effective_fault_domain
        with self._lock:
            peers = self.storage.list_nodes_by_domain(domain)
            # The reporting node may already be persisted DEAD (gateway path) or not yet
            # (a confirmed host-level signal). Count it exactly once either way — adding a
            # blind +1 double-counted it and tripped quorum/all domains one node early.
            unreachable_names = {
                p.name for p in peers if p.status == HealthStatus.DEAD.value
            }
            if whole_domain:
                unreachable_names |= {p.name for p in peers}
            else:
                unreachable_names.add(node_name)
            unreachable_count = len(unreachable_names)

            # A domain with a single listener has no blast radius to collapse; the normal
            # per-node circuit breaker is strictly better information there.
            if len(peers) <= 1:
                return {
                    "action": "single_listener_domain",
                    "domain": domain,
                    "affected_nodes": [node_name],
                    "rehomed_count": 0,
                    "event": None,
                }

            if not confirmed_dead:
                return {
                    "action": "trip_pending",
                    "domain": domain,
                    "reason": "single_failure_below_threshold",
                    "unreachable_in_domain": unreachable_count,
                    "trip_policy": self.trip_policy,
                    "affected_nodes": [],
                    "rehomed_count": 0,
                    "event": None,
                }

            if not self._should_trip(domain, unreachable_count):
                return {
                    "action": "trip_pending",
                    "domain": domain,
                    "unreachable_in_domain": unreachable_count,
                    "trip_policy": self.trip_policy,
                    "affected_nodes": [],
                    "rehomed_count": 0,
                    "event": None,
                }

            last_trip = self._recent_trips.get(domain)
            if last_trip is not None and (time.monotonic() - last_trip) < DOMAIN_COOLDOWN_SEC:
                return {
                    "action": "already_isolated",
                    "domain": domain,
                    "affected_nodes": [],
                    "rehomed_count": 0,
                    "event": None,
                }

            self._recent_trips[domain] = time.monotonic()

        # One bulk DEAD transition + ONE circuit event for the whole domain (R3).
        affected, event = self.storage.mark_domain_dead(domain, reason=reason)
        if not affected:
            return {
                "action": "already_isolated",
                "domain": domain,
                "affected_nodes": [],
                "rehomed_count": 0,
                "event": None,
            }

        self.storage.incr_metric(
            "ip_pool_domain_trip_total",
            {"domain": domain, "policy": self.trip_policy}
        )
        # NOTE: `ip_pool_host_unreachable_seconds` is owned by EasytierPeerMonitor, which
        # measures the duration. Resetting it here used to clobber the elapsed value with
        # 0.0 on every trip, making the gauge permanently useless.

        rehome_result: Dict[str, Any] = {"rehomed_count": 0, "details": []}
        if self.router is not None:
            rehome_result = self.router.rehome_leases_for_domain(domain, affected)

        return {
            "action": "isolated",
            "domain": domain,
            "trip_policy": self.trip_policy,
            "affected_nodes": [n.name for n in affected],
            "rehomed_count": rehome_result.get("rehomed_count", 0),
            "allocations_rehomed_count": rehome_result.get("allocations_rehomed_count", 0),
            "event": event,
            "rehome": rehome_result,
        }

    def note_success(self, node_name: str):
        """
        Clears a domain's cooldown marker when a listener answers again.

        Recovery is still per-node (each node walks its own HALF_OPEN canary), this only
        re-arms the isolator so a genuine second failure is not swallowed.
        """
        # Fast path: no trip is cooling down, so there is nothing to re-arm. This keeps the
        # per-request cost at zero on the healthy path (no node lookup, no lock).
        if not self._recent_trips:
            return
        node = self.storage.get_node(node_name)
        if not node:
            return
        with self._lock:
            self._recent_trips.pop(node.effective_fault_domain, None)


class EasytierPeerMonitor:
    """
    Sub-5s whole-host unreachability detection from `easytier-cli peer` (R4).

    EasyTier peers that vanish from the peer table are authoritative: the machine is gone,
    so every listener on it is unreachable. This bypasses SOCKS/HTTP probe timeouts, which
    is what made the incident take 15s+ to notice. Nodes without a mesh presence (plain
    public-IP proxies) are simply absent from every mesh host and are never tripped by
    this monitor — they fall back to the TCP-connect fast path in the gateway.
    """

    # "ipv4" -> "hostname" hint mapping is intentionally explicit rather than inferred:
    # guessing a hostname from a mesh IP is how you silently isolate the wrong machine.
    def __init__(
        self,
        storage: Storage,
        isolator: FailureDomainIsolator,
        cli_path: str = "easytier-cli",
        timeout: float = 3.0,
        host_by_mesh_ip: Optional[Dict[str, str]] = None,
        runner: Optional[Callable[[List[str]], str]] = None
    ):
        self.storage = storage
        self.isolator = isolator
        self.cli_path = cli_path
        self.timeout = timeout
        self.host_by_mesh_ip = dict(host_by_mesh_ip or {})
        self._runner = runner
        # mesh_ip -> consecutive scans in which it was absent. A single missed scrape must
        # not trip a domain; `miss_threshold` consecutive misses must.
        self._miss_counts: Dict[str, int] = {}
        self._missing_since: Dict[str, float] = {}
        self._last_seen: Dict[str, float] = {}
        # Only hosts that are *known mesh participants* may be tripped: either declared by
        # the operator (EASYTIER_HOST_MAP) or observed at least once in the peer table.
        # Inferring participation from "node.host is an IP" would mark every public-IP or
        # loopback listener (127.0.0.1 has 51 listeners in the production topology) as a
        # dead mesh host on the second scrape — a self-inflicted whole-pool kill.
        self._seen_hosts: set = set()
        self._hosts_cache: Optional[Dict[str, str]] = None
        self._hosts_cache_at: float = 0.0
        self._gauge_values: Dict[str, float] = {}
        self.miss_threshold = max(1, int(os.environ.get("EASYTIER_PEER_MISS_SCRAPES", "2")))

    # -- mesh introspection ----------------------------------------------------

    def _run_cli(self, args: List[str]) -> str:
        if self._runner is not None:
            return self._runner(args)
        completed = subprocess.run(
            [self.cli_path, *args],
            capture_output=True,
            text=True,
            timeout=self.timeout,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"{self.cli_path} {' '.join(args)} failed: {completed.stderr.strip()[:200]}"
            )
        return completed.stdout

    def list_peers(self) -> List[Dict[str, str]]:
        """
        Returns the current peer table as dicts with at least `ipv4` and `hostname`.

        `easytier-cli peer` prints an aligned table; the parser tolerates either the table
        form or a JSON array (some builds support `-o json`). Returning an empty list means
        "no peers known", which callers must treat as *no signal*, never as "all missing".
        """
        raw = self._run_cli(["peer"])
        return self.parse_peer_output(raw)

    @staticmethod
    def parse_peer_output(raw: str) -> List[Dict[str, str]]:
        if not raw or not raw.strip():
            return []

        stripped = raw.strip()
        if stripped.startswith("[") or stripped.startswith("{"):
            try:
                data = json.loads(stripped)
            except json.JSONDecodeError:
                data = None
            if data is not None:
                if isinstance(data, dict):
                    data = data.get("peers") or data.get("routes") or []
                return [
                    {
                        "ipv4": str(p.get("ipv4") or p.get("ip") or ""),
                        "hostname": str(p.get("hostname") or p.get("host") or ""),
                    }
                    for p in data
                    if isinstance(p, dict)
                ]

        peers: List[Dict[str, str]] = []
        for line in raw.splitlines():
            line = line.strip()
            if not line or line.startswith("---") or "ipv4" in line.lower():
                continue
            ipv4_match = re.search(r"\b(\d{1,3}(?:\.\d{1,3}){3})\b", line)
            if not ipv4_match:
                continue
            # Hostname column is the last whitespace-separated token in the table form.
            hostname = line.split()[-1] if line.split() else ""
            peers.append({"ipv4": ipv4_match.group(1), "hostname": hostname})
        return peers

    # -- monitoring ------------------------------------------------------------

    HOSTS_CACHE_TTL = 30.0

    def resolve_mesh_hosts(self) -> Dict[str, str]:
        """
        Maps mesh IP -> fault domain using (a) explicit overrides, (b) node metadata.

        Only hosts that look like mesh addresses participate; loopback is excluded. The
        result is cached briefly because the peer monitor scrapes every second and the
        node table has hundreds of rows.
        """
        now = time.monotonic()
        if self._hosts_cache is not None and (now - self._hosts_cache_at) < self.HOSTS_CACHE_TTL:
            return dict(self._hosts_cache)

        mapping: Dict[str, str] = dict(self.host_by_mesh_ip)
        for node in self.storage.list_nodes():
            mesh_ip = node.host if self._looks_like_mesh_ip(node.host) else None
            if not mesh_ip:
                # Port-forwarded mesh listeners: the fault domain is the host label
                # (e.g. "mesh-nl"); record it under the node's reported exit/host if the
                # caller supplied an explicit mesh IP binding.
                continue
            mapping.setdefault(mesh_ip, node.effective_fault_domain)
        self._hosts_cache = mapping
        self._hosts_cache_at = now
        return dict(mapping)

    @staticmethod
    def _looks_like_ip(value: str) -> bool:
        return bool(re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}", (value or "").strip()))

    @classmethod
    def _looks_like_mesh_ip(cls, value: str) -> bool:
        """An IPv4 that can plausibly be an EasyTier peer (never loopback/unspecified)."""
        candidate = (value or "").strip()
        if not cls._looks_like_ip(candidate):
            return False
        return not candidate.startswith("127.") and candidate != "0.0.0.0"

    def _set_gauge(self, domain: str, value: float):
        """Writes the host-unreachable gauge only when it actually changed."""
        if self._gauge_values.get(domain) == value:
            return
        self._gauge_values[domain] = value
        self.storage.set_metric(
            "ip_pool_host_unreachable_seconds", {"host": domain}, value
        )

    def scan_once(self) -> Dict[str, Any]:
        """
        One scrape. Marks a mesh host unreachable after `miss_threshold` consecutive
        scrapes in which it is absent, then asks the isolator to drain its domain.
        """
        try:
            peers = self.list_peers()
        except Exception as exc:  # CLI missing, mesh down, timeout
            return {"ok": False, "error": str(exc), "tripped": []}

        # No peer table at all is ambiguous (mesh daemon restarting) — never interpret it
        # as "every host vanished".
        if not peers:
            return {"ok": True, "peers": 0, "note": "empty peer table, no signal", "tripped": []}

        present = {p["ipv4"] for p in peers if p.get("ipv4")}
        now = time.monotonic()
        self._seen_hosts |= present
        mesh_hosts = self.resolve_mesh_hosts()

        # A host is monitored once either the operator declared it or it has been seen in
        # the mesh. Hosts that have never appeared are not proof of anything.
        explicit = set(self.host_by_mesh_ip)
        monitored = {
            ip: domain for ip, domain in mesh_hosts.items()
            if ip in explicit or ip in self._seen_hosts
        }

        tripped: List[Dict[str, Any]] = []
        for mesh_ip, label in monitored.items():
            # The operator's EASYTIER_HOST_MAP label and the nodes' stored fault domain can
            # legitimately differ (nodes.yaml may omit `fault_domain`, in which case it falls
            # back to `host`). Isolation must use the key storage actually uses, otherwise
            # `mark_domain_dead` matches nothing and the outage is silently ignored.
            domain = self._effective_domain(mesh_ip, label)
            if mesh_ip in present:
                self._miss_counts.pop(mesh_ip, None)
                self._missing_since.pop(mesh_ip, None)
                self._last_seen[mesh_ip] = now
                self._set_gauge(domain, 0.0)
                continue

            first_miss = self._missing_since.setdefault(mesh_ip, now)
            misses = self._miss_counts.get(mesh_ip, 0) + 1
            self._miss_counts[mesh_ip] = misses
            self._set_gauge(domain, round(now - first_miss, 3))

            if misses < self.miss_threshold:
                continue

            # Already isolated: keep measuring the outage, do not re-run the sweep.
            counts = self.storage.domain_status_counts(domain)
            total = sum(counts.values())
            if total and counts.get(HealthStatus.DEAD.value, 0) >= total:
                self._miss_counts.pop(mesh_ip, None)
                continue

            result = self.isolator.report_unreachable(
                self._pick_domain_node(domain),
                reason="easytier_peer_missing",
                confirmed_dead=True,
                whole_domain=True,
            )
            if result.get("action") == "isolated":
                tripped.append({"domain": domain, "mesh_ip": mesh_ip, "result": result})
            # Reset the arming counter so the next real outage is detected afresh; keep
            # `_missing_since` so the outage gauge keeps growing instead of restarting.
            self._miss_counts.pop(mesh_ip, None)

        return {"ok": True, "peers": len(peers), "tripped": tripped, "watched": sorted(monitored)}

    def _effective_domain(self, mesh_ip: str, label: str) -> str:
        """
        Resolves the storage-level fault-domain key for a monitored mesh host.

        `label` (from EASYTIER_HOST_MAP) is preferred when nodes are actually filed under
        it; otherwise the nodes' own key — typically their `host` IP — is authoritative.
        """
        if self.storage.list_nodes_by_domain(label):
            return label
        if label != mesh_ip and self.storage.list_nodes_by_domain(mesh_ip):
            return mesh_ip
        return label

    def _pick_domain_node(self, domain: str) -> str:
        nodes = self.storage.list_nodes_by_domain(domain)
        return nodes[0].name if nodes else ""
