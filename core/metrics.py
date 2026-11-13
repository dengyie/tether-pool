"""
core/metrics.py - Cloud-native Prometheus / OpenMetrics generator for IP Pool
"""

from typing import List, Dict
from core.storage import Storage


class MetricsCollector:
    def __init__(self, storage: Storage):
        self.storage = storage

    def generate_metrics(self) -> str:
        """
        Produces Prometheus-compatible text representation with low cardinality.
        Strict rule: Never include node IPs or full proxy URLs as labels.
        """
        lines: List[str] = [
            "# Prometheus Metrics for IP Pool System",
            "# HELP ip_pool_up Operational health status of the IP pool control plane",
            "# TYPE ip_pool_up gauge",
            "ip_pool_up 1",
            ""
        ]

        # 1. Node counts by region, egress_type, and status
        node_dist = self.storage.get_metrics_node_distribution()
        lines.append("# HELP ip_pool_node_count Total proxy nodes grouped by region, egress type, status and tier")
        lines.append("# TYPE ip_pool_node_count gauge")
        for item in node_dist:
            lines.append(
                f'ip_pool_node_count{{region="{item["region"]}",egress_type="{item["egress_type"]}",status="{item["status"]}",tier="{item.get("tier", "standard")}"}} {item["count"]}'
            )
        lines.append("")

        # 2. Active account bindings by region and egress_type
        leases_dist = self.storage.get_metrics_bindings_distribution()
        lines.append("# HELP ip_pool_account_bindings Active account->exit bindings grouped by region, egress type and tier")
        lines.append("# TYPE ip_pool_account_bindings gauge")
        for item in leases_dist:
            lines.append(
                f'ip_pool_account_bindings{{region="{item["region"]}",egress_type="{item["egress_type"]}",tier="{item.get("tier", "standard")}"}} {item["count"]}'
            )
        lines.append("")

        # 3. Probe RTT Latencies (in seconds)
        latencies = self.storage.get_metrics_probe_latencies()
        lines.append("# HELP ip_pool_probe_latency_seconds_avg Average probe RTT in seconds for operational nodes")
        lines.append("# TYPE ip_pool_probe_latency_seconds_avg gauge")
        for item in latencies:
            avg_sec = round(item["avg_latency"] / 1000.0, 4)
            lines.append(
                f'ip_pool_probe_latency_seconds_avg{{region="{item["region"]}",egress_type="{item["egress_type"]}"}} {avg_sec}'
            )
        lines.append("")

        lines.append("# HELP ip_pool_probe_latency_seconds_max Maximum probe RTT in seconds for operational nodes")
        lines.append("# TYPE ip_pool_probe_latency_seconds_max gauge")
        for item in latencies:
            max_sec = round(item["max_latency"] / 1000.0, 4)
            lines.append(
                f'ip_pool_probe_latency_seconds_max{{region="{item["region"]}",egress_type="{item["egress_type"]}"}} {max_sec}'
            )
        lines.append("")

        # 4. Circuit Breaker transitions
        transitions = self.storage.get_metrics_circuit_transitions()
        lines.append("# HELP ip_pool_circuit_transitions_total Total circuit breaker state transition events")
        lines.append("# TYPE ip_pool_circuit_transitions_total counter")
        for item in transitions:
            lines.append(
                f'ip_pool_circuit_transitions_total{{region="{item["region"]}",from_state="{item["from_state"]}",to_state="{item["to_state"]}"}} {item["count"]}'
            )
        lines.append("")

        lines.extend(self._runtime_metrics_block())

        return "\n".join(lines) + "\n"

    def _runtime_metrics_block(self) -> List[str]:
        """
        R7: metrics fed from the in-process runtime_metrics table (bounded cardinality,
        surfaced here so the existing /metrics scrape stays the single entry point).

        Histogram buckets are stored pre-aggregated per `le` (cumulative), plus `_sum` /
        `_count`; the exporter renders the standard Prometheus bucket/sum/count layout.
        """
        lines: List[str] = []
        runtime = self.storage.get_runtime_metrics()

        counters = {
            "ip_pool_retry_exhausted_total": (
                "# HELP ip_pool_retry_exhausted_total Gateway retry budget exhausted per service and reason",
                "# TYPE ip_pool_retry_exhausted_total counter",
            ),
            "ip_pool_rehome_total": (
                "# HELP ip_pool_rehome_total Account binding rehomes per service and trigger",
                "# TYPE ip_pool_rehome_total counter",
            ),
            "ip_pool_rehome_blocked_total": (
                "# HELP ip_pool_rehome_blocked_total Rehome sweeps that kept an account on its (broken) node because the credential's bound constraint set had no healthy candidate in scope",
                "# TYPE ip_pool_rehome_blocked_total counter",
            ),
            "ip_pool_domain_trip_total": (
                "# HELP ip_pool_domain_trip_total Whole failure-domain isolation trips",
                "# TYPE ip_pool_domain_trip_total counter",
            ),
            "ip_pool_ban_events_total": (
                "# HELP ip_pool_ban_events_total Ban reports received per service and reason (ban-aware allocation)",
                "# TYPE ip_pool_ban_events_total counter",
            ),
        }
        for name, (help_line, type_line) in counters.items():
            emitted = False
            for item in runtime:
                if item["name"] != name:
                    continue
                if not emitted:
                    lines.append(help_line)
                    lines.append(type_line)
                    emitted = True
                labels = ",".join(f'{k}="{v}"' for k, v in sorted(item["labels"].items()))
                lines.append(f"{name}{{{labels}}} {item['value']:.0f}")
            if emitted:
                lines.append("")

        # Histogram: exit nodes attempted per client request (gateway).
        histogram = "ip_pool_gateway_attempts_per_request"
        hist_items = [i for i in runtime if i["name"].startswith(histogram)]
        if hist_items:
            lines.append("# HELP ip_pool_gateway_attempts_per_request Exit nodes attempted per client request")
            lines.append("# TYPE ip_pool_gateway_attempts_per_request histogram")
            bucket_rows = sorted(
                (i for i in hist_items if "_bucket" in i["name"]),
                key=lambda i: float("inf") if i["labels"].get("le") == "+Inf" else float(i["labels"]["le"]),
            )
            for item in bucket_rows:
                le = item["labels"].get("le", "+Inf")
                lines.append(
                    f'ip_pool_gateway_attempts_per_request_bucket{{le="{le}"}} {item["value"]:.0f}'
                )
            for item in hist_items:
                if item["name"] == histogram + "_sum":
                    lines.append(f"{histogram}_sum {item['value']:.2f}")
                elif item["name"] == histogram + "_count":
                    lines.append(f"{histogram}_count {item['value']:.0f}")
            lines.append("")

        # Gauge: whole-host unreachable duration (seconds) per fault domain.
        gauge_name = "ip_pool_host_unreachable_seconds"
        gauge_items = [i for i in runtime if i["name"] == gauge_name]
        if gauge_items:
            lines.append("# HELP ip_pool_host_unreachable_seconds How long a host has been unreachable, per fault domain")
            lines.append("# TYPE ip_pool_host_unreachable_seconds gauge")
            for item in gauge_items:
                labels = ",".join(f'{k}="{v}"' for k, v in sorted(item["labels"].items()))
                lines.append(f'{gauge_name}{{{labels}}} {item["value"]:.3f}')
            lines.append("")

        # Gauge: active quarantines per service (ban-aware allocation, 2026-11-14)。
        # scrape 时从 ban_events 表现算, 不进 runtime_metrics (实时性优先, 量级个位数)。
        try:
            active_quarantines = self.storage.get_active_quarantines()
        except Exception:
            active_quarantines = []
        if active_quarantines:
            per_service: Dict[str, int] = {}
            for q in active_quarantines:
                per_service[q.service] = per_service.get(q.service, 0) + 1
            lines.append("# HELP ip_pool_quarantine_active Currently quarantined (banned) nodes per service")
            lines.append("# TYPE ip_pool_quarantine_active gauge")
            for svc in sorted(per_service):
                lines.append(f'ip_pool_quarantine_active{{service="{svc}"}} {per_service[svc]}')
            lines.append("")

        return lines
