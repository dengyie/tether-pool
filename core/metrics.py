"""
core/metrics.py - Cloud-native Prometheus / OpenMetrics generator for IP Pool
"""

from typing import List
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

        return "\n".join(lines) + "\n"
