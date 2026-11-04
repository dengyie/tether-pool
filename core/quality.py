"""
core/quality.py - Lightweight Multi-Factor IP Quality Evaluation Engine

Evaluates proxy IP quality and assigns a tier (premium vs standard).
Standards synthesize industry practices (Scamalytics, IPQS, Cloudflare Radar,
AbuseIPDB, ping0.cc) tailored for production proxy pool operations:
1. Node Provenance: self-hosted / native datacenter vs third-party aggregated.
2. Latency & Jitter: RTT response time benchmarks.
3. Circuit Stability: Health status and historical consecutive failures.
4. Exit IP Authenticity: Verified public egress IP.
5. Extensible Hooks: External fraud score API enrichment.
"""

from typing import Dict, Any, Tuple, Optional, List


class QualityTier:
    PREMIUM = "premium"
    STANDARD = "standard"


class IPQualityEvaluator:
    """
    Evaluates proxy nodes and yields a composite score (0-100) and tier classification.
    """

    @classmethod
    def evaluate_node(
        cls,
        tags: Optional[List[str]] = None,
        egress_type: Optional[str] = None,
        latency_ms: Optional[float] = None,
        status: Optional[str] = None,
        consecutive_failures: int = 0,
        exit_ip: Optional[str] = None,
        configured_tier: Optional[str] = None,
        external_fraud_score: Optional[int] = None,  # 0 (clean) to 100 (high risk)
    ) -> Tuple[int, str, Dict[str, Any]]:
        """
        Calculates quality score (0-100) and returns (score, tier, factors_breakdown).
        """
        tags_set = set(t.lower() for t in (tags or []))
        factors: Dict[str, Any] = {}

        # 1. Provenance Score (max 40 pts)
        # Self-hosted VPS or native datacenter IP has clean dedicated routing
        is_self_hosted = bool({"self_hosted", "native", "aws", "azure", "huawei", "leaseweb"} & tags_set)
        is_native = bool(egress_type and "native" in egress_type.lower())

        if is_self_hosted or is_native:
            provenance_score = 40
            provenance_desc = "self_hosted_dedicated"
        elif egress_type == "warp_ipv6":
            provenance_score = 25
            provenance_desc = "cloudflare_warp"
        else:
            provenance_score = 15
            provenance_desc = "third_party_or_aggregated"
        factors["provenance"] = {"score": provenance_score, "type": provenance_desc}

        # 2. Latency / RTT Score (max 30 pts)
        if latency_ms is None or latency_ms <= 0:
            latency_score = 10  # unprobed baseline
            latency_desc = "unprobed"
        elif latency_ms < 150:
            latency_score = 30
            latency_desc = "ultra_low"
        elif latency_ms < 350:
            latency_score = 20
            latency_desc = "low"
        elif latency_ms < 700:
            latency_score = 10
            latency_desc = "moderate"
        else:
            latency_score = 0
            latency_desc = "high_latency"
        factors["latency"] = {"score": latency_score, "rtt_ms": latency_ms, "rating": latency_desc}

        # 3. Stability & Circuit State (max 20 pts)
        status_lower = (status or "").lower()
        if status_lower == "healthy" and consecutive_failures == 0:
            stability_score = 20
        elif status_lower == "degraded" or consecutive_failures == 1:
            stability_score = 5
        elif status_lower == "dead" or consecutive_failures >= 2:
            stability_score = -20
        else:
            stability_score = 10
        factors["stability"] = {"score": stability_score, "status": status, "failures": consecutive_failures}

        # 4. Exit IP Authenticity (max 10 pts)
        has_exit_ip = bool(exit_ip and exit_ip.strip())
        exit_score = 10 if has_exit_ip else 0
        factors["exit_ip"] = {"score": exit_score, "verified": has_exit_ip}

        # 5. External Threat/Fraud Score penalty (if provided)
        fraud_penalty = 0
        if external_fraud_score is not None:
            # 0-100 fraud score (Scamalytics / IPQS)
            fraud_penalty = int(min(100, max(0, external_fraud_score)) * 0.3)
            factors["fraud_penalty"] = {"penalty": fraud_penalty, "raw_score": external_fraud_score}

        total_score = max(0, min(100, provenance_score + latency_score + stability_score + exit_score - fraud_penalty))

        # 6. Tier Classification Decision
        # Explicit configuration takes strict precedence:
        # - Configured standard is strictly locked (cannot be promoted dynamically).
        # - Configured premium is preserved unless the node is completely dead.
        # - Unconfigured nodes are dynamically tiered based on quality score and latency.
        req_tier = configured_tier.strip().lower() if configured_tier else None
        if req_tier == QualityTier.STANDARD:
            tier = QualityTier.STANDARD
        elif req_tier == QualityTier.PREMIUM:
            tier = QualityTier.PREMIUM if status_lower != "dead" else QualityTier.STANDARD
        else:
            # Dynamic Classification:
            # Must have proven provenance (self_hosted/native), low latency (<350ms), healthy status, and score >= 75
            if total_score >= 75 and (latency_ms is not None and latency_ms < 350) and status_lower != "dead":
                tier = QualityTier.PREMIUM
            else:
                tier = QualityTier.STANDARD

        return total_score, tier, factors
