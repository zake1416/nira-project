from typing import Dict

from iso_trust.config import FeedConfig


def compute_trust_score(metrics: Dict, cfg: FeedConfig) -> Dict:
    completeness = metrics.get("completeness", {})
    consistency = metrics.get("consistency", {})
    sanity = metrics.get("sanity", {})
    dst = metrics.get("dst", {})

    missing_hours = completeness.get("missing_hours_total", 0)
    missing_zones = completeness.get("missing_zones_per_hour", 0)
    dst_days = dst.get("dst_days", 0)
    consistency_violations = consistency.get("violation_count", 0)

    anomaly_events = (
        sanity.get("negative_count", 0)
        + sanity.get("spike_count", 0)
        + sanity.get("flatline_count", 0)
    )

    score = 100.0
    score -= missing_hours * cfg.penalties.missing_hour
    score -= missing_zones * cfg.penalties.missing_zone
    score -= dst_days * cfg.penalties.dst_day
    score -= consistency_violations * cfg.penalties.consistency_violation
    score -= anomaly_events * cfg.penalties.anomaly

    score = max(0.0, round(score, 2))

    if score >= 90:
        level = "HIGH"
    elif score >= 70:
        level = "DEGRADED"
    else:
        level = "DO_NOT_USE"

    return {
        "score": score,
        "level": level,
        "breakdown": {
            "missing_hours": missing_hours,
            "missing_zones": missing_zones,
            "dst_days": dst_days,
            "consistency_violations": consistency_violations,
            "anomaly_events": anomaly_events,
        },
    }
