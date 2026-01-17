from typing import Dict, List, Tuple
import pandas as pd

from iso_trust.config import FeedConfig
from iso_trust.normalize import CANONICAL_TOTAL_ZONE


def check_consistency(df: pd.DataFrame, cfg: FeedConfig) -> Tuple[List[Dict], Dict]:
    issues: List[Dict] = []

    df_valid = df.dropna(subset=["timestamp_local"])
    if df_valid.empty:
        return issues, {}

    components = df_valid[df_valid["zone"] != CANONICAL_TOTAL_ZONE]
    total = df_valid[df_valid["zone"] == CANONICAL_TOTAL_ZONE]

    if total.empty:
        issues.append({
            "severity": "HIGH",
            "check": "consistency",
            "message": "No CAISO total observations found to validate internal consistency.",
            "hint": "Ensure the total zone column is included in the source feed.",
        })
        return issues, {}

    sum_by_ts = components.groupby("timestamp_local")["mw"].sum()
    total_by_ts = total.groupby("timestamp_local")["mw"].mean()
    joined = pd.concat([sum_by_ts, total_by_ts], axis=1, keys=["sum_zones", "total"])
    joined = joined.dropna()

    if joined.empty:
        issues.append({
            "severity": "HIGH",
            "check": "consistency",
            "message": "Unable to compute consistency; total or component zones missing per hour.",
            "hint": "Verify that zonal data and total data share the same timestamps.",
        })
        return issues, {}

    joined["abs_error"] = (joined["total"] - joined["sum_zones"]).abs()
    joined["pct_error"] = joined["abs_error"] / joined["total"].abs().replace(0, pd.NA)

    abs_thresh = cfg.thresholds.consistency_abs_mw
    pct_thresh = cfg.thresholds.consistency_pct

    violations = joined[(joined["abs_error"] > abs_thresh) | (joined["pct_error"] > pct_thresh)]
    violation_count = len(violations)

    if violation_count:
        issues.append({
            "severity": "HIGH",
            "check": "consistency",
            "message": (
                f"{violation_count} hours exceed consistency thresholds "
                f"(abs>{abs_thresh} MW or pct>{pct_thresh:.2%})."
            ),
            "hint": "Investigate mismatched totals or zonal corrections in the source feed.",
        })

    metrics = {
        "violation_count": int(violation_count),
        "max_abs_error": float(joined["abs_error"].max()),
        "max_pct_error": float(joined["pct_error"].max()),
        "mean_abs_error": float(joined["abs_error"].mean()),
        "mean_pct_error": float(joined["pct_error"].mean()),
    }

    return issues, metrics
