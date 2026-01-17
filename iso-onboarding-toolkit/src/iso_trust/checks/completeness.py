from typing import Dict, List, Tuple
import pandas as pd

from iso_trust.config import FeedConfig
from iso_trust.normalize import CANONICAL_TOTAL_ZONE


def check_completeness(
    df: pd.DataFrame,
    cfg: FeedConfig,
    day_info: Dict[str, Dict],
) -> Tuple[List[Dict], Dict]:
    issues: List[Dict] = []

    expected_zones = [
        CANONICAL_TOTAL_ZONE if z == cfg.total_zone else z for z in cfg.zones
    ]
    expected_zone_count = len(expected_zones)

    df_valid = df.dropna(subset=["timestamp_local"])
    if df_valid.empty:
        issues.append({
            "severity": "HIGH",
            "check": "completeness",
            "message": "No valid timestamps available to assess completeness.",
            "hint": "Resolve timestamp parsing or DST localization errors.",
        })
        return issues, {}

    df_valid = df_valid.copy()
    df_valid["local_date"] = df_valid["timestamp_local"].dt.date.astype(str)

    expected_rows = 0
    missing_hours_total = 0
    missing_by_day: Dict[str, int] = {}

    for date_str, info in day_info.items():
        expected_hours = info["expected_hours"]
        expected_rows += expected_hours * expected_zone_count
        day_rows = df_valid[df_valid["local_date"] == date_str]
        for zone in expected_zones:
            zone_rows = day_rows[day_rows["zone"] == zone]
            missing = max(expected_hours - len(zone_rows), 0)
            if missing:
                missing_by_day[date_str] = missing_by_day.get(date_str, 0) + missing
            missing_hours_total += missing

    actual_rows = len(df_valid)
    completeness_pct = actual_rows / expected_rows if expected_rows else 0.0

    per_ts = df_valid.groupby("timestamp_local")["zone"].nunique()
    missing_zones_per_hour = int((expected_zone_count - per_ts).clip(lower=0).sum())

    if missing_hours_total > 0:
        severity = "HIGH" if completeness_pct < 0.98 else "MEDIUM"
        issues.append({
            "severity": severity,
            "check": "completeness",
            "message": (
                f"Missing {missing_hours_total} hourly observations across zones. "
                f"Completeness {completeness_pct:.2%}."
            ),
            "hint": "Identify gaps by date/zone and reconcile missing hours with ISO source files.",
        })

    if missing_zones_per_hour > 0:
        issues.append({
            "severity": "HIGH",
            "check": "completeness",
            "message": f"{missing_zones_per_hour} zone observations missing within hourly timestamps.",
            "hint": "Ensure all expected zones are present for every hour before downstream use.",
        })

    metrics = {
        "expected_rows": int(expected_rows),
        "actual_rows": int(actual_rows),
        "completeness_pct": completeness_pct,
        "missing_hours_total": int(missing_hours_total),
        "missing_zones_per_hour": int(missing_zones_per_hour),
        "missing_by_day": missing_by_day,
    }

    return issues, metrics
