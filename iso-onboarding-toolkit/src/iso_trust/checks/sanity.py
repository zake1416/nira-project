from typing import Dict, List, Tuple
import pandas as pd

from iso_trust.config import FeedConfig


def check_value_sanity(df: pd.DataFrame, cfg: FeedConfig) -> Tuple[List[Dict], Dict]:
    issues: List[Dict] = []

    df_valid = df.dropna(subset=["timestamp_local"]).copy()
    if df_valid.empty:
        return issues, {}

    df_valid["mw"] = pd.to_numeric(df_valid["mw"], errors="coerce")
    negative_count = int((df_valid["mw"] < 0).sum())
    if negative_count:
        issues.append({
            "severity": "HIGH",
            "check": "sanity",
            "message": f"{negative_count} observations have negative MW values.",
            "hint": "Confirm ISO data corrections or filter negative values before use.",
        })

    spike_abs = cfg.thresholds.spike_abs_mw
    spike_pct = cfg.thresholds.spike_pct

    spike_count = 0
    flatline_count = 0
    flatline_window = cfg.thresholds.flatline_window_hours

    for zone, zone_df in df_valid.groupby("zone"):
        zone_df = zone_df.sort_values("timestamp_local")
        diff = zone_df["mw"].diff().abs()
        prev = zone_df["mw"].shift(1).abs()
        pct = diff / prev.replace(0, pd.NA)
        spike_mask = (diff > spike_abs) | (pct > spike_pct)
        spike_count += int(spike_mask.sum())

        rolling_std = zone_df["mw"].rolling(flatline_window, min_periods=flatline_window).std()
        flatline_count += int((rolling_std == 0).sum())

    if spike_count:
        issues.append({
            "severity": "MEDIUM",
            "check": "sanity",
            "message": f"{spike_count} hourly changes exceed spike thresholds.",
            "hint": "Review sudden load jumps and verify they align with ISO event notes.",
        })

    if flatline_count:
        issues.append({
            "severity": "MEDIUM",
            "check": "sanity",
            "message": f"{flatline_count} flatline windows detected across zones.",
            "hint": "Investigate upstream data freezes or missing updates.",
        })

    metrics = {
        "negative_count": int(negative_count),
        "spike_count": int(spike_count),
        "flatline_count": int(flatline_count),
    }

    return issues, metrics
