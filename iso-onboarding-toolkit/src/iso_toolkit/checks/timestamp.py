from typing import List, Dict, Tuple
import pandas as pd
from zoneinfo import ZoneInfo
from iso_toolkit.config import FeedConfig


def normalize_timestamps_and_check_dst(
    df: pd.DataFrame,
    cfg: FeedConfig,
) -> Tuple[pd.DataFrame, List[Dict], Dict]:
    issues: List[Dict] = []
    stats: Dict = {}
    ts_col = cfg.timestamp_col

    if ts_col not in df.columns:
        return df, [{
            "severity": "HIGH",
            "check": "timestamp",
            "message": f"Timestamp column '{ts_col}' missing; cannot normalize.",
            "hint": "Fix config or ensure feed includes a timestamp column."
        }], stats

    out = df.copy()

    out[ts_col] = pd.to_datetime(out[ts_col], errors="coerce")

    null_ts = int(out[ts_col].isna().sum())
    if null_ts:
        issues.append({
            "severity": "HIGH",
            "check": "timestamp",
            "message": f"{null_ts} rows have unparseable timestamps.",
            "hint": "Ensure timestamps match ISO format or document the exact pattern.",
        })

    tz = ZoneInfo(cfg.timezone)

    if out[ts_col].dt.tz is None:
        try:
            localized = out[ts_col].dt.tz_localize(
                tz,
                ambiguous="NaT",
                nonexistent="NaT",
            )
            dst_nat = int(localized.isna().sum()) - null_ts
            if dst_nat > 0:
                issues.append({
                    "severity": "MEDIUM",
                    "check": "timestamp",
                    "message": (
                        f"{dst_nat} timestamps fell into DST ambiguous/nonexistent windows and became NaT."
                    ),
                    "hint": (
                        "Consider ISO-specific DST rules: choose fold=1 for ambiguous times or shift-forward for nonexistent."
                    ),
                })
            out[ts_col] = localized
        except Exception as e:
            issues.append({
                "severity": "HIGH",
                "check": "timestamp",
                "message": f"Failed to tz_localize timestamps: {e}",
                "hint": "Confirm timezone and timestamp format; consider providing UTC timestamps upstream.",
            })

    try:
        out[ts_col] = out[ts_col].dt.tz_convert("UTC")
    except Exception as e:
        issues.append({
            "severity": "HIGH",
            "check": "timestamp",
            "message": f"Failed to convert timestamps to UTC: {e}",
            "hint": "Ensure timestamps are timezone-aware before conversion.",
        })

    if ts_col in out.columns:
        ts = out[ts_col].dropna()
        stats["min_utc"] = str(ts.min()) if len(ts) else None
        stats["max_utc"] = str(ts.max()) if len(ts) else None

        if len(ts) and not ts.is_monotonic_increasing:
            issues.append({
                "severity": "MEDIUM",
                "check": "timestamp",
                "message": "Timestamps are not sorted increasing.",
                "hint": "Sort by timestamp before downstream processing to avoid windowing errors.",
            })

    return out, issues, stats
