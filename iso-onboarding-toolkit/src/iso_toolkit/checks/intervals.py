from typing import List, Dict
import pandas as pd
from iso_toolkit.config import FeedConfig


def check_missing_intervals(df: pd.DataFrame, cfg: FeedConfig) -> List[Dict]:
    issues: List[Dict] = []
    ts_col = cfg.timestamp_col

    if ts_col not in df.columns:
        return issues

    d = df[[ts_col]].dropna().copy()
    if d.empty:
        return [{
            "severity": "HIGH",
            "check": "intervals",
            "message": "No valid timestamps available to check intervals.",
            "hint": "Fix timestamp parsing/localization.",
        }]

    d = d.sort_values(ts_col)
    expected = cfg.expected_frequency

    start, end = d[ts_col].iloc[0], d[ts_col].iloc[-1]
    expected_index = pd.date_range(start=start, end=end, freq=expected, tz="UTC")
    actual_index = pd.DatetimeIndex(d[ts_col].unique()).tz_convert("UTC")

    missing = expected_index.difference(actual_index)
    missing_count = len(missing)

    if missing_count > 0:
        diffs = d[ts_col].diff().dropna()
        max_gap = diffs.max()
        issues.append({
            "severity": "HIGH" if missing_count > 0.01 * len(expected_index) else "MEDIUM",
            "check": "intervals",
            "message": (
                f"Missing {missing_count} expected intervals ({expected}) between {start} and {end}. "
                f"Max observed gap: {max_gap}."
            ),
            "hint": "Backfill missing intervals or confirm ISO publication cadence and holidays/outage behavior.",
        })

    return issues
