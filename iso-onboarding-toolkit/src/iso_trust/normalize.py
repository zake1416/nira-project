from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo
import pandas as pd

from iso_trust.config import FeedConfig
from iso_trust.dst import build_day_type_map, localize_with_dst


CANONICAL_TOTAL_ZONE = "CAISO_TOTAL"


def normalize_caiso(df: pd.DataFrame, cfg: FeedConfig) -> Tuple[pd.DataFrame, List[Dict], Dict[str, Dict]]:
    """Normalize CAISO-style wide CSVs into the canonical long format.

    Hour-ending (HE) is interpreted as the interval ending at the given hour.
    Hour-number (HR) is interpreted as the interval beginning at hour-1.
    """
    issues: List[Dict] = []

    work = df.copy()
    work.columns = [str(c).strip() for c in work.columns]

    hour_col = cfg.hour_col
    hour_type = cfg.hour_type
    zones = list(cfg.zones)
    total_zone = cfg.total_zone

    if hour_col not in work.columns:
        if "HE" in work.columns:
            hour_col = "HE"
            hour_type = "HE"
            issues.append({
                "severity": "MEDIUM",
                "check": "normalize",
                "message": "Configured hour column missing; auto-detected HE column.",
                "hint": "Confirm whether CAISO file uses HE or HR semantics.",
            })
        elif "HR" in work.columns:
            hour_col = "HR"
            hour_type = "HR"
            issues.append({
                "severity": "MEDIUM",
                "check": "normalize",
                "message": "Configured hour column missing; auto-detected HR column.",
                "hint": "Confirm whether CAISO file uses HE or HR semantics.",
            })

    if total_zone not in work.columns:
        lower_map = {c.lower(): c for c in work.columns}
        if "caiso total" in lower_map:
            work = work.rename(columns={lower_map["caiso total"]: total_zone})
        elif "caiso" in lower_map:
            work = work.rename(columns={lower_map["caiso"]: total_zone})
            issues.append({
                "severity": "MEDIUM",
                "check": "normalize",
                "message": "Configured total zone missing; auto-mapped CAISO to CAISO Total.",
                "hint": "Verify whether total column should be CAISO or CAISO Total.",
            })

    required = [cfg.date_col, hour_col] + zones
    missing = [c for c in required if c not in work.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    work[cfg.date_col] = pd.to_datetime(work[cfg.date_col], errors="coerce")
    invalid_date = int(work[cfg.date_col].isna().sum())
    if invalid_date:
        issues.append({
            "severity": "HIGH",
            "check": "normalize",
            "message": f"{invalid_date} rows have unparseable dates in '{cfg.date_col}'.",
            "hint": "Verify date format or normalize upstream before ingestion.",
        })

    work[hour_col] = pd.to_numeric(work[hour_col], errors="coerce")
    invalid_hour = int(work[hour_col].isna().sum())
    if invalid_hour:
        issues.append({
            "severity": "HIGH",
            "check": "normalize",
            "message": f"{invalid_hour} rows have invalid hours in '{hour_col}'.",
            "hint": "Ensure hour column is numeric and within 1-24.",
        })

    invalid_mask = work[cfg.date_col].isna() | work[hour_col].isna()
    if invalid_mask.any():
        work = work[~invalid_mask].copy()

    id_vars = [cfg.date_col, hour_col, "source_file"]
    note_col = None
    if cfg.note_col and cfg.note_col in work.columns:
        note_col = cfg.note_col
    else:
        for col in work.columns:
            if col.lower().startswith("note") or col.lower().startswith("unnamed"):
                note_col = col
                break
    if note_col:
        id_vars.append(note_col)

    long = work.melt(
        id_vars=id_vars,
        value_vars=zones,
        var_name="zone",
        value_name="mw",
    )

    zone_map = {total_zone: CANONICAL_TOTAL_ZONE}
    long["zone"] = long["zone"].replace(zone_map)

    hour = long[hour_col]
    date_base = long[cfg.date_col].dt.normalize()
    if hour_type == "HE":
        ts_naive = date_base + pd.to_timedelta(hour, unit="h")
    else:
        ts_naive = date_base + pd.to_timedelta(hour - 1, unit="h")

    invalid_hour_mask = (hour < 1) | (hour > 24) | hour.isna()

    tz = ZoneInfo(cfg.timezone)
    day_info = build_day_type_map(long[cfg.date_col].dropna(), tz)
    ts_local, dst_flags = localize_with_dst(ts_naive, tz, day_info)

    flags: List[List[str]] = []
    for idx in range(len(long)):
        row_flags: List[str] = []
        if invalid_hour_mask.iloc[idx]:
            row_flags.append("INVALID_HOUR")
        row_flags.extend(dst_flags[idx])
        if note_col and note_col in long.columns:
            note_val = long[note_col].iloc[idx]
            if isinstance(note_val, str) and note_val.strip():
                row_flags.append("SOURCE_NOTE")
        flags.append(row_flags)

    long["timestamp_local"] = ts_local
    long["timestamp_utc"] = ts_local.dt.tz_convert("UTC")
    long["iso"] = cfg.iso
    long["flags"] = flags

    long = long[[
        "timestamp_local",
        "timestamp_utc",
        "iso",
        "zone",
        "mw",
        "source_file",
        "flags",
    ]]

    return long, issues, day_info
