from typing import List, Dict
import pandas as pd
from iso_toolkit.config import FeedConfig


def check_units(df: pd.DataFrame, cfg: FeedConfig) -> List[Dict]:
    issues: List[Dict] = []
    val = cfg.value_col

    if val not in df.columns:
        return issues

    s = pd.to_numeric(df[val], errors="coerce")
    bad = int(s.isna().sum())
    if bad:
        issues.append({
            "severity": "MEDIUM",
            "check": "units",
            "message": f"{bad} rows in '{val}' could not be coerced to numeric.",
            "hint": "Ensure missing values are empty/null and not strings like 'N/A'.",
        })

    if "unit" in df.columns:
        units = sorted({u for u in df["unit"].dropna().unique()})
        if len(units) > 1:
            issues.append({
                "severity": "HIGH",
                "check": "units",
                "message": f"Multiple units detected in feed: {units}",
                "hint": "Normalize to a canonical unit (e.g., MW) at ingestion.",
            })
        elif len(units) == 1 and cfg.value_unit and units[0] != cfg.value_unit:
            issues.append({
                "severity": "MEDIUM",
                "check": "units",
                "message": (
                    f"Feed unit '{units[0]}' differs from configured canonical '{cfg.value_unit}'."
                ),
                "hint": "Convert values or correct the configuration.",
            })

    return issues
