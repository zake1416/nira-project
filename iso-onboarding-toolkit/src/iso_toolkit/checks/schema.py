from typing import List, Dict
import pandas as pd
from iso_toolkit.config import FeedConfig


def check_schema(df: pd.DataFrame, cfg: FeedConfig) -> List[Dict]:
    issues = []
    cols = set(df.columns)

    for c in cfg.required_columns:
        if c not in cols:
            issues.append({
                "severity": "HIGH",
                "check": "schema",
                "message": f"Missing required column: {c}",
                "hint": f"Update the extractor or mapping to include '{c}'.",
            })

    if cfg.value_col in cols:
        try:
            pd.to_numeric(df[cfg.value_col], errors="raise")
        except Exception:
            issues.append({
                "severity": "HIGH",
                "check": "schema",
                "message": f"Value column '{cfg.value_col}' is not consistently numeric.",
                "hint": "Coerce values to numeric and handle non-numeric rows upstream.",
            })

    return issues
