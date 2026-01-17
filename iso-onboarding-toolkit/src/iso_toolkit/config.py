from dataclasses import dataclass
from typing import List, Optional
import yaml


@dataclass
class FeedConfig:
    feed_name: str
    timestamp_col: str
    timezone: str
    expected_frequency: str
    value_col: str
    value_unit: Optional[str]
    required_columns: List[str]
    optional_columns: List[str]


def load_config(path: str) -> FeedConfig:
    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    return FeedConfig(
        feed_name=raw["feed_name"],
        timestamp_col=raw["timestamp_col"],
        timezone=raw["timezone"],
        expected_frequency=raw["expected_frequency"],
        value_col=raw["value_col"],
        value_unit=raw.get("value_unit"),
        required_columns=raw.get("required_columns", []),
        optional_columns=raw.get("optional_columns", []),
    )
