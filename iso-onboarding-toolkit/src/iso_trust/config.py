from dataclasses import dataclass
from typing import Dict, List, Optional
import yaml


@dataclass
class Thresholds:
    consistency_abs_mw: float
    consistency_pct: float
    spike_abs_mw: float
    spike_pct: float
    flatline_window_hours: int


@dataclass
class Penalties:
    missing_hour: float
    missing_zone: float
    dst_day: float
    consistency_violation: float
    anomaly: float


@dataclass
class FeedConfig:
    iso: str
    timezone: str
    date_col: str
    hour_col: str
    hour_type: str
    zones: List[str]
    total_zone: str
    note_col: Optional[str]
    expected_frequency: str
    thresholds: Thresholds
    penalties: Penalties


def load_config(path: str) -> FeedConfig:
    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    thresholds = raw.get("thresholds", {})
    penalties = raw.get("penalties", {})

    return FeedConfig(
        iso=raw["iso"],
        timezone=raw["timezone"],
        date_col=raw["date_col"],
        hour_col=raw["hour_col"],
        hour_type=raw["hour_type"],
        zones=raw["zones"],
        total_zone=raw["total_zone"],
        note_col=raw.get("note_col"),
        expected_frequency=raw.get("expected_frequency", "1h"),
        thresholds=Thresholds(
            consistency_abs_mw=thresholds.get("consistency_abs_mw", 250.0),
            consistency_pct=thresholds.get("consistency_pct", 0.01),
            spike_abs_mw=thresholds.get("spike_abs_mw", 2500.0),
            spike_pct=thresholds.get("spike_pct", 0.08),
            flatline_window_hours=thresholds.get("flatline_window_hours", 6),
        ),
        penalties=Penalties(
            missing_hour=penalties.get("missing_hour", 0.75),
            missing_zone=penalties.get("missing_zone", 0.5),
            dst_day=penalties.get("dst_day", 2.0),
            consistency_violation=penalties.get("consistency_violation", 1.5),
            anomaly=penalties.get("anomaly", 1.0),
        ),
    )


def validate_config(cfg: FeedConfig) -> None:
    if cfg.hour_type not in {"HE", "HR"}:
        raise ValueError("hour_type must be 'HE' or 'HR'.")
    if not cfg.zones:
        raise ValueError("zones must include at least one ISO zone.")
    if cfg.total_zone not in cfg.zones:
        raise ValueError("total_zone must be present in zones.")
