from typing import Dict, Iterable, List, Tuple
from zoneinfo import ZoneInfo
import pandas as pd

DAY_NORMAL = "NORMAL"
DAY_SPRING = "SPRING_FORWARD"
DAY_FALL = "FALL_BACK"


def classify_day_type(date_value, tz: ZoneInfo) -> Tuple[str, int]:
    """Return (day_type, expected_hours) for a local date in the ISO timezone."""
    date_value = pd.Timestamp(date_value).date()
    start = pd.Timestamp(date_value).tz_localize(tz)
    end = pd.Timestamp(date_value) + pd.Timedelta(days=1)
    end = end.tz_localize(tz)
    hours = int((end - start).total_seconds() / 3600)
    if hours == 23:
        return DAY_SPRING, hours
    if hours == 25:
        return DAY_FALL, hours
    return DAY_NORMAL, hours


def build_day_type_map(dates: Iterable[pd.Timestamp], tz: ZoneInfo) -> Dict[str, Dict]:
    unique_dates = sorted({pd.Timestamp(d).date() for d in dates})
    day_info: Dict[str, Dict] = {}
    for d in unique_dates:
        day_type, hours = classify_day_type(d, tz)
        day_info[str(d)] = {"type": day_type, "expected_hours": hours}
    return day_info


def localize_with_dst(
    naive_ts: pd.Series,
    tz: ZoneInfo,
    day_info: Dict[str, Dict],
) -> Tuple[pd.Series, List[List[str]]]:
    """Localize naive timestamps with DST-aware flags.

    For fall-back days, duplicated local times are disambiguated by assigning
    the first occurrence to DST and the second to standard time.
    Nonexistent spring-forward times are flagged and left as NaT.
    """
    flags: List[List[str]] = [[] for _ in range(len(naive_ts))]

    date_strs = naive_ts.dt.date.astype(str)
    fall_back_dates = {
        d for d, info in day_info.items() if info["type"] == DAY_FALL
    }
    spring_dates = {
        d for d, info in day_info.items() if info["type"] == DAY_SPRING
    }

    duplicate_mask = naive_ts.duplicated(keep=False)
    fall_back_mask = date_strs.isin(fall_back_dates)
    ambiguous_mask = duplicate_mask & fall_back_mask

    ambiguous = None
    drop_idx = []
    if ambiguous_mask.any():
        ambiguous = pd.Series([False] * len(naive_ts), index=naive_ts.index, dtype=object)
        grouped = naive_ts[ambiguous_mask].groupby(naive_ts)
        for _, idx in grouped.groups.items():
            idx_list = list(idx)
            if len(idx_list) == 2:
                ambiguous.loc[idx_list[0]] = True
                ambiguous.loc[idx_list[1]] = False
                for i in idx_list:
                    flags[i].append("DST_FALL_BACK_DUPLICATE")
            else:
                for i in idx_list:
                    drop_idx.append(i)
                    flags[i].append("DST_FALL_BACK_AMBIGUOUS")

    if drop_idx:
        naive_ts = naive_ts.copy()
        naive_ts.loc[drop_idx] = pd.NaT

    localized = naive_ts.dt.tz_localize(
        tz,
        ambiguous=ambiguous if ambiguous is not None else "NaT",
        nonexistent="NaT",
    )

    nat_mask = localized.isna()
    if nat_mask.any():
        for i in localized[nat_mask].index:
            if date_strs.loc[i] in spring_dates:
                flags[i].append("DST_SPRING_FORWARD_NONEXISTENT")
            else:
                flags[i].append("DST_LOCALIZE_FAILED")

    return localized, flags
