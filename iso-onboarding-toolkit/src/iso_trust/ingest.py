from typing import Iterable, List
import glob
import os
import pandas as pd


def resolve_inputs(patterns: Iterable[str]) -> List[str]:
    paths: List[str] = []
    for pattern in patterns:
        matches = glob.glob(pattern)
        if matches:
            paths.extend(matches)
        else:
            paths.append(pattern)
    return sorted(set(paths))


def _read_single(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    return pd.read_csv(path)


def read_inputs(paths: Iterable[str]) -> pd.DataFrame:
    frames = []
    for path in resolve_inputs(paths):
        df = _read_single(path)
        df["source_file"] = os.path.basename(path)
        frames.append(df)
    if not frames:
        raise FileNotFoundError("No input files found.")
    return pd.concat(frames, ignore_index=True)
