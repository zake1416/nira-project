import pandas as pd


def read_input(path: str) -> pd.DataFrame:
    # v1: CSV only; easy to extend later
    return pd.read_csv(path)
