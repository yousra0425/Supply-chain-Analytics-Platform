
from pathlib import Path

import duckdb
import pandas as pd


DB_PATH = Path("data/processed/inventory_history.duckdb")


def read_historical_bronze() -> pd.DataFrame:
    if not DB_PATH.exists():
        raise FileNotFoundError(DB_PATH)

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        df = con.execute(
            "SELECT * FROM blocked_stock_raw"
        ).df()

    print(f"Historical records loaded: {len(df)}")
    return df


if __name__ == "__main__":
    data = read_historical_bronze()

    print("\nColumns:")
    print(data.columns.tolist())

    print("\nSample:")
    print(data[["Plant", "Material", "Blocked", "Value"]].head())
