
from pathlib import Path

import pandas as pd

from src.ingestion.excel_loader import (
    DATA_DIR,
    FILE_NAME,
    load_excel,
    validate_data,
)

OUTPUT_DIR = Path("data/historical")


def export_historical_snapshot() -> Path:
    df = load_excel(DATA_DIR / FILE_NAME)
    df = validate_data(df)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    output_path = OUTPUT_DIR / "blocked_stock_2026_08_17.parquet"

    df.to_parquet(
        output_path,
        engine="pyarrow",
        index=False,
    )

    # Verify that the export preserves the original records.
    saved_df = pd.read_parquet(output_path)

    assert len(saved_df) == len(df)
    assert list(saved_df.columns) == list(df.columns)

    print(f"\nHistorical snapshot created: {output_path}")
    print(f"Rows: {len(saved_df)}")
    print(f"Columns: {len(saved_df.columns)}")

    return output_path


if __name__ == "__main__":
    export_historical_snapshot()
