
from pathlib import Path

import pandas as pd


DATA_DIR = Path("data/incoming")
FILE_NAME = "Blocked Stock 17.08.26 - recreated.xlsx"

REQUIRED_COLUMNS = [
    "Plant",
    "Material",
    "Batch",
    "Blocked",
    "Value",
    "Currency",
    "Created on",
    "Blocked Since (Days)",
    "Status",
]


def load_excel(file_path: Path) -> pd.DataFrame:
    """Load blocked-stock data from the Excel workbook."""

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_excel(
        file_path,
        sheet_name=0,
        header=2,
        dtype={
            "Plant": str,
            "Material": str,
            "Batch": str,
            "ID": str,
            "pn-plant": str,
        },
        engine="openpyxl",
    )

    df.columns = df.columns.str.strip()

    # Remove completely empty records
    df = df.dropna(how="all").reset_index(drop=True)

    return df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate required columns and numeric values."""

    missing = [
        column for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    for column in ["Blocked", "Value", "Blocked Since (Days)"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df["Created on"] = pd.to_datetime(
        df["Created on"], errors="coerce"
    )

    # Do not silently discard invalid records
    print("\nData quality report:")
    print(df[REQUIRED_COLUMNS].isna().sum().to_string())

    invalid = df["Blocked"].isna() | df["Value"].isna()

    if invalid.any():
        raise ValueError(
            f"{invalid.sum()} records contain invalid "
            "Blocked or Value fields"
        )

    return df


def main():
    file_path = DATA_DIR / FILE_NAME

    df = load_excel(file_path)
    df = validate_data(df)

    print("\nExcel loaded successfully!")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")

    print("\nFirst five records:")
    print(df[["Plant", "Material", "Blocked", "Value"]].head())

    print("\nData types:")
    print(df.dtypes.to_string())


if __name__ == "__main__":
    main()
