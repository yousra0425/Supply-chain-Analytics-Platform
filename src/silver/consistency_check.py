
import pandas as pd

from src.silver.historical_reader import read_historical_bronze
from src.silver.live_reader import read_live_bronze
from src.silver.transformations import transform_bronze


COLUMNS = [
    "plant",
    "material",
    "batch",
    "blocked",
    "value",
    "currency",
    "created_on",
    "blocked_since_days",
]


def prepare(df):
    result = df[COLUMNS].copy()

    result["created_on"] = pd.to_datetime(
        result["created_on"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    for col in ["blocked", "value", "blocked_since_days"]:
        result[col] = pd.to_numeric(result[col], errors="coerce")

    for col in ["plant", "material", "batch", "currency"]:
        result[col] = result[col].astype("string").str.strip()

    return result.fillna("<MISSING>")


def check_consistency():
    historical = transform_bronze(
        read_historical_bronze(), "historical"
    )

    live = transform_bronze(
        read_live_bronze(), "live"
    )

    # Compare record counts, including repeated identical records.
    historical_counts = prepare(historical).value_counts(
        dropna=False
    )
    live_counts = prepare(live).value_counts(
        dropna=False
    )

    comparison = historical_counts.compare(
        live_counts,
        keep_shape=True,
        keep_equal=False,
    ) if historical_counts.index.equals(live_counts.index) else None

    difference = historical_counts.sub(
        live_counts, fill_value=0
    )

    mismatched = difference[difference != 0]

    print("\nHISTORICAL VS LIVE")
    print("Historical records:", len(historical))
    print("Live records:", len(live))
    print("Different record groups:", len(mismatched))

    if mismatched.empty and len(historical) == len(live):
        print("SUCCESS: Both datasets match on key business fields.")
    else:
        print("WARNING: Data differences found!")
        print(mismatched.head(10))
        raise ValueError("Historical and live datasets differ")


if __name__ == "__main__":
    check_consistency()
