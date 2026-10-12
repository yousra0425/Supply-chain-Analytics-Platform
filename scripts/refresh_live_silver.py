
from pathlib import Path

from src.silver.live_reader import read_live_bronze
from src.silver.transformations import transform_bronze

OUTPUT = Path("data/processed/silver_live.parquet")

EXPECTED_RECORDS = 1886
EXPECTED_VALUE = 1158169.31


def main():
    print("\n=== REFRESH LIVE SILVER ===\n")

    # Read the existing Cassandra Bronze records.
    bronze = read_live_bronze()

    # Apply the updated Silver transformation.
    silver = transform_bronze(bronze, "live")

    print("\nMaterial classifications:")
    print(silver["material_type"].value_counts(dropna=False))

    records = len(silver)
    valid = int(silver["is_valid"].sum())
    total_value = round(float(silver["value"].sum()), 2)
    missing = int(silver["material_type"].isna().sum())

    print("\nValidation:")
    print("Records:", records)
    print("Valid:", valid)
    print("Blocked value:", total_value)
    print("Missing classifications:", missing)

    if records != EXPECTED_RECORDS:
        raise RuntimeError("Unexpected record count")

    if valid != EXPECTED_RECORDS:
        raise RuntimeError("Invalid Silver records found")

    if abs(total_value - EXPECTED_VALUE) > 0.01:
        raise RuntimeError("Blocked value mismatch")

    if missing:
        raise RuntimeError("Missing material classifications")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    # Only overwrite the Parquet after validation passes.
    silver.to_parquet(OUTPUT, index=False)

    print("\nLive Silver successfully refreshed:")
    print(OUTPUT)


if __name__ == "__main__":
    main()
