
import re

import pandas as pd


def normalize_column_name(name):
    name = str(name).strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")

def transform_bronze(df, source):
    """Apply common Silver transformations to Bronze records."""

    if source not in ("historical", "live"):
        raise ValueError("Source must be historical or live")

    df = df.copy()

    # Normalize column names
    df.columns = [
        normalize_column_name(col) for col in df.columns
    ]

    
    # Material classification from SAP "Component /FG"
    # Normalized source column: component_fg
    if "component_fg" not in df.columns:
        raise ValueError(
            "Missing 'Component /FG' classification in Bronze data"
        )

    material_type_mapping = {
        "finish goods": "Finished Goods",
        "produced component": "Produced Component",
        "purchased component": "Purchased Component",
        "packaging": "Packaging",
        "rm": "Raw Material",
    }

    raw_type = (
        df["component_fg"]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    df["material_type"] = raw_type.map(
        material_type_mapping
    ).astype("string")

    unknown_types = df.loc[
        df["material_type"].isna(),
        "component_fg"
    ].drop_duplicates().tolist()

    if unknown_types:
        raise ValueError(
            f"Unknown material classifications: {unknown_types}"
        )


    required = [
        "plant",
        "material",
        "blocked",
        "value",
        "currency",
        "blocked_since_days",
    ]

    missing = [col for col in required if col not in df.columns]

    if missing:
        raise ValueError(f"Missing columns: {missing}")

    # Preserve material identifiers
    for col in ["plant", "material", "batch"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    # Numeric fields
    for col in ["blocked", "value", "blocked_since_days"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Dates
    for col in ["created_on", "last_change", "end_of_life_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Preserve source metadata
    df["data_source"] = source

    if "source_date" not in df.columns:
        df["source_date"] = "2026-08-17"

    # Ageing categories
    df["ageing_category"] = pd.cut(
        df["blocked_since_days"],
        bins=[-float("inf"), 30, 90, 180, float("inf")],
        labels=["0-30 days", "31-90 days", "91-180 days", "180+ days"],
    ).astype("string")

    # Value categories
    df["value_category"] = pd.cut(
        df["value"],
        bins=[-float("inf"), 100, 1000, float("inf")],
        labels=["Low", "Medium", "High"],
    ).astype("string")

    # Basic data quality flag
    df["is_valid"] = (
        df["plant"].notna()
        & df["material"].notna()
        & df["blocked"].notna()
        & df["value"].notna()
        & (df["blocked"] >= 0)
        & (df["blocked_since_days"] >= 0)
    )

    return df


if __name__ == "__main__":
    from src.silver.historical_reader import read_historical_bronze
    from src.silver.live_reader import read_live_bronze

    historical = transform_bronze(
        read_historical_bronze(), "historical"
    )

    live = transform_bronze(
        read_live_bronze(), "live"
    )

    print("\nSILVER RESULTS")
    print("Historical:", len(historical))
    print("Live:", len(live))

    print("\nHistorical ageing distribution:")
    print(historical["ageing_category"].value_counts(dropna=False))

    print("\nLive ageing distribution:")
    print(live["ageing_category"].value_counts(dropna=False))

    print("\nData quality:")
    print("Historical valid:", historical["is_valid"].sum())
    print("Live valid:", live["is_valid"].sum())

    # Save previews for inspection
    output_dir = "data/processed"
    historical.to_parquet(
        f"{output_dir}/silver_historical.parquet", index=False
    )
    live.to_parquet(
        f"{output_dir}/silver_live.parquet", index=False
    )

    print("\nSilver datasets exported successfully.")
