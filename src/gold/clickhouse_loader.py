
from pathlib import Path
import hashlib
import os

import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv

load_dotenv(".env")

DATABASE = "inventory_v2"
SNAPSHOT_DATE = "2026-08-17"

    
COLUMNS = [
    "record_id",
    "snapshot_date",
    "plant",
    "material",
    "batch",
    "blocked_quantity",
    "blocked_value",
    "currency",
    "blocked_since_days",
    "ageing_category",
    "stock_category",
    "material_type",
    "data_source",
]




def get_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )


def create_tables(client):
    client.command(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

    for table in ["blocked_stock_historical", "blocked_stock_live"]:
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE}.{table} (
                record_id String,
                snapshot_date Date,
                plant String,
                material String,
                batch Nullable(String),
                blocked_quantity Float64,
                blocked_value Float64,
                currency String,
                blocked_since_days Int32,
                ageing_category String,
                stock_category Nullable(String),
                data_source String
            )
            ENGINE = MergeTree()
            ORDER BY (snapshot_date, record_id)
        """)


def prepare_data(file_path, source):
    df = pd.read_parquet(file_path)

    if not df["is_valid"].all():
        raise ValueError("Invalid Silver records found")

    result = pd.DataFrame()

    if source == "live":
        if "event_id" not in df.columns:
            raise ValueError("Missing live event_id")
        result["record_id"] = df["event_id"].astype(str)
    else:
        result["record_id"] = [
            hashlib.sha256(
                f"{SNAPSHOT_DATE}:{i}".encode()
            ).hexdigest()
            for i in range(len(df))
        ]

    result["snapshot_date"] = pd.to_datetime(
        df["source_date"]
    ).dt.date

    result["plant"] = df["plant"].astype(str)
    result["material"] = df["material"].astype(str)
    result["batch"] = df["batch"]
    result["blocked_quantity"] = df["blocked"].astype(float)
    result["blocked_value"] = df["value"].astype(float)
    result["currency"] = df["currency"].astype(str)

    result["blocked_since_days"] = (
        df["blocked_since_days"].astype(int)
    )

    result["ageing_category"] = df["ageing_category"]
    result["stock_category"] = df.get(
        "stock_category", pd.Series([None] * len(df))
    )
    
    if "material_type" not in df.columns:
        raise ValueError("Missing material_type in Silver data")

    result["material_type"] = (
        df["material_type"].astype(str)
    )

    result["data_source"] = source

    # Convert pandas missing values to Python None.
    result = result.astype(object).where(pd.notna(result), None)

    return result[COLUMNS]
 
 


def load_table(client, table, file_path, source):
    data = prepare_data(file_path, source)

    if data.empty:
        raise ValueError("Refusing to load an empty dataset")

    if data["record_id"].duplicated().any():
        raise ValueError("Duplicate record IDs detected")

    # Development full-refresh:
    # Replace this table's contents, never append a replay twice.
    client.command(f"TRUNCATE TABLE {DATABASE}.{table}")

    client.insert(
        f"{DATABASE}.{table}",
        data.values.tolist(),
        column_names=COLUMNS,
    )

    count = client.query(
        f"SELECT count() FROM {DATABASE}.{table}"
    ).result_rows[0][0]

    if count != len(data):
        raise RuntimeError(
            f"Expected {len(data)} rows, found {count}"
        )

    print(f"{table}: {count} records loaded")


def main():
    client = get_client()

    create_tables(client)

    load_table(
        client,
        "blocked_stock_historical",
        Path("data/processed/silver_historical.parquet"),
        "historical",
    )

    load_table(
        client,
        "blocked_stock_live",
        Path("data/processed/silver_live.parquet"),
        "live",
    )

    print("\nGold loading completed successfully!")


if __name__ == "__main__":
    main()
