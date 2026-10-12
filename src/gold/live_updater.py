
import hashlib
import json
from datetime import datetime, timezone

import pandas as pd

from src.gold.clickhouse_loader import get_client
from src.silver.transformations import transform_bronze


TABLE = "inventory_v2.blocked_stock_live_updates"

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
    "event_version",
    "processed_at",
]


def update_live_gold(event_id, raw_data, source_date):
    # Recover the original event record
    record = json.loads(raw_data)

    df = pd.DataFrame([record])
    df["event_id"] = event_id
    df["source_date"] = source_date

    # Apply the same Silver transformation
    # used by the historical pipeline.
    silver = transform_bronze(df, "live")
    row = silver.iloc[0]

    if not bool(row["is_valid"]):
        raise ValueError(f"Invalid Silver record: {event_id}")

    if pd.isna(row["material_type"]):
        raise ValueError(
            f"Missing material classification: {event_id}"
        )

    snapshot_date = pd.to_datetime(
        row["source_date"]
    ).date()

    # Preserve the existing deterministic event version.
    version = int(
        hashlib.sha256(
            json.dumps(
                {
                    "event_id": event_id,
                    "raw_data": record,
                    "source_date": source_date,
                },
                sort_keys=True,
                default=str,
            ).encode("utf-8")
        ).hexdigest()[:15],
        16,
    )

    def optional_text(value):
        return None if pd.isna(value) else str(value)

    values = [
        event_id,
        snapshot_date,
        str(row["plant"]),
        str(row["material"]),
        optional_text(row.get("batch")),
        float(row["blocked"]),
        float(row["value"]),
        str(row["currency"]),
        int(row["blocked_since_days"]),
        str(row["ageing_category"]),
        optional_text(row.get("stock_category")),
        str(row["material_type"]),
        "live",
        version,
        datetime.now(timezone.utc),
    ]

    client = get_client()

    try:
        client.insert(
            TABLE,
            [values],
            column_names=COLUMNS,
        )
    finally:
        client.close()

    return event_id
