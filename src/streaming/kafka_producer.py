
import hashlib
import json
import time
from datetime import datetime, timezone
import os
import pandas as pd
from kafka import KafkaProducer

from src.ingestion.excel_loader import (
    DATA_DIR,
    FILE_NAME,
    load_excel,
    validate_data,
)


TOPIC = "inventory.v2.raw"
BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)
SOURCE_DATE = "2026-08-17"


def create_event(row, row_number):
    record = row.to_dict()

    # Convert pandas values into JSON-compatible types
    for key, value in record.items():
        if pd.isna(value):
            record[key] = None
        elif isinstance(value, (pd.Timestamp, datetime)):
            record[key] = value.isoformat()
        elif hasattr(value, "item"):
            record[key] = value.item()

    # Stable identifier for the same source record
    event_id = hashlib.sha256(
        f"{SOURCE_DATE}:{row_number}".encode()
    ).hexdigest()

    return {
        "event_id": event_id,
        "event_type": "blocked_stock.snapshot",
        "source_date": SOURCE_DATE,
        "streamed_at": datetime.now(timezone.utc).isoformat(),
        "material": record.get("Material"),
        "plant": record.get("Plant"),
        "batch_number": record.get("Batch"),
        "blocked_quantity": record.get("Blocked"),
        "inventory_value": record.get("Value"),
        "currency": record.get("Currency"),
        "raw_data": record,
    }


def stream_data(limit=10, delay=0.5):
    df = validate_data(load_excel(DATA_DIR / FILE_NAME))

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False
        ).encode("utf-8"),
        acks="all",
        retries=3,
    )

    try:
        for index, (_, row) in enumerate(df.head(limit).iterrows()):
            event = create_event(row, index)

            future = producer.send(
                TOPIC,
                key=event["event_id"].encode("utf-8"),
                value=event,
            )

            metadata = future.get(timeout=15)

            print(
                f"Sent {index + 1}/{min(limit, len(df))}: "
                f"{event['material']} "
                f"(partition={metadata.partition}, "
                f"offset={metadata.offset})"
            )

            time.sleep(delay)

        producer.flush()

        print("\nStreaming completed successfully!")

    finally:
        producer.close()


if __name__ == "__main__":
    stream_data(limit=1886, delay=0.05)
