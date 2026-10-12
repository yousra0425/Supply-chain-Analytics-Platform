
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


API_URL = "http://127.0.0.1:8080/api/live/metrics"
DB_PATH = Path("data/processed/streaming_metrics.db")


def initialize_database():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS streaming_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                published_messages INTEGER,
                committed_messages INTEGER,
                consumer_lag INTEGER,
                cassandra_records INTEGER
            )
        """)


def collect_metrics():
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    data = response.json()

    kafka = data["kafka"]
    cassandra = data["cassandra"]

    timestamp = datetime.now(timezone.utc).isoformat()

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO streaming_metrics (
                timestamp,
                published_messages,
                committed_messages,
                consumer_lag,
                cassandra_records
            )
            VALUES (?, ?, ?, ?, ?)
        """, (
            timestamp,
            kafka["published_messages"],
            kafka["committed_messages"],
            kafka["consumer_lag"],
            cassandra["stored_records"],
        ))

    print(
        f"{timestamp} | "
        f"Published: {kafka['published_messages']} | "
        f"Committed: {kafka['committed_messages']} | "
        f"Lag: {kafka['consumer_lag']}"
    )


def main():
    initialize_database()

    print("Streaming metrics collector started...")
    print("Collecting every 10 seconds. Press Ctrl+C to stop.")

    try:
        while True:
            try:
                collect_metrics()
            except Exception as exc:
                print(f"Collection error: {exc}")

            time.sleep(10)

    except KeyboardInterrupt:
        print("\nMetrics collector stopped.")


if __name__ == "__main__":
    main()
