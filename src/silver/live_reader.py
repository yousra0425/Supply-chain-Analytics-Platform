
import json
import os
import pandas as pd
from cassandra.cluster import Cluster
from src.utils.cassandra_connection import CassandraConnection

def read_live_bronze() -> pd.DataFrame:
    cluster = Cluster(
        [os.getenv("CASSANDRA_HOST", "127.0.0.1")],
        port=9042,
        connection_class=CassandraConnection,
        connect_timeout=15,
        control_connection_timeout=15,
    )

    try:
        session = cluster.connect("inventory_v2")
        session.default_fetch_size = 500

        rows = session.execute("""
            SELECT event_id, source_date, received_at, raw_data
            FROM blocked_stock_events
        """)

        records = []

        for row in rows:
            record = json.loads(row.raw_data)

            record["event_id"] = row.event_id
            record["source_date"] = row.source_date
            record["received_at"] = row.received_at

            records.append(record)

        df = pd.DataFrame(records)

        print(f"Live records loaded: {len(df)}")
        return df

    finally:
        cluster.shutdown()


if __name__ == "__main__":
    data = read_live_bronze()

    print("\nFirst five records:")
    print(
        data[
            ["Plant", "Material", "Blocked", "Value"]
        ].head()
    )

    print("\nAvailable columns:", len(data.columns))
