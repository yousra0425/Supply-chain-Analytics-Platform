import os
from datetime import datetime, timedelta, timezone

from cassandra.cluster import Cluster
from src.utils.cassandra_connection import CassandraConnection
from cassandra.util import Date


def get_recent_events(limit=20):
    cluster = Cluster(
        [os.getenv("CASSANDRA_HOST", "127.0.0.1")],
        port=9042,
        connection_class=CassandraConnection,    connect_timeout=15,
        control_connection_timeout=15,
    )

    try:
        session = cluster.connect("inventory_v2")

        query = session.prepare("""
            SELECT
                message_id,
                event_id,
                material,
                plant,
                batch_number,
                blocked_quantity,
                blocked_value,
                source_date,
                received_at
            FROM recent_inventory_events_v2
            WHERE event_date = ?
            LIMIT ?
        """)

        records = []
        today = datetime.now(timezone.utc).date()

        for days_ago in range(3):
            target_date = today - timedelta(days=days_ago)

            rows = session.execute(
                query,
                (Date(target_date), limit),
            )

            for row in rows:
                records.append({
                    "message_id": row.message_id,
                    "event_id": row.event_id,
                    "material": row.material,
                    "plant": row.plant,
                    "batch": row.batch_number,
                    "blocked_quantity": row.blocked_quantity,
                    "blocked_value": row.blocked_value,
                    "source_date": row.source_date,
                    "received_at": (
                        row.received_at.isoformat()
                        if row.received_at
                        else None
                    ),
                })

        records.sort(
            key=lambda record: record["received_at"] or "",
            reverse=True,
        )

        return records[:limit]

    finally:
        cluster.shutdown()
