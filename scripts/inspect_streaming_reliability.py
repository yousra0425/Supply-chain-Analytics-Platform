
from collections import Counter

from cassandra.cluster import Cluster
from cassandra.io.asyncorereactor import AsyncoreConnection


KEYSPACE = "inventory_v2"

TABLES = [
    "kafka_processing_ledger",
    "dlq_recovery_ledger",
]


def main():
    cluster = Cluster(
        ["127.0.0.1"],
        port=9042,
        connection_class=AsyncoreConnection,
        connect_timeout=15,
        control_connection_timeout=15,
    )

    try:
        session = cluster.connect(KEYSPACE)
        session.default_fetch_size = 500

        print("\n=== STREAMING RELIABILITY INSPECTION ===")

        for table in TABLES:
            print(f"\n{table}")

            metadata = session.cluster.metadata.keyspaces[
                KEYSPACE
            ].tables.get(table)

            if metadata is None:
                print("Table not found")
                continue

            print("Columns:")
            print(", ".join(metadata.columns.keys()))

            rows = list(
                session.execute(f"SELECT * FROM {table}")
            )

            print("Total ledger entries:", len(rows))

            if not rows:
                continue

            fields = rows[0]._fields

            if "status" in fields:
                counts = Counter(
                    getattr(row, "status", None)
                    for row in rows
                )

                print("Status distribution:")
                for status, count in sorted(
                    counts.items(),
                    key=lambda item: str(item[0]),
                ):
                    print(f"  {status}: {count}")

            print("\nSample entries:")
            for row in rows[:3]:
                print(row)

    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()

