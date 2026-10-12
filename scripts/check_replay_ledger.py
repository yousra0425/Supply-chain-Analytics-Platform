
from cassandra.cluster import Cluster
from cassandra.io.asyncorereactor import AsyncoreConnection

EVENT_ID = (
    "5910f5397bea63e9b91de8c6c0ce3a9c5bc66a3c7fe3cc886ac04575c5392b94"
)

MESSAGE_ID = "inventory.v2.raw:0:3935"

cluster = Cluster(
    ["127.0.0.1"],
    port=9042,
    connection_class=AsyncoreConnection,
    connect_timeout=15,
    control_connection_timeout=15,
)

try:
    session = cluster.connect("inventory_v2")

    rows = list(session.execute("""
        SELECT message_id, event_id, status, processed_at
        FROM kafka_processing_ledger
    """))

    matching_event = [
        row for row in rows
        if row.event_id == EVENT_ID
    ]

    matching_offset = [
        row for row in rows
        if row.message_id == MESSAGE_ID
    ]

    print("\n=== REPLAY PROCESSING HISTORY ===")
    print("Total ledger entries:", len(rows))
    print("Entries for event:", len(matching_event))
    print("Entries for offset 3935:", len(matching_offset))

    for row in matching_event:
        print(
            "Message:", row.message_id,
            "| Status:", row.status,
            "| Processed:", row.processed_at,
        )

    if not matching_offset:
        print("\nOffset 3935 has no processing ledger entry.")

finally:
    cluster.shutdown()
