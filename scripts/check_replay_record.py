
import hashlib

from src.gold.clickhouse_loader import get_client

EVENT_ID = hashlib.sha256(
    b"2026-08-17:0"
).hexdigest()

client = get_client()

try:
    rows = client.query("""
        SELECT
            record_id,
            material,
            plant,
            material_type,
            blocked_value,
            processed_at
        FROM inventory_v2.blocked_stock_live_updates FINAL
        WHERE record_id = %(event_id)s
    """, parameters={"event_id": EVENT_ID}).result_rows

    print("\n=== REPLAY TARGET ===")
    print("Event ID:", EVENT_ID)
    print("Existing Gold records:", len(rows))

    for row in rows:
        print("Material:", row[1])
        print("Plant:", row[2])
        print("Material type:", row[3])
        print("Blocked value:", row[4])
        print("Last processed:", row[5])

    if len(rows) != 1:
        raise RuntimeError(
            "Expected exactly one existing Gold record"
        )

    if not rows[0][3]:
        raise RuntimeError(
            "Existing Gold classification is missing"
        )

finally:
    client.close()
