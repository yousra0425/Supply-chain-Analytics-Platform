
from src.gold.clickhouse_loader import get_client

DATABASE = "inventory_v2"

TABLES = {
    "Historical": "blocked_stock_historical",
    "Live": "blocked_stock_live_updates",
}

client = get_client()

try:
    print("\n=== PHASE 22: BASELINE VALIDATION ===\n")

    for label, table in TABLES.items():
        suffix = " FINAL" if label == "Live" else ""

        result = client.query(f"""
            SELECT
                count() AS records,
                uniqExact(record_id) AS unique_records,
                uniqExact(material) AS materials,
                uniqExact(plant) AS plants,
                round(sum(blocked_value), 2) AS total_value,
                countIf(material_type IS NULL
                    OR material_type = '') AS missing_types
            FROM {DATABASE}.{table}{suffix}
        """)

        row = result.result_rows[0]

        print(f"{label} Gold")
        print(f"  Records:           {row[0]}")
        print(f"  Unique record IDs: {row[1]}")
        print(f"  Materials:         {row[2]}")
        print(f"  Plants:            {row[3]}")
        print(f"  Blocked value:     EUR {row[4]:,.2f}")
        print(f"  Missing types:     {row[5]}")
        print()

    print("=== VALIDATION COMPLETED ===")

finally:
    client.close()
