
from src.gold.clickhouse_loader import get_client

DATABASE = "inventory_v2"
TABLE = f"{DATABASE}.blocked_stock_historical"
LOOKUP = f"{DATABASE}.material_type_backfill_tmp"

client = get_client()

try:
    # Verify the temporary lookup table
    lookup_count = client.query(
        f"SELECT count() FROM {LOOKUP}"
    ).result_rows[0][0]

    print("Lookup records:", lookup_count)

    if lookup_count != 1886:
        raise RuntimeError("Expected 1886 lookup records")

    # Check that all lookup IDs are unique
    unique_count = client.query(
        f"SELECT uniqExact(record_id) FROM {LOOKUP}"
    ).result_rows[0][0]

    if unique_count != lookup_count:
        raise RuntimeError("Duplicate IDs in lookup table")

    # Confirm each historical record has one lookup entry
    matched = client.query(f"""
        SELECT count()
        FROM {TABLE} AS h
        INNER JOIN {LOOKUP} AS l
            ON h.record_id = l.record_id
    """).result_rows[0][0]

    print("Matched records:", matched)

    if matched != 1886:
        raise RuntimeError("Historical and lookup records do not match")

    # Backfill using an in-memory dictionary of ID -> type
    mapping = client.query(
        f"SELECT record_id, material_type FROM {LOOKUP}"
    ).result_rows

    # Build a ClickHouse map expression.
    # This avoids correlated subqueries and external dictionaries.
    keys = [row[0] for row in mapping]
    values = [row[1] for row in mapping]

    # Quote each string safely for SQL literals
    def sql_quote(text):
        return "'" + str(text).replace("\\", "\\\\").replace("'", "\\'") + "'"

    pairs = ", ".join(
        f"{sql_quote(k)}, {sql_quote(v)}"
        for k, v in zip(keys, values)
    )

    print("Applying classification update...")

    client.command(f"""
        ALTER TABLE {TABLE}
        UPDATE material_type = transform(
            record_id,
            [{", ".join(sql_quote(k) for k in keys)}],
            [{", ".join(sql_quote(v) for v in values)}],
            ''
        )
        WHERE 1
        SETTINGS mutations_sync = 2
    """)

    print("Backfill complete.")

    result = client.query(f"""
        SELECT
            material_type,
            count() AS records,
            round(sum(blocked_value), 2) AS blocked_value
        FROM {TABLE}
        GROUP BY material_type
        ORDER BY blocked_value DESC
    """)

    print("\n=== HISTORICAL GOLD CLASSIFICATION ===")
    for row in result.result_rows:
        print(row)

finally:
    client.close()
