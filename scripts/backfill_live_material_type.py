
import pandas as pd

from src.gold.clickhouse_loader import get_client


TABLE = "inventory_v2.blocked_stock_live_updates"
SILVER_FILE = "data/processed/silver_live.parquet"
EXPECTED_RECORDS = 1886
EXPECTED_VALUE = 1158169.31


def sql_quote(value):
    text = str(value)
    text = text.replace("\\", "\\\\")
    text = text.replace("'", "\\'")
    return f"'{text}'"


def main():
    print("\n=== LIVE GOLD MATERIAL TYPE BACKFILL ===\n")

    silver = pd.read_parquet(SILVER_FILE)

    required = [
        "event_id",
        "plant",
        "material",
        "value",
        "material_type",
        "is_valid",
    ]

    missing = [
        col for col in required
        if col not in silver.columns
    ]

    if missing:
        raise RuntimeError(f"Missing Silver columns: {missing}")

    if len(silver) != EXPECTED_RECORDS:
        raise RuntimeError("Unexpected Silver record count")

    if not silver["is_valid"].all():
        raise RuntimeError("Invalid Silver records")

    if silver["material_type"].isna().any():
        raise RuntimeError("Missing material classifications")

    if silver["event_id"].duplicated().any():
        raise RuntimeError("Duplicate Silver event IDs")

    lookup = silver[
        ["event_id", "plant", "material", "value", "material_type"]
    ].copy()

    lookup.columns = [
        "record_id",
        "plant_silver",
        "material_silver",
        "value_silver",
        "material_type",
    ]

    lookup["record_id"] = lookup["record_id"].astype(str)
    lookup["plant_silver"] = lookup["plant_silver"].astype(str)
    lookup["material_silver"] = lookup["material_silver"].astype(str)
    lookup["value_silver"] = lookup["value_silver"].astype(float)

    client = get_client()

    try:
        # FINAL returns the deduplicated live view.
        result = client.query(f"""
            SELECT
                record_id,
                plant,
                material,
                blocked_value
            FROM {TABLE} FINAL
        """)

        gold = pd.DataFrame(
            result.result_rows,
            columns=[
                "record_id",
                "plant_gold",
                "material_gold",
                "value_gold",
            ],
        )

        if len(gold) != EXPECTED_RECORDS:
            raise RuntimeError(
                f"Unexpected live Gold count: {len(gold)}"
            )

        if gold["record_id"].duplicated().any():
            raise RuntimeError("Duplicate Gold IDs after FINAL")

        joined = lookup.merge(
            gold,
            on="record_id",
            how="left",
            indicator=True,
            validate="one_to_one",
        )

        matches = (
            (joined["_merge"] == "both")
            & (
                joined["plant_silver"]
                == joined["plant_gold"]
            )
            & (
                joined["material_silver"]
                == joined["material_gold"]
            )
            & (
                (
                    joined["value_silver"]
                    - joined["value_gold"]
                ).abs() < 0.01
            )
        )

        if not matches.all():
            raise RuntimeError(
                "Live Silver and Gold records differ. "
                "Backfill cancelled."
            )

        total = round(float(gold["value_gold"].sum()), 2)

        if abs(total - EXPECTED_VALUE) > 0.01:
            raise RuntimeError(
                f"Unexpected Gold total: {total}"
            )

        print(
            f"Silver/Gold matching: "
            f"{int(matches.sum())}/{EXPECTED_RECORDS}"
        )

        keys = [
            sql_quote(value)
            for value in joined["record_id"]
        ]

        types = [
            sql_quote(value)
            for value in joined["material_type"]
        ]

        # Only update records with a missing classification.
        # No truncation or replay is performed.
        query = f"""
            ALTER TABLE {TABLE}
            UPDATE material_type = transform(
                record_id,
                [{", ".join(keys)}],
                [{", ".join(types)}],
                ''
            )
            WHERE material_type IS NULL
            SETTINGS mutations_sync = 2
        """

        print("Applying live Gold classification backfill...")
        client.command(query)

        result = client.query(f"""
            SELECT
                ifNull(material_type, '(missing)') AS type,
                count() AS records,
                round(sum(blocked_value), 2) AS blocked_value
            FROM {TABLE} FINAL
            GROUP BY type
            ORDER BY blocked_value DESC
        """)

        print("\n=== LIVE GOLD CLASSIFICATION ===")

        for row in result.result_rows:
            print(row)

        remaining = client.query(f"""
            SELECT count()
            FROM {TABLE} FINAL
            WHERE material_type IS NULL
               OR material_type = ''
        """).result_rows[0][0]

        if remaining:
            raise RuntimeError(
                f"{remaining} classifications still missing"
            )

        print("\nLive Gold backfill successful.")

    finally:
        client.close()


if __name__ == "__main__":
    main()
