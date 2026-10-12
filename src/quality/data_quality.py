
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from cassandra.cluster import Cluster
from src.utils.cassandra_connection import CassandraConnection
from src.gold.clickhouse_loader import get_client


ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/incoming/Blocked Stock 17.08.26 - recreated.xlsx"
HISTORICAL = ROOT / "data/historical/blocked_stock_2026_08_17.parquet"
SILVER_HISTORICAL = ROOT / "data/processed/silver_historical.parquet"
SILVER_LIVE = ROOT / "data/processed/silver_live.parquet"
DUCKDB = ROOT / "data/processed/inventory_history.duckdb"
REPORT_DIR = ROOT / "data/quality"

EXPECTED_RECORDS = 1886


class QualityReport:
    def __init__(self):
        self.checks = []

    def check(self, name, condition, details=""):
        passed = bool(condition)

        self.checks.append({
            "check": name,
            "status": "PASS" if passed else "FAIL",
            "details": str(details),
        })

        symbol = "PASS" if passed else "FAIL"
        print(f"[{symbol}] {name}: {details}")

    def error(self, name, exc):
        self.check(name, False, f"{type(exc).__name__}: {exc}")

    def save(self):
        REPORT_DIR.mkdir(parents=True, exist_ok=True)

        passed = sum(
            c["status"] == "PASS" for c in self.checks
        )
        failed = len(self.checks) - passed

        report = {
            "generated_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "source_snapshot_date": "2026-08-17",
            "total_checks": len(self.checks),
            "passed": passed,
            "failed": failed,
            "overall_status": (
                "PASS" if failed == 0 else "FAIL"
            ),
            "checks": self.checks,
        }

        output = REPORT_DIR / "data_quality_report.json"

        output.write_text(
            json.dumps(report, indent=2),
            encoding="utf-8",
        )

        print("\n" + "=" * 45)
        print("DATA QUALITY SUMMARY")
        print("=" * 45)
        print("Passed:", passed)
        print("Failed:", failed)
        print("Report:", output)

        return report


def validate_source(report):
    print("\n=== SOURCE VALIDATION ===")

    if not SOURCE.exists():
        report.check("Excel file exists", False, SOURCE)
        return

    report.check("Excel file exists", True, SOURCE.name)

    df = pd.read_excel(SOURCE, header=2)

    report.check(
        "Excel row count",
        len(df) == EXPECTED_RECORDS,
        f"{len(df)} records",
    )

    report.check(
        "Excel column count",
        len(df.columns) == 62,
        f"{len(df.columns)} columns",
    )

    if HISTORICAL.exists():
        parquet_df = pd.read_parquet(HISTORICAL)

        report.check(
            "Historical Parquet row count",
            len(parquet_df) == len(df),
            f"{len(parquet_df)} records",
        )
    else:
        report.check(
            "Historical Parquet exists",
            False,
            HISTORICAL,
        )


def validate_duckdb(report):
    print("\n=== DUCKDB BRONZE VALIDATION ===")

    if not DUCKDB.exists():
        report.check("DuckDB exists", False, DUCKDB)
        return

    with duckdb.connect(
        str(DUCKDB),
        read_only=True,
    ) as con:
        records = con.execute(
            "SELECT COUNT(*) FROM blocked_stock_raw"
        ).fetchone()[0]

        ingestions = con.execute(
            "SELECT COUNT(*) FROM ingestion_history"
        ).fetchone()[0]

        imported_rows = con.execute(
            "SELECT SUM(row_count) FROM ingestion_history"
        ).fetchone()[0]

    report.check(
        "DuckDB record count",
        records == EXPECTED_RECORDS,
        f"{records} records",
    )

    report.check(
        "DuckDB ingestion history",
        ingestions == 1,
        f"{ingestions} snapshot(s)",
    )

    report.check(
        "DuckDB ingestion row count",
        imported_rows == records,
        f"Ledger: {imported_rows}; Bronze: {records}",
    )


def validate_cassandra(report):
    print("\n=== CASSANDRA BRONZE VALIDATION ===")

    cluster = Cluster(
        ["127.0.0.1"],
        port=9042,
        connection_class=CassandraConnection,        
        connect_timeout=15,
        control_connection_timeout=15,
    )

    try:
        session = cluster.connect("inventory_v2")

        rows = list(session.execute("""
            SELECT event_id
            FROM blocked_stock_events
        """))

        ids = [row.event_id for row in rows]

        report.check(
            "Cassandra record count",
            len(ids) == EXPECTED_RECORDS,
            f"{len(ids)} records",
        )

        report.check(
            "Cassandra unique event IDs",
            len(ids) == len(set(ids)),
            f"{len(set(ids))} unique IDs",
        )

    finally:
        cluster.shutdown()


def validate_silver(report):
    print("\n=== SILVER VALIDATION ===")

    for name, path in (
        ("Historical", SILVER_HISTORICAL),
        ("Live", SILVER_LIVE),
    ):
        if not path.exists():
            report.check(
                f"{name} Silver file exists",
                False,
                path,
            )
            continue

        df = pd.read_parquet(path)

        report.check(
            f"{name} Silver record count",
            len(df) == EXPECTED_RECORDS,
            f"{len(df)} records",
        )

        if "is_valid" in df.columns:
            invalid = (
                ~df["is_valid"].fillna(False).astype(bool)
            ).sum()

            report.check(
                f"{name} Silver valid records",
                invalid == 0,
                f"{invalid} invalid rows",
            )

        if "record_id" in df.columns:
            duplicates = df["record_id"].duplicated().sum()

            report.check(
                f"{name} Silver duplicate IDs",
                duplicates == 0,
                f"{duplicates} duplicates",
            )

        if "material" in df.columns:
            missing = (
                df["material"].isna()
                | df["material"].astype(str).str.strip().eq("")
            ).sum()

            report.check(
                f"{name} Silver material identifiers",
                missing == 0,
                f"{missing} missing identifiers",
            )


def validate_clickhouse(report):
    print("\n=== CLICKHOUSE GOLD VALIDATION ===")

    client = get_client()

    try:
        tables = {
            "Historical": (
                "inventory_v2.blocked_stock_historical"
            ),
            "Live": (
                "inventory_v2.blocked_stock_live_updates FINAL"
            ),
        }

        results = {}

        for name, table in tables.items():
            row = client.query(f"""
                SELECT
                    count(),
                    uniqExact(record_id),
                    round(sum(blocked_value), 2)
                FROM {table}
            """).result_rows[0]

            count, unique, value = row

            results[name] = {
                "count": count,
                "unique": unique,
                "value": float(value),
            }

            report.check(
                f"{name} Gold record count",
                count == EXPECTED_RECORDS,
                f"{count} records",
            )

            report.check(
                f"{name} Gold unique record IDs",
                unique == count,
                f"{unique} unique IDs",
            )

        if len(results) == 2:
            historical = results["Historical"]
            live = results["Live"]

            report.check(
                "Historical/Live Gold row counts",
                historical["count"] == live["count"],
                f"{historical['count']} vs {live['count']}",
            )

            report.check(
                "Historical/Live Gold values",
                abs(
                    historical["value"] - live["value"]
                ) < 0.01,
                (
                    f"€{historical['value']:,.2f} vs "
                    f"€{live['value']:,.2f}"
                ),
            )

            report.check(
                "Expected blocked stock value",
                abs(
                    historical["value"] - 1158169.31
                ) < 0.01,
                f"€{historical['value']:,.2f}",
            )

    finally:
        client.close()


def main():
    report = QualityReport()

    validators = [
        ("Source", validate_source),
        ("DuckDB", validate_duckdb),
        ("Cassandra", validate_cassandra),
        ("Silver", validate_silver),
        ("ClickHouse", validate_clickhouse),
    ]

    for name, validator in validators:
        try:
            validator(report)
        except Exception as exc:
            report.error(f"{name} validation execution", exc)

    result = report.save()

    if result["failed"] > 0:
        raise RuntimeError(
            f"Data quality failed: {result['failed']} checks failed"
        )

    return result


if __name__ == "__main__":
    main()
