
from pathlib import Path
import hashlib

import duckdb
import pandas as pd


DB_PATH = Path("data/processed/inventory_history.duckdb")
SNAPSHOT_PATH = Path(
    "data/historical/blocked_stock_2026_08_17.parquet"
)


def calculate_file_hash(file_path: Path) -> str:
    """Identify a snapshot using its contents."""
    return hashlib.sha256(file_path.read_bytes()).hexdigest()


def initialize_database(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_history (
            file_hash VARCHAR PRIMARY KEY,
            filename VARCHAR,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            row_count INTEGER
        )
    """)


def process_snapshot(file_path: Path):
    if not file_path.exists():
        raise FileNotFoundError(file_path)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    file_hash = calculate_file_hash(file_path)

    with duckdb.connect(str(DB_PATH)) as con:
        initialize_database(con)

        existing = con.execute(
            "SELECT 1 FROM ingestion_history WHERE file_hash = ?",
            [file_hash],
        ).fetchone()

        if existing:
            print("Snapshot already processed. Skipping.")
            return

        df = pd.read_parquet(file_path)

        if df.empty:
            raise ValueError("Historical snapshot is empty.")

        df["source_file"] = file_path.name
        df["source_hash"] = file_hash

        con.execute("BEGIN TRANSACTION")

        try:
            # Preserve the original column names in Bronze.
            con.register("incoming_snapshot", df)

            con.execute("""
                CREATE TABLE IF NOT EXISTS blocked_stock_raw AS
                SELECT * FROM incoming_snapshot WHERE 1 = 0
            """)

            con.execute("""
                INSERT INTO blocked_stock_raw
                SELECT * FROM incoming_snapshot
            """)

            con.execute("""
                INSERT INTO ingestion_history
                    (file_hash, filename, row_count)
                VALUES (?, ?, ?)
            """, [file_hash, file_path.name, len(df)])

            con.execute("COMMIT")

        except Exception:
            con.execute("ROLLBACK")
            raise

        finally:
            con.unregister("incoming_snapshot")

        print("Historical snapshot successfully ingested!")
        print(f"File: {file_path.name}")
        print(f"Records: {len(df)}")
        print(f"Database: {DB_PATH}")


if __name__ == "__main__":
    process_snapshot(SNAPSHOT_PATH)
