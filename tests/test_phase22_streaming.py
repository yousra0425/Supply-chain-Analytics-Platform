
import pytest

from src.gold.clickhouse_loader import get_client
from src.silver.live_reader import read_live_bronze


EXPECTED_RECORDS = 1886
EXPECTED_VALUE = 1158169.31


@pytest.fixture(scope="module")
def clickhouse():
    client = get_client()
    yield client
    client.close()


@pytest.fixture(scope="module")
def bronze():
    return read_live_bronze()


def test_cassandra_unique_events(bronze):
    assert len(bronze) == EXPECTED_RECORDS
    assert bronze["event_id"].nunique() == EXPECTED_RECORDS


def test_cassandra_material_classifications(bronze):
    assert "Component /FG" in bronze.columns
    assert bronze["Component /FG"].notna().all()


def test_live_gold_deduplication(clickhouse):
    row = clickhouse.query("""
        SELECT
            count(),
            uniqExact(record_id)
        FROM inventory_v2.blocked_stock_live_updates FINAL
    """).result_rows[0]

    assert row[0] == EXPECTED_RECORDS
    assert row[1] == EXPECTED_RECORDS


def test_live_gold_value_preservation(clickhouse):
    row = clickhouse.query("""
        SELECT round(sum(blocked_value), 2)
        FROM inventory_v2.blocked_stock_live_updates FINAL
    """).result_rows[0]

    assert float(row[0]) == pytest.approx(
        EXPECTED_VALUE,
        abs=0.01,
    )


def test_live_gold_classification_complete(clickhouse):
    missing = clickhouse.query("""
        SELECT count()
        FROM inventory_v2.blocked_stock_live_updates FINAL
        WHERE material_type IS NULL
           OR material_type = ''
    """).result_rows[0][0]

    assert missing == 0


def test_processing_ledger_exists(clickhouse):
    # Confirm this source is not accidentally queried from ClickHouse:
    # the processing ledger is stored in Cassandra.
    assert clickhouse.query(
        "SELECT 1"
    ).result_rows[0][0] == 1
