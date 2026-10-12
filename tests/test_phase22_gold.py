
import pytest

from src.gold.clickhouse_loader import get_client


DATABASE = "inventory_v2"

TABLES = {
    "historical": "blocked_stock_historical",
    "live": "blocked_stock_live_updates FINAL",
}

EXPECTED_RECORDS = 1886
EXPECTED_VALUE = 1158169.31

EXPECTED_TYPES = {
    "Finished Goods": (1171, 525624.28),
    "Purchased Component": (153, 312557.99),
    "Produced Component": (548, 229373.51),
    "Raw Material": (6, 89608.77),
    "Packaging": (8, 1004.76),
}


@pytest.fixture(scope="module")
def client():
    connection = get_client()
    yield connection
    connection.close()


def get_summary(client, source):
    table = TABLES[source]

    return client.query(f"""
        SELECT
            count(),
            uniqExact(record_id),
            uniqExact(material),
            uniqExact(plant),
            round(sum(blocked_value), 2),
            countIf(
                material_type IS NULL
                OR material_type = ''
            )
        FROM {DATABASE}.{table}
    """).result_rows[0]


@pytest.mark.parametrize("source", ["historical", "live"])
def test_record_count_and_uniqueness(client, source):
    summary = get_summary(client, source)

    assert summary[0] == EXPECTED_RECORDS
    assert summary[1] == EXPECTED_RECORDS


@pytest.mark.parametrize("source", ["historical", "live"])
def test_inventory_totals(client, source):
    summary = get_summary(client, source)

    assert summary[2] == 547
    assert summary[3] == 6
    assert float(summary[4]) == pytest.approx(
        EXPECTED_VALUE,
        abs=0.01,
    )


@pytest.mark.parametrize("source", ["historical", "live"])
def test_no_missing_material_types(client, source):
    summary = get_summary(client, source)

    assert summary[5] == 0


@pytest.mark.parametrize("source", ["historical", "live"])
def test_material_type_distribution(client, source):
    table = TABLES[source]

    rows = client.query(f"""
        SELECT
            material_type,
            count(),
            round(sum(blocked_value), 2)
        FROM {DATABASE}.{table}
        GROUP BY material_type
    """).result_rows

    actual = {
        row[0]: (int(row[1]), float(row[2]))
        for row in rows
    }

    assert set(actual) == set(EXPECTED_TYPES)

    for category, (count, value) in EXPECTED_TYPES.items():
        actual_count, actual_value = actual[category]

        assert actual_count == count
        assert actual_value == pytest.approx(
            value,
            abs=0.01,
        )


def test_historical_and_live_totals_match(client):
    historical = get_summary(client, "historical")
    live = get_summary(client, "live")

    assert historical[:4] == live[:4]
    assert float(historical[4]) == pytest.approx(
        float(live[4]),
        abs=0.01,
    )
