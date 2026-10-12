
import os

import pytest
import requests
from dotenv import load_dotenv

load_dotenv(".env")

BASE_URL = os.getenv(
    "TEST_API_BASE_URL",
    "http://127.0.0.1:8081",
)

EXPECTED_RECORDS = 1886
EXPECTED_VALUE = 1158169.31

EXPECTED_TYPES = {
    "Finished Goods": 1171,
    "Purchased Component": 153,
    "Produced Component": 548,
    "Raw Material": 6,
    "Packaging": 8,
}


@pytest.fixture(scope="module")
def session():
    s = requests.Session()

    try:
        response = s.get(
            f"{BASE_URL}/api/overview",
            params={"source": "historical"},
            timeout=15,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        s.close()
        pytest.fail(
            f"FastAPI is unavailable at {BASE_URL}: {exc}"
        )

    yield s
    s.close()


@pytest.mark.parametrize("source", ["historical", "live"])
def test_overview(session, source):
    response = session.get(
        f"{BASE_URL}/api/overview",
        params={"source": source},
        timeout=15,
    )

    assert response.status_code == 200

    data = response.json()

    assert data["total_records"] == EXPECTED_RECORDS
    assert data["unique_materials"] == 547
    assert data["total_plants"] == 6
    assert float(data["blocked_value_eur"]) == pytest.approx(
        EXPECTED_VALUE, abs=0.01
    )


@pytest.mark.parametrize("source", ["historical", "live"])
def test_ageing(session, source):
    response = session.get(
        f"{BASE_URL}/api/ageing",
        params={"source": source},
        timeout=15,
    )

    assert response.status_code == 200
    records = response.json()

    assert len(records) == 4
    assert sum(r["records"] for r in records) == EXPECTED_RECORDS

    assert sum(
        float(r["blocked_value_eur"]) for r in records
    ) == pytest.approx(EXPECTED_VALUE, abs=0.02)


@pytest.mark.parametrize("source", ["historical", "live"])
def test_plants(session, source):
    response = session.get(
        f"{BASE_URL}/api/plants",
        params={"source": source},
        timeout=15,
    )

    assert response.status_code == 200
    records = response.json()

    assert len(records) == 6
    assert sum(r["records"] for r in records) == EXPECTED_RECORDS


@pytest.mark.parametrize("source", ["historical", "live"])
def test_material_types(session, source):
    response = session.get(
        f"{BASE_URL}/api/material-types",
        params={"source": source},
        timeout=15,
    )

    assert response.status_code == 200
    records = response.json()["records"]

    actual = {
        row["material_type"]: row["records"]
        for row in records
    }

    assert actual == EXPECTED_TYPES


@pytest.mark.parametrize("source", ["historical", "live"])
def test_top_materials(session, source):
    response = session.get(
        f"{BASE_URL}/api/top-materials",
        params={"source": source, "limit": 10},
        timeout=15,
    )

    assert response.status_code == 200
    records = response.json()["records"]

    assert len(records) == 10
    assert records[0]["material"] == "956339-2"

    assert float(
        records[0]["blocked_value_eur"]
    ) == pytest.approx(104514.40, abs=0.01)

    values = [
        float(row["blocked_value_eur"])
        for row in records
    ]
    assert values == sorted(values, reverse=True)


@pytest.mark.parametrize("source", ["historical", "live"])
def test_materials_pagination(session, source):
    response = session.get(
        f"{BASE_URL}/api/materials",
        params={
            "source": source,
            "page": 1,
            "page_size": 20,
        },
        timeout=15,
    )

    assert response.status_code == 200
    data = response.json()

    assert data["total"] == EXPECTED_RECORDS
    assert len(data["records"]) == 20


def test_invalid_material_type_source(session):
    response = session.get(
        f"{BASE_URL}/api/material-types",
        params={"source": "invalid"},
        timeout=15,
    )

    assert response.status_code == 400


def test_top_materials_limit_validation(session):
    response = session.get(
        f"{BASE_URL}/api/top-materials",
        params={"source": "historical", "limit": 100},
        timeout=15,
    )

    assert response.status_code == 422
