
import pandas as pd
import pytest

from src.silver.transformations import (
    normalize_column_name,
    transform_bronze,
)


def sample_data():
    return pd.DataFrame([
        {
            "Plant": "1002",
            "Material": "956339-2",
            "Batch": "6000018639",
            "Blocked": 100,
            "Value": 5000,
            "Currency": "EUR",
            "Blocked Since (Days)": 120,
            "Component /FG": "Finish Goods",
        }
    ])


def test_column_normalization():
    assert normalize_column_name(
        "Component /FG"
    ) == "component_fg"


def test_material_classification():
    result = transform_bronze(
        sample_data(), "historical"
    )

    assert result.iloc[0]["material_type"] == "Finished Goods"


@pytest.mark.parametrize(
    "days, expected",
    [
        (15, "0-30 days"),
        (60, "31-90 days"),
        (120, "91-180 days"),
        (200, "180+ days"),
    ],
)
def test_ageing_categories(days, expected):
    data = sample_data()
    data["Blocked Since (Days)"] = days

    result = transform_bronze(data, "historical")

    assert result.iloc[0]["ageing_category"] == expected


def test_invalid_negative_stock():
    data = sample_data()
    data["Blocked"] = -10

    result = transform_bronze(data, "historical")

    assert not bool(result.iloc[0]["is_valid"])


def test_unknown_material_type():
    data = sample_data()
    data["Component /FG"] = "Unknown"

    with pytest.raises(ValueError):
        transform_bronze(data, "historical")


def test_invalid_source():
    with pytest.raises(ValueError):
        transform_bronze(sample_data(), "other")
