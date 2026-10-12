
import os

import requests
from dotenv import load_dotenv

load_dotenv(".env")

API_URL = os.getenv(
    "API_BASE_URL",
    "http://127.0.0.1:8080",
)


def fetch_data(endpoint, source="historical"):
    response = requests.get(
        f"{API_URL}{endpoint}",
        params={"source": source},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def get_overview(source="historical"):
    return fetch_data("/api/overview", source)


def get_ageing(source="historical"):
    return fetch_data("/api/ageing", source)


def get_plants(source="historical"):
    return fetch_data("/api/plants", source)


def get_materials(
    source="historical",
    plant=None,
    search=None,
    page=1,
    page_size=20,
    sort_by="blocked_value",
    sort_desc=True,
):
    response = requests.get(
        f"{API_URL}/api/materials",
        params={
            "source": source,
            "plant": plant,
            "search": search,
            "page": page,
            "page_size": page_size,
            "sort_by": sort_by,
            "sort_desc": sort_desc,
        },
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def get_plant_options(source="historical"):
    response = requests.get(
        f"{API_URL}/api/filters/plants",
        params={"source": source},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()["plants"]

def get_live_metrics():
    response = requests.get(
        f"{API_URL}/api/live/metrics",
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def get_live_history(limit=60):
    response = requests.get(
        f"{API_URL}/api/live/history",
        params={"limit": limit},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()["records"]


def get_recent_events(limit=20):
    response = requests.get(
        f"{API_URL}/api/live/recent-events",
        params={"limit": limit},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()["records"]


def get_material_types(source="historical"):
    """Fetch blocked value grouped by material classification."""
    return fetch_data("/api/material-types", source)["records"]


def get_top_materials(source="historical", limit=10):
    """Fetch materials ranked by total blocked value."""
    response = requests.get(
        f"{API_URL}/api/top-materials",
        params={
            "source": source,
            "limit": limit,
        },
        timeout=15,
    )
    response.raise_for_status()
    return response.json()["records"]
