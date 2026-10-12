
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi import HTTPException, Query
from src.gold.clickhouse_loader import get_client
from src.gold.clickhouse_loader import get_client
from src.gold.analytics import (
    overview_kpis,
    ageing_analysis,
    plant_analysis,
)
from src.api.live_metrics import get_live_metrics
from typing import Literal
from src.gold.analytics import get_materials, get_plant_filters
import sqlite3
from pathlib import Path
from datetime import datetime
from src.api.recent_events import get_recent_events

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.clickhouse = get_client()
    try:
        yield
    finally:
        app.state.clickhouse.close()


app = FastAPI(
    title="TE Inventory Intelligence API",
    description="Blocked stock analytics and monitoring",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
def root():
    return {
        "application": "TE Inventory Intelligence",
        "status": "running",
    }


@app.get("/api/health")
def health():
    try:
        app.state.clickhouse.query("SELECT 1")
        return {"status": "healthy", "database": "ClickHouse"}
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="ClickHouse unavailable",
        )


@app.get("/api/overview")
def overview(
    source: str = Query(
        default="historical",
        pattern="^(historical|live)$",
    )
):
    table = f"blocked_stock_{source}"
    return overview_kpis(app.state.clickhouse, source)


@app.get("/api/ageing")
def ageing(
    source: str = Query(
        default="historical",
        pattern="^(historical|live)$",
    )
):
    table = f"blocked_stock_{source}"
    return ageing_analysis(app.state.clickhouse, source)


@app.get("/api/plants")
def plants(
    source: str = Query(
        default="historical",
        pattern="^(historical|live)$",
    )
):
    table = f"blocked_stock_{source}"
    return plant_analysis(app.state.clickhouse, source)


@app.get("/api/materials")
def materials(
    source: Literal["historical", "live"] = "historical",
    plant: str | None = None,
    search: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    sort_by: str = "blocked_value",
    sort_desc: bool = True,
):
    try:
        return get_materials(
            app.state.clickhouse,
            source=source,
            plant=plant,
            search=search,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_desc=sort_desc,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/filters/plants")
def material_plant_filters(
    source: Literal["historical", "live"] = "historical",
):
    return {
        "plants": get_plant_filters(
            app.state.clickhouse, source
        )
    }


@app.get("/api/live/metrics")
def live_metrics():
    try:
        return get_live_metrics()

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Live metrics unavailable: {type(exc).__name__}",
        ) from exc


@app.get("/api/live/history")
def live_history(limit: int = Query(60, ge=2, le=500)):
    db_path = Path("data/processed/streaming_metrics.db")

    if not db_path.exists():
        raise HTTPException(
            status_code=404,
            detail="No streaming history available",
        )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row

        rows = conn.execute("""
            SELECT
                timestamp,
                published_messages,
                committed_messages,
                consumer_lag,
                cassandra_records
            FROM streaming_metrics
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)).fetchall()

    # Return observations in chronological order.
    records = [dict(row) for row in reversed(rows)]

    previous = None

    for record in records:
        if previous is None:
            record["publish_rate"] = 0.0
            record["commit_rate"] = 0.0
        else:
            current_time = datetime.fromisoformat(
                record["timestamp"]
            )
            previous_time = datetime.fromisoformat(
                previous["timestamp"]
            )

            seconds = (
                current_time - previous_time
            ).total_seconds()

            if seconds > 0:
                record["publish_rate"] = round(
                    max(
                        0,
                        record["published_messages"]
                        - previous["published_messages"],
                    ) / seconds,
                    2,
                )

                record["commit_rate"] = round(
                    max(
                        0,
                        record["committed_messages"]
                        - previous["committed_messages"],
                    ) / seconds,
                    2,
                )
            else:
                record["publish_rate"] = 0.0
                record["commit_rate"] = 0.0

        previous = record

    return {"records": records}


@app.get("/api/live/recent-events")
def recent_events(limit: int = Query(20, ge=1, le=100)):
    try:
        return {
            "records": get_recent_events(limit)
        }
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Recent events unavailable: {type(exc).__name__}",
        ) from exc


@app.get("/api/material-types")
def get_material_types(source: str = "historical"):
    if source not in ("historical", "live"):
        raise HTTPException(400, "Invalid source")

    table = (
        "inventory_v2.blocked_stock_historical"
        if source == "historical"
        else "inventory_v2.blocked_stock_live_updates"
    )

    client = get_client()
    try:
        final = " FINAL" if source == "live" else ""

        result = client.query(f"""
            SELECT
                material_type,
                count() AS records,
                round(sum(blocked_value), 2)
                    AS blocked_value_eur
            FROM {table}{final}
            WHERE material_type IS NOT NULL
            GROUP BY material_type
            ORDER BY blocked_value_eur DESC
        """)

        records = [
            {
                "material_type": row[0],
                "records": row[1],
                "blocked_value_eur": row[2],
            }
            for row in result.result_rows
        ]

        return {"records": records}
    finally:
        client.close()


@app.get("/api/top-materials")
def get_top_materials(
    source: str = "historical",
    limit: int = Query(default=10, ge=1, le=50),
):
    if source not in ("historical", "live"):
        raise HTTPException(400, "Invalid source")

    table = (
        "inventory_v2.blocked_stock_historical"
        if source == "historical"
        else "inventory_v2.blocked_stock_live_updates"
    )

    client = get_client()
    try:
        final = " FINAL" if source == "live" else ""

        result = client.query(f"""
            SELECT
                material,
                round(sum(blocked_value), 2)
                    AS blocked_value_eur,
                count() AS blocked_lines,
                uniqExact(plant) AS affected_plants
            FROM {table}{final}
            GROUP BY material
            ORDER BY blocked_value_eur DESC
            LIMIT {limit}
        """)

        records = [
            {
                "material": row[0],
                "blocked_value_eur": row[1],
                "blocked_lines": row[2],
                "affected_plants": row[3],
            }
            for row in result.result_rows
        ]

        return {"records": records}
    finally:
        client.close()
