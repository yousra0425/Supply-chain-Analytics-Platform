
from src.gold.clickhouse_loader import get_client


DATABASE = "inventory_v2"


def resolve_table(source):
    tables = {
        "historical": "blocked_stock_historical",
        "live": "blocked_stock_live_updates FINAL",
    }

    if source not in tables:
        raise ValueError("Invalid data source")

    return f"{DATABASE}.{tables[source]}"


def overview_kpis(client, source="historical"):
    table = resolve_table(source)

    query = f"""
        SELECT
            count() AS total_records,
            uniqExact(material) AS unique_materials,
            uniqExact(plant) AS total_plants,
            round(sum(blocked_value), 2) AS blocked_value_eur
        FROM {table}
    """

    result = client.query(query)
    return dict(zip(result.column_names, result.result_rows[0]))


def ageing_analysis(client, source="historical"):
    table = resolve_table(source)

    query = f"""
        SELECT
            ageing_category,
            count() AS records,
            round(sum(blocked_value), 2) AS blocked_value_eur
        FROM {table}
        GROUP BY ageing_category
        ORDER BY
            multiIf(
                ageing_category = '0-30 days', 1,
                ageing_category = '31-90 days', 2,
                ageing_category = '91-180 days', 3,
                4
            )
    """

    result = client.query(query)

    return [
        dict(zip(result.column_names, row))
        for row in result.result_rows
    ]


def plant_analysis(client, source="historical"):
    table = resolve_table(source)

    query = f"""
        SELECT
            plant,
            count() AS records,
            uniqExact(material) AS unique_materials,
            round(sum(blocked_value), 2) AS blocked_value_eur
        FROM {table}
        GROUP BY plant
        ORDER BY blocked_value_eur DESC
    """

    result = client.query(query)

    return [
        dict(zip(result.column_names, row))
        for row in result.result_rows
    ]


def get_materials(
    client,
    source="historical",
    plant=None,
    search=None,
    page=1,
    page_size=20,
    sort_by="blocked_value",
    sort_desc=True,
):
    table = resolve_table(source)

    allowed_sort = {
        "material",
        "plant",
        "blocked_quantity",
        "blocked_value",
        "blocked_since_days",
        "ageing_category",
    }

    if sort_by not in allowed_sort:
        raise ValueError("Invalid sort column")

    if page < 1 or page_size < 1 or page_size > 100:
        raise ValueError("Invalid pagination parameters")

    conditions = []
    params = {}

    if plant:
        conditions.append("plant = {plant:String}")
        params["plant"] = plant

    if search:
        conditions.append(
            "(positionCaseInsensitive(material, {search:String}) > 0 "
            "OR positionCaseInsensitive(ifNull(batch, ''), "
            "{search:String}) > 0)"
        )
        params["search"] = search

    where = " AND ".join(conditions) if conditions else "1 = 1"

    count_query = f"""
        SELECT count()
        FROM {table}
        WHERE {where}
    """

    total = client.query(
        count_query,
        parameters=params,
    ).result_rows[0][0]

    direction = "DESC" if sort_desc else "ASC"
    offset = (page - 1) * page_size

    query = f"""
        SELECT
            record_id,
            plant,
            material,
            batch,
            blocked_quantity,
            blocked_value,
            currency,
            blocked_since_days,
            ageing_category,
            stock_category
        FROM {table}
        WHERE {where}
        ORDER BY {sort_by} {direction}, record_id ASC
        LIMIT {page_size}
        OFFSET {offset}
    """

    result = client.query(
        query,
        parameters=params,
    )

    records = [
        dict(zip(result.column_names, row))
        for row in result.result_rows
    ]

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "records": records,
    }


def get_plant_filters(client, source="historical"):
    table = resolve_table(source)

    result = client.query(f"""
        SELECT DISTINCT plant
        FROM {table}
        ORDER BY plant
    """)

    return [row[0] for row in result.result_rows]


def main():
    client = get_client()

    try:
        for source in ["historical", "live"]:
            print(f"\n===== {source.upper()} =====")
            print("Table:", resolve_table(source))

            print("\nOVERVIEW")
            print(overview_kpis(client, source))

            print("\nAGEING ANALYSIS")
            for row in ageing_analysis(client, source):
                print(row)

            print("\nPLANT ANALYSIS")
            for row in plant_analysis(client, source):
                print(row)

            print("\nMATERIALS")
            materials = get_materials(
                client,
                source=source,
                page=1,
                page_size=5,
            )
            print("Total:", materials["total"])
            print("Returned:", len(materials["records"]))

            print("\nPLANT FILTERS")
            print(get_plant_filters(client, source))

    finally:
        client.close()


if __name__ == "__main__":
    main()
