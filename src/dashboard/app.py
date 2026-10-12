"""Blocked Stock — inventory management dashboard.
API contract remains in src.dashboard.api_client; no backend changes required.
"""
import os
from datetime import datetime
from pathlib import Path
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, dash_table, dcc, html, no_update
from dash.exceptions import PreventUpdate
from src.dashboard.api_client import (
    get_ageing,
    get_live_history,
    get_live_metrics,
    get_materials,
    get_overview,
    get_plant_options,
    get_plants,
    get_recent_events,
    get_material_types,
    get_top_materials,
)
ROOT = Path(__file__).resolve().parents[2]
NAVY = "#182C45"
ORANGE = "#E87524"
MUTED = "#66758B"
SOURCE_OPTIONS = [
    {"label": "Current baseline", "value": "historical"},
    {"label": "Processed records", "value": "live"},
]
AGE_ORDER = ["0-30 days", "31-90 days", "91-180 days", "180+ days"]
app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    assets_folder=str(ROOT / "assets"),
    title="Blocked Stock | TE Connectivity",
)
server = app.server
def metric(title, value, caption=None, accent=False):
    return html.Div(
        className="metric-card" + (" metric-card--accent" if accent else ""),
        children=[
            html.P(title, className="metric-label"),
            html.Div(str(value), className="metric-value"),
            html.P(caption or "", className="metric-caption"),
        ],
    )
def section_header(eyebrow, title, note=None, extra=None):
    return html.Div(
        className="section-header",
        children=[
            html.Div([
                html.P(eyebrow, className="eyebrow") if eyebrow else None,
                html.H2(title, className="section-title"),
                html.P(note, className="section-note") if note else None,
            ]),
            extra,
        ],
    )
def page_header(kicker, title, subtitle, actions=None):
    return html.Div(
        className="page-header",
        children=[
            html.Div([
                html.P(kicker, className="eyebrow") if kicker else None,
                html.H1(title),
                html.P(subtitle, className="page-intro"),
            ]),
            html.Div(actions, className="page-actions") if actions else None,
        ],
    )
def chart_panel(eyebrow, title, chart_id, subtitle):
    return html.Section(
        className="panel chart-panel",
        children=[
            section_header(eyebrow, title, subtitle),
            dcc.Graph(
                id=chart_id,
                config={"displayModeBar": False, "responsive": True},
                className="analytics-graph",
                style={"height": "340px", "width": "100%"},
            ),
        ],
    )
def source_dropdown(component_id, value="historical"):
    return dcc.Dropdown(
        id=component_id,
        options=SOURCE_OPTIONS,
        value=value,
        clearable=False,
        searchable=False,
        className="source-dropdown",
    )
def overview_layout():
    return html.Div(
        id="overview-page", className="page-content", children=[
            page_header(
                None,
                "Blocked stock overview",
                
                [html.Span("VIEW", className="field-label"), source_dropdown("source-filter")],
            ),
            html.Div(id="kpi-container", className="kpi-grid"),
            html.Div(className="charts-grid", children=[
                chart_panel(None, "Ageing distribution", "ageing-chart", "Share of blocked value by ageing bracket"),
                chart_panel(None, "Exposure by plant", "plant-chart", "Ranked by total blocked value"),
            ]),
            html.Div(className="charts-grid charts-grid--details", children=[
                chart_panel(None, "Blocked value by material type", "material-types-chart", "Five categories from the SAP classification"),
                chart_panel(None, "Top 10 materials by blocked value", "top-materials-chart", "Aggregated across batches and plants"),
            ]),
            html.Section(className="panel insights-panel", children=[
                section_header(None, "Key observations", "Concentration and ageing indicators from the selected dataset"),
                html.Div(id="overview-insights", className="insights-grid"),
            ]),
            html.Section(className="panel recent-panel", children=[
                html.Div(className="table-heading", children=[
                    section_header(None, "Largest blocked-stock lines", "Highest individual blocked values; not aggregated by material"),
                ]),
                dash_table.DataTable(
                    id="overview-top-records", columns=[
                        {"name": "Material", "id": "material"},
                        {"name": "Plant", "id": "plant"},
                        {"name": "Batch", "id": "batch"},
                        {"name": "Blocked value · EUR", "id": "blocked_value", "type": "numeric", "format": {"specifier": ",.2f"}},
                        {"name": "Age · days", "id": "blocked_since_days", "type": "numeric"},
                    ], data=[], page_size=8, **TABLE_STYLES,
                ),
            ]),
        ],
    )
MATERIAL_COLUMNS = [
    {"name": "Plant", "id": "plant"},
    {"name": "Material", "id": "material"},
    {"name": "Batch", "id": "batch"},
    {"name": "Blocked qty", "id": "blocked_quantity", "type": "numeric", "format": {"specifier": ",.2f"}},
    {"name": "Blocked value · EUR", "id": "blocked_value", "type": "numeric", "format": {"specifier": ",.2f"}},
    {"name": "Age · days", "id": "blocked_since_days", "type": "numeric"},
    {"name": "Ageing", "id": "ageing_category"},
    {"name": "Category", "id": "stock_category"},
]
TABLE_STYLES = dict(
    style_table={"overflowX": "auto"},
    style_cell={"textAlign": "left", "padding": "12px 14px", "minWidth": "100px", "fontFamily": "inherit", "fontSize": "12px", "border": "none", "borderBottom": "1px solid #EDF0F3"},
    style_header={"backgroundColor": "#F7F8FA", "fontWeight": "600", "color": "#56657A", "border": "none", "borderBottom": "1px solid #E3E8EE", "padding": "13px 14px"},
    style_data={"backgroundColor": "#FFFFFF", "color": "#25354B"},
    style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "#FBFCFD"}],
    css=[{"selector": ".dash-spreadsheet-container .dash-spreadsheet-inner td", "rule": "vertical-align: middle;"}],
)
def analysis_layout():
    return html.Div(
        id="analysis-page", className="page-content", style={"display": "none"}, children=[
            page_header(
                None,
                "Blocked stock analysis",
                "Find and review blocked materials, batches and ageing.",
            ),
            html.Section(className="panel filters-panel", children=[
                section_header(None, "Filters"),
                html.Div(className="filters-card", children=[
                    html.Div(className="field", children=[
                        html.Label("Data view"), source_dropdown("analysis-source"),
                    ]),
                    html.Div(className="field", children=[
                        html.Label("Plant"),
                        dcc.Dropdown(id="plant-filter", placeholder="All plants", clearable=True),
                    ]),
                    html.Div(className="field field--search", children=[
                        html.Label("Material or batch"),
                        dcc.Input(id="material-search", type="text", placeholder="Search material or batch...", debounce=True, className="search-input"),
                    ]),
                ]),
            ]),
            html.Section(className="panel table-panel", children=[
                html.Div(className="table-heading", children=[
                    section_header(None, "Inventory records"),
                    html.Span(id="records-count", className="records-count"),
                ]),
                html.Div(id="materials-error", className="inline-error"),
                dash_table.DataTable(
                    id="materials-table", columns=MATERIAL_COLUMNS, data=[],
                    page_action="custom", page_current=0, page_size=20, page_count=0,
                    sort_action="custom", sort_mode="single", sort_by=[],
                    **TABLE_STYLES,
                ),
            ]),
        ],
    )
RECENT_COLUMNS = [
    {"name": "Processed at · UTC", "id": "received_at"},
    {"name": "Material", "id": "material"},
    {"name": "Plant", "id": "plant"},
    {"name": "Batch", "id": "batch"},
    {"name": "Blocked qty", "id": "blocked_quantity", "type": "numeric", "format": {"specifier": ",.2f"}},
    {"name": "Value · EUR", "id": "blocked_value", "type": "numeric", "format": {"specifier": ",.2f"}},
    {"name": "Record date", "id": "source_date"},
]
def live_layout():
    return html.Div(
        id="live-page", className="page-content", style={"display": "none"}, children=[
            page_header(
                None,
                "Activity",
                "Inventory processing activity and recently received records.",
                [html.Span("Refreshes every 10 seconds", className="refresh-hint")],
            ),
            dcc.Interval(id="live-interval", interval=10_000, n_intervals=0),
            html.Div(id="live-status", className="live-status"),
            html.Div(id="live-kpis", className="kpi-grid"),
            html.Div(className="charts-grid", children=[
                chart_panel(None, "Processing activity", "live-throughput-chart", "Events per second · sampled"),
                chart_panel(None, "Processing backlog", "live-lag-chart", "Events awaiting processing · sampled"),
            ]),
            html.Section(className="panel recent-panel", children=[
                html.Div(className="table-heading", children=[
                    section_header(None, "Recent activity", "Record processing times · UTC"),
                ]),
                html.Div(id="recent-events-error", className="inline-error"),
                dash_table.DataTable(
                    id="recent-events-table", columns=RECENT_COLUMNS, data=[],
                    page_size=10, sort_action="native", **TABLE_STYLES,
                ),
            ]),
            html.P(
                "Activity reflects processing of the 17 August 2026 data extract; "
                "it does not indicate changes received directly from SAP.",
                className="data-disclaimer",
            ),
        ],
    )
app.layout = html.Div(className="app-container", children=[
    dcc.Location(id="url"),
    html.Aside(className="sidebar", children=[
        html.Div(
            className="brand-group",
            children=[
                html.Img(
                    src="/assets/te-connectivity-logo.png",
                    alt="TE Connectivity",
                    className="brand-logo",
                ),
                html.Div("Blocked Stock", className="brand"),
            ],
        ),
        html.Nav(className="nav-list", children=[
            dcc.Link("Overview", href="/", id="nav-overview", className="nav-item active"),
            dcc.Link("Materials", href="/analysis", id="nav-analysis", className="nav-item"),
            dcc.Link("Activity", href="/live", id="nav-live", className="nav-item"),
        ]),
    ]),
    html.Main(className="main-content", children=[
        html.Div(className="topbar", children=[
            html.Span("Blocked Stock", className="topbar-breadcrumb"),
            html.Div(className="topbar-right", children=[
                html.Span("Supply Chain", className="topbar-meta"),
                html.Div(className="user-chip", children=[
                    html.Div("YA", className="user-avatar"),
                    html.Div(className="user-details", children=[
                        html.Div("Yousra Amami", className="user-name"),
                        html.Div("Supply Chain Analyst", className="user-role"),
                    ]),
                ]),
            ]),
        ]),
        overview_layout(), analysis_layout(), live_layout(),
    ]),
])
@app.callback(
    Output("overview-page", "style"), Output("analysis-page", "style"), Output("live-page", "style"),
    Output("nav-overview", "className"), Output("nav-analysis", "className"), Output("nav-live", "className"),
    Input("url", "pathname"),
)
def navigate(pathname):
    analysis = pathname == "/analysis"
    live = pathname == "/live"
    overview = not analysis and not live
    return (
        {"display": "block" if overview else "none"},
        {"display": "block" if analysis else "none"},
        {"display": "block" if live else "none"},
        "nav-item active" if overview else "nav-item",
        "nav-item active" if analysis else "nav-item",
        "nav-item active" if live else "nav-item",
    )
def common_figure(fig, horizontal=False):
    fig.update_layout(
        template="plotly_white", paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF",
        font=dict(family="Segoe UI, Arial, sans-serif", size=11, color=MUTED),
        margin=dict(l=18 if horizontal else 35, r=18, t=16, b=42),
        hoverlabel=dict(bgcolor=NAVY, font_color="white"),
        showlegend=False,
    )
    fig.update_xaxes(showgrid=False, zeroline=False, linecolor="#E4E9EF", tickfont=dict(size=11))
    fig.update_yaxes(showgrid=True, gridcolor="#F0F2F5", zeroline=False, tickfont=dict(size=11))
    return fig

def unavailable_chart(message, height=390):
    fig = go.Figure()
    fig.update_layout(
        template="plotly_white", height=height,
        paper_bgcolor="white", plot_bgcolor="white",
        margin=dict(l=12, r=12, t=12, b=12),
        xaxis=dict(visible=False), yaxis=dict(visible=False),
        annotations=[dict(
            text=message, x=0.5, y=0.5,
            xref="paper", yref="paper", showarrow=False,
            font=dict(size=12, color=MUTED),
        )],
    )
    return fig



@app.callback(
    Output("material-types-chart", "figure"),
    Output("top-materials-chart", "figure"),
    Input("source-filter", "value"),
)
def update_extra_analytics(source):
    try:
        types = get_material_types(source)
        materials = get_top_materials(source, limit=10)

        # Material type distribution
        types = sorted(
            types,
            key=lambda x: float(x["blocked_value_eur"]),
        )

        material_fig = go.Figure()

        if types:
            material_fig.add_trace(go.Bar(
                x=[float(x["blocked_value_eur"]) for x in types],
                y=[x["material_type"] for x in types],
                orientation="h",
                marker_color=[
                    ORANGE if x["material_type"] == "Finished Goods"
                    else NAVY
                    for x in types
                ],
                text=[
                    f"€{float(x['blocked_value_eur']) / 1000:,.1f}k"
                    for x in types
                ],
                textposition="outside",
                cliponaxis=False,
                customdata=[x["records"] for x in types],
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Blocked value: €%{x:,.2f}<br>"
                    "Records: %{customdata}<extra></extra>"
                ),
            ))

        common_figure(material_fig, horizontal=True)
        material_fig.update_layout(
            height=390,
            margin=dict(l=145, r=85, t=10, b=35),
            bargap=0.35,
        )
        material_fig.update_xaxes(
            showgrid=True,
            gridcolor="#EDF0F3",
            tickformat="~s",
        )
        material_fig.update_yaxes(showgrid=False)

        # Top 10 materials
        materials = sorted(
            materials,
            key=lambda x: float(x["blocked_value_eur"]),
        )

        top_fig = go.Figure()

        if materials:
            top_fig.add_trace(go.Bar(
                x=[float(x["blocked_value_eur"]) for x in materials],
                y=[str(x["material"]) for x in materials],
                orientation="h",
                marker_color=[
                    ORANGE if i == len(materials) - 1
                    else NAVY
                    for i in range(len(materials))
                ],
                text=[
                    f"€{float(x['blocked_value_eur']) / 1000:,.1f}k"
                    for x in materials
                ],
                textposition="outside",
                cliponaxis=False,
                customdata=[x["blocked_lines"] for x in materials],
                hovertemplate=(
                    "<b>Material %{y}</b><br>"
                    "Blocked value: €%{x:,.2f}<br>"
                    "Blocked lines: %{customdata}<extra></extra>"
                ),
            ))

        common_figure(top_fig, horizontal=True)
        top_fig.update_layout(
            height=390,
            margin=dict(l=105, r=85, t=10, b=35),
            bargap=0.25,
        )
        top_fig.update_xaxes(
            showgrid=True,
            gridcolor="#EDF0F3",
            tickformat="~s",
        )
        top_fig.update_yaxes(showgrid=False)

        return material_fig, top_fig

    except Exception as exc:
        print(f"Extra analytics error: {type(exc).__name__}: {exc}")

        empty = go.Figure()
        empty.update_layout(
            template="plotly_white",
            height=390,
            paper_bgcolor="white",
            plot_bgcolor="white",
        )

        return empty, go.Figure(empty)


@app.callback(
    Output("kpi-container", "children"),
    Output("ageing-chart", "figure"),
    Output("plant-chart", "figure"),
    Output("overview-insights", "children"),
    Output("overview-top-records", "data"),
    Input("source-filter", "value"),
)
def update_dashboard(source):
    try:
        overview = get_overview(source)
        ageing = get_ageing(source)
        plants = get_plants(source)
        top_records = get_materials(
            source=source, page=1, page_size=8,
            sort_by="blocked_value", sort_desc=True,
        )["records"]
        total_value = float(overview["blocked_value_eur"])
        ageing = sorted(ageing, key=lambda r: AGE_ORDER.index(
            str(r.get("ageing_category"))) if str(r.get("ageing_category")) in AGE_ORDER else 99)
        old_value = sum(float(r["blocked_value_eur"]) for r in ageing
                        if str(r["ageing_category"]) in ("91-180 days", "180+ days"))
        old_percent = 100 * old_value / total_value if total_value else 0
        plants = sorted(plants, key=lambda r: float(r.get("blocked_value_eur") or 0), reverse=True)
        top_plant = plants[0] if plants else None
        top_two = sum(float(p["blocked_value_eur"]) for p in plants[:2])
        top_two_pct = 100 * top_two / total_value if total_value else 0
        cards = [
            metric("Blocked stock value", f"€{total_value:,.2f}", "Total financial exposure", accent=True),
            metric("Blocked stock lines", f"{overview['total_records']:,}", "Individual inventory entries"),
            metric("Distinct materials", f"{overview['unique_materials']:,}", "Affected material codes"),
            metric("Affected plants", f"{overview['total_plants']:,}", "Locations with blocked stock"),
            metric("Exposure over 90 days", f"€{old_value:,.0f}", f"{old_percent:.1f}% of blocked value", accent=True),
            metric("Highest exposure plant", str(top_plant["plant"]) if top_plant else "—",
                   f"€{float(top_plant['blocked_value_eur']):,.0f} blocked" if top_plant else "No records"),
        ]
        # Composition view: distribution of value, not number of records.
        values = [float(r["blocked_value_eur"]) for r in ageing]
        af = go.Figure(go.Pie(
            labels=[r["ageing_category"] for r in ageing],
            values=values,
            hole=0.69,
            sort=False,
            marker=dict(colors=["#23415E", "#647C95", "#E6AC75", "#D76B33"],
                        line=dict(color="white", width=3)),
            textinfo="percent", textposition="inside",
            textfont=dict(size=12, color="white"),
            hovertemplate="%{label}<br>€%{value:,.2f} · %{percent}<extra></extra>",
        ))
        af.update_layout(
            height=330, autosize=True, paper_bgcolor="white",
            font=dict(family="Segoe UI, Arial", size=11, color=MUTED),
            margin=dict(l=10, r=10, t=8, b=20),
            showlegend=True,
            legend=dict(orientation="h", y=-0.03, x=0.5, xanchor="center", font=dict(size=10)),
            annotations=[dict(text=f"{old_percent:.1f}%<br><span style='font-size:11px'>over 90 days</span>",
                              x=0.5, y=0.53, showarrow=False,
                              font=dict(size=22, color=NAVY), align="center")],
        )
        ranked = list(reversed(plants))
        pf = go.Figure(go.Bar(
            y=[str(p["plant"]) for p in ranked],
            x=[float(p["blocked_value_eur"]) for p in ranked],
            orientation="h",
            marker_color=[ORANGE if p in plants[:2] else "#29435D" for p in ranked],
            text=[f"€{float(p['blocked_value_eur'])/1000:,.0f}k" for p in ranked],
            textposition="outside", cliponaxis=False,
            hovertemplate="Plant %{y}<br>€%{x:,.2f}<extra></extra>",
        ))
        common_figure(pf, horizontal=True)
        pf.update_layout(height=330, autosize=True, bargap=0.42,
                         margin=dict(l=36, r=75, t=10, b=25),
                         xaxis_title=None, yaxis_title=None)
        pf.update_xaxes(showgrid=True, gridcolor="#EDF0F3", tickformat="~s")
        pf.update_yaxes(showgrid=False)
        observations = [
            ("Ageing exposure", f"{old_percent:.1f}%",
             f"€{old_value:,.0f} has been blocked for more than 90 days."),
            ("Plant concentration", f"{top_two_pct:.1f}%",
             "Share of value held in the two highest-exposure plants."),
            ("Priority review", str(top_records[0].get("material", "—")) if top_records else "—",
             f"Largest individual blocked line: €{float(top_records[0]['blocked_value']):,.2f}." if top_records else "No records."),
        ]
        insights = [html.Div(className="insight-item", children=[
            html.P(label, className="insight-label"),
            html.Div(value, className="insight-value"),
            html.P(detail, className="insight-description"),
        ]) for label, value, detail in observations]
        return cards, af, pf, insights, top_records
    except Exception as exc:
        import traceback
        traceback.print_exc()
        empty = go.Figure()
        return [html.Div("Overview is currently unavailable.", className="inline-error")], empty, empty, [], []
@app.callback(Output("plant-filter", "options"), Input("analysis-source", "value"))
def update_plant_options(source):
    try:
        return [{"label": str(p), "value": str(p)} for p in get_plant_options(source)]
    except Exception as exc:
        print(f"Plant options error: {exc}")
        return []
@app.callback(
    Output("materials-table", "page_current"),
    Input("analysis-source", "value"), Input("plant-filter", "value"), Input("material-search", "value"),
    prevent_initial_call=True,
)
def reset_material_page(source, plant, search):
    return 0
@app.callback(
    Output("materials-table", "data"),
    Output("materials-table", "page_count"),
    Output("records-count", "children"),
    Output("materials-error", "children"),
    Input("analysis-source", "value"),
    Input("plant-filter", "value"),
    Input("material-search", "value"),
    Input("materials-table", "page_current"),
    Input("materials-table", "page_size"),
    Input("materials-table", "sort_by"),
)
def update_materials(
    source, plant, search, page_current, page_size, sort_by
):
    try:
        allowed_columns = {
            "plant",
            "material",
            "batch",
            "blocked_quantity",
            "blocked_value",
            "blocked_since_days",
            "ageing_category",
            "stock_category",
        }
        sort_column = "blocked_value"
        descending = True
        if sort_by:
            requested = sort_by[0]["column_id"]
            if requested in allowed_columns:
                sort_column = requested
                descending = (
                    sort_by[0]["direction"] == "desc"
                )
        page_size = page_size or 20
        result = get_materials(
            source=source or "historical",
            plant=plant or None,
            search=search or None,
            page=(page_current or 0) + 1,
            page_size=page_size,
            sort_by=sort_column,
            sort_desc=descending,
        )
        records = result["records"]
        total = result["total"]
        pages = max(
            1,
            (total + page_size - 1) // page_size,
        )
        return (
            records,
            pages,
            f"{total:,} records",
            "",
        )
    except Exception as exc:
        print(
            f"Materials callback error: {type(exc).__name__}: {exc}"
        )
        return (
            [],
            1,
            "Unavailable",
            "Unable to load inventory records.",
        )
def time_label(value):
    if not value:
        return "—"
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.strftime("%d %b %Y · %H:%M:%S")
    except (ValueError, TypeError):
        return str(value)
@app.callback(
    Output("live-status", "children"), Output("live-kpis", "children"),
    Output("live-throughput-chart", "figure"),
    Output("live-lag-chart", "figure"),
    Input("live-interval", "n_intervals"), Input("url", "pathname"),
)
def update_live_monitoring(n_intervals, pathname):
    if pathname != "/live":
        raise PreventUpdate
    try:
        metrics = get_live_metrics()
        kafka = metrics["kafka"]
        cassandra = metrics["cassandra"]
        lag = kafka["consumer_lag"]
        status = html.Div(
            [html.Span(className="status-dot"), "Processing up to date" if lag == 0 else f"{lag:,} events pending"],
            className="status-healthy" if lag == 0 else "status-pending",
        )
        cards = [
            metric("Events received", f"{kafka['published_messages']:,}", "Total processed stream inputs", accent=True),
            metric("Events handled", f"{kafka['committed_messages']:,}", "Acknowledged events"),
            metric("Pending events", f"{lag:,}", "Awaiting processing"),
            metric("Inventory records", f"{cassandra['stored_records']:,}", "Unique records in store"),
        ]
    except Exception as exc:
        print(f"Live metrics error: {exc}")
        return (
            html.Div("Live monitoring unavailable", className="status-error"),
            [html.Div("Activity metrics could not be loaded.", className="inline-error")],
            go.Figure(), go.Figure(),
        )
    # Do not erase working metrics if history sampling is temporarily down.
    try:
        history = get_live_history(limit=60)
        timestamps = [row["timestamp"] for row in history]
        throughput = go.Figure()
        throughput.add_trace(go.Scatter(
            x=timestamps, y=[row.get("publish_rate", 0) for row in history],
            mode="lines", name="Received", line=dict(color=ORANGE, width=2.5),
            hovertemplate="%{x}<br>%{y:.2f} msg/s<extra>Published</extra>",
        ))
        throughput.add_trace(go.Scatter(
            x=timestamps, y=[row.get("commit_rate", 0) for row in history],
            mode="lines", name="Handled", line=dict(color=NAVY, width=2),
            hovertemplate="%{x}<br>%{y:.2f} msg/s<extra>Committed</extra>",
        ))
        common_figure(throughput)
        throughput.update_layout(height=320, autosize=True)
        throughput.update_layout(showlegend=True, legend=dict(orientation="h", y=-0.30, x=0), yaxis_title="Messages / second", margin=dict(l=40, r=18, t=15, b=55))
        lag_fig = go.Figure(go.Scatter(
            x=timestamps, y=[row["consumer_lag"] for row in history],
            mode="lines", line=dict(color=ORANGE, width=2.5),
            fill="tozeroy", fillcolor="rgba(232,117,36,0.07)",
            hovertemplate="%{x}<br>%{y} pending<extra></extra>",
        ))
        common_figure(lag_fig)
        lag_fig.update_layout(height=320, autosize=True)
        lag_fig.update_layout(yaxis_title="Pending messages", margin=dict(l=40, r=18, t=15, b=55))
    except Exception as exc:
        print(f"Live history error: {exc}")
        throughput = go.Figure()
        lag_fig = go.Figure()
        common_figure(throughput)
        throughput.update_layout(height=320, autosize=True)
        common_figure(lag_fig)
        lag_fig.update_layout(height=320, autosize=True)
        throughput.add_annotation(text="Streaming history unavailable", x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False)
        lag_fig.add_annotation(text="Streaming history unavailable", x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False)
    return status, cards, throughput, lag_fig
@app.callback(
    Output("recent-events-table", "data"), Output("recent-events-error", "children"),
    Input("live-interval", "n_intervals"), Input("url", "pathname"),
)
def update_recent_events(n_intervals, pathname):
    if pathname != "/live":
        raise PreventUpdate
    try:
        rows = get_recent_events(limit=20)
        return [
            {
                "received_at": time_label(r.get("received_at")),
                "material": r.get("material"), "plant": r.get("plant"),
                "batch": r.get("batch"), "blocked_quantity": r.get("blocked_quantity"),
                "blocked_value": r.get("blocked_value"), "source_date": r.get("source_date"),
            }
            for r in rows
        ], ""
    except Exception as exc:
        print(f"Recent events error: {exc}")
        return [], "Recent events are unavailable. Check the FastAPI endpoint."
if __name__ == "__main__":
    app.run(
    debug=False,
    host=os.getenv("DASH_HOST", "127.0.0.1"),
    port=int(os.getenv("DASH_PORT", "8051")),
)
