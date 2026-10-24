"""Interactive MongoDB-backed flight performance dashboard."""

from calendar import month_abbr
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html

CANCELLATION_REASONS = {
    "A": "Carrier",
    "B": "Weather",
    "C": "National air system",
    "D": "Security",
    "UNKNOWN": "Unknown",
}
SEASON_MONTHS = {
    "winter": [12, 1, 2],
    "spring": [3, 4, 5],
    "summer": [6, 7, 8],
    "autumn": [9, 10, 11],
}
CHART_COLORS = ["#2563eb", "#f97316", "#0f766e", "#7c3aed"]
CARD_STYLE = {
    "background": "white",
    "border": "1px solid #dbe4f0",
    "borderRadius": "12px",
    "boxShadow": "0 4px 16px rgba(15, 23, 42, 0.06)",
    "padding": "16px 18px",
    "minWidth": "180px",
    "flex": "1",
}


def _options(values: list[Any], labels: dict[Any, str] | None = None):
    labels = labels or {}
    return [
        {"label": labels.get(value, str(value)), "value": value} for value in values
    ]


def _documents(
    database: Any,
    collection: str,
    query: dict[str, Any],
    *,
    sort: list[tuple[str, int]] | None = None,
    limit: int = 10_000,
) -> list[dict[str, Any]]:
    cursor = database[collection].find(query, {"_id": 0, "lineage": 0})
    if sort:
        cursor = cursor.sort(sort)
    return list(cursor.limit(limit))


def _safe_rate(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator else None


def _weighted_average(frame: pd.DataFrame, value: str, weight: str) -> float | None:
    valid = frame[value].notna() & frame[weight].gt(0)
    if not valid.any():
        return None
    return float(
        (frame.loc[valid, value] * frame.loc[valid, weight]).sum()
        / frame.loc[valid, weight].sum()
    )


def _period_query(year: int | None, month: int | str | None, season: str | None):
    query: dict[str, Any] = {}
    if year is not None:
        query["report_year"] = year
    if month not in (None, "all"):
        query["report_month"] = int(month)
    elif season:
        query["report_month"] = {"$in": SEASON_MONTHS[season]}
    return query


def _period_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    frame = pd.DataFrame(rows)
    scheduled = int(frame["scheduled_flights"].sum())
    arrival_results = int(frame["arrival_result_flights"].sum())
    delayed = int(frame["delayed_flights"].sum())
    cancelled = int(frame["cancelled_flights"].sum())
    return {
        "scheduled_flights": scheduled,
        "arrival_result_flights": arrival_results,
        "delayed_flights": delayed,
        "cancelled_flights": cancelled,
        "arrival_delay_rate": _safe_rate(delayed, arrival_results),
        "cancellation_rate": _safe_rate(cancelled, scheduled),
        "average_arrival_delay_minutes": _weighted_average(
            frame, "average_arrival_delay_minutes", "arrival_result_flights"
        ),
    }


def _performance_frame(rows: list[dict[str, Any]], key: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    source = pd.DataFrame(rows)
    records = []
    for name, group in source.groupby(key, dropna=False):
        summary = _period_summary(group.to_dict("records"))
        records.append({key: name, **summary})
    return pd.DataFrame(records)


def _airport_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    source = pd.DataFrame(rows)
    records = []
    for airport, group in source.groupby("airport"):
        airport_names = group["airport_name"].dropna()
        arrivals = int(group["scheduled_arrivals"].sum())
        results = int(group["arrival_result_flights"].sum())
        delayed = int(group["delayed_arrivals"].sum())
        cancelled = int(group["cancelled_arrivals"].sum())
        records.append(
            {
                "airport": airport,
                "airport_name": airport_names.iloc[0]
                if not airport_names.empty
                else str(airport),
                "scheduled_arrivals": arrivals,
                "arrival_delay_rate": _safe_rate(delayed, results),
                "arrival_cancellation_rate": _safe_rate(cancelled, arrivals),
            }
        )
    return pd.DataFrame(records)


def _route_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    source = pd.DataFrame(rows)
    records = []
    for (origin, destination), group in source.groupby(["origin", "destination"]):
        records.append(
            {
                "origin": origin,
                "destination": destination,
                "origin_latitude": group["origin_latitude"].dropna().iloc[0]
                if group["origin_latitude"].notna().any()
                else None,
                "origin_longitude": group["origin_longitude"].dropna().iloc[0]
                if group["origin_longitude"].notna().any()
                else None,
                "destination_latitude": group["destination_latitude"].dropna().iloc[0]
                if group["destination_latitude"].notna().any()
                else None,
                "destination_longitude": group["destination_longitude"].dropna().iloc[0]
                if group["destination_longitude"].notna().any()
                else None,
                "scheduled_flights": int(group["scheduled_flights"].sum()),
            }
        )
    return pd.DataFrame(records)


def _cancellation_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    totals = frame.groupby("cancellation_code", as_index=False)[
        "cancelled_flights"
    ].sum()
    totals["cancellation_reason"] = (
        totals["cancellation_code"]
        .map(CANCELLATION_REASONS)
        .fillna("Other / undocumented")
    )
    totals["reason_label"] = totals.apply(
        lambda row: f"{row['cancellation_reason']} ({row['cancellation_code']})", axis=1
    )
    return totals


def _empty_figure(title: str) -> go.Figure:
    figure = go.Figure()
    figure.update_layout(title=title, template="plotly_white")
    figure.add_annotation(
        text="No data for the selected filters",
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
    )
    return figure


def _finish_figure(figure: go.Figure, *, percent_y: bool = False) -> go.Figure:
    figure.update_layout(
        template="plotly_white",
        colorway=CHART_COLORS,
        margin={"l": 52, "r": 24, "t": 72, "b": 50},
        legend_title_text="",
        hovermode="closest",
    )
    if percent_y:
        figure.update_yaxes(tickformat=".0%")
    return figure


def build_dashboard_view(
    database: Any,
    year: int | None,
    month: int | str | None,
    season: str | None,
    carrier: str | None,
    airport: str | None,
    route: str | None,
):
    """Build KPI text and figures from monthly MongoDB Gold rows."""
    query = _period_query(year, month, season)
    month_rows = _documents(
        database, "month_summary", query, sort=[("report_month", 1)]
    )
    summary = _period_summary(month_rows)
    loaded_months = sorted({row["report_month"] for row in month_rows})
    month_names = ", ".join(month_abbr[value] for value in loaded_months) or "none"
    coverage = (
        f"Coverage: {len(loaded_months)} of 12 months loaded ({month_names}). "
        "Whole-year measures use all loaded months."
        if month in (None, "all") and year is not None
        else f"Coverage: {month_names}."
    )

    kpis = (
        f"{summary.get('scheduled_flights', 0):,}",
        "—"
        if summary.get("arrival_delay_rate") is None
        else f"{summary['arrival_delay_rate']:.1%}",
        "—"
        if summary.get("cancellation_rate") is None
        else f"{summary['cancellation_rate']:.1%}",
        "—"
        if summary.get("average_arrival_delay_minutes") is None
        else f"{summary['average_arrival_delay_minutes']:.1f} min",
    )

    carrier_query = dict(query)
    if carrier:
        carrier_query["reporting_airline"] = carrier
    carrier_frame = _performance_frame(
        _documents(database, "carrier_summary", carrier_query), "reporting_airline"
    )
    if carrier_frame.empty:
        carrier_figure = _empty_figure("Carrier reliability")
    else:
        carrier_figure = px.scatter(
            carrier_frame,
            x="arrival_delay_rate",
            y="cancellation_rate",
            size="scheduled_flights",
            color="average_arrival_delay_minutes",
            hover_name="reporting_airline",
            size_max=42,
            color_continuous_scale="RdYlGn_r",
            title="Carrier reliability: lower and left is better",
            labels={
                "arrival_delay_rate": "Arrival delay rate",
                "cancellation_rate": "Cancellation rate",
                "scheduled_flights": "Scheduled flights",
                "average_arrival_delay_minutes": "Average delay (min)",
            },
        )
        carrier_figure.update_xaxes(tickformat=".0%")
        carrier_figure.update_yaxes(tickformat=".0%")
        _finish_figure(carrier_figure)

    trend_query = {
        "report_year": {
            "$in": sorted(database["month_summary"].distinct("report_year"))
        }
    }
    trend_rows = _documents(
        database,
        "month_summary",
        trend_query,
        sort=[("report_year", 1), ("report_month", 1)],
    )
    if trend_rows:
        trend_frame = pd.DataFrame(trend_rows)
        trend_frame["month_name"] = trend_frame["report_month"].map(
            lambda value: month_abbr[value]
        )
        trend_frame["year"] = trend_frame["report_year"].astype(str)
        trend_figure = px.line(
            trend_frame,
            x="month_name",
            y="arrival_delay_rate",
            color="year",
            markers=True,
            category_orders={"month_name": list(month_abbr)[1:]},
            title="Monthly arrival-delay rate by year",
            labels={"month_name": "Month", "arrival_delay_rate": "Arrival delay rate"},
        )
        _finish_figure(trend_figure, percent_y=True)
    else:
        trend_figure = _empty_figure("Monthly arrival-delay rate by year")

    cancellation_frame = _cancellation_frame(
        _documents(database, "cancellation_summary", query)
    )
    if cancellation_frame.empty:
        cancellation_figure = _empty_figure("Why flights were cancelled")
    else:
        cancellation_figure = px.pie(
            cancellation_frame,
            names="reason_label",
            values="cancelled_flights",
            hole=0.55,
            title="Why flights were cancelled",
            color_discrete_sequence=CHART_COLORS,
        )
        cancellation_figure.update_traces(textinfo="percent+label")
        _finish_figure(cancellation_figure)

    airport_query = dict(query)
    if airport:
        airport_query["airport"] = airport
    airport_frame = _airport_frame(
        _documents(database, "airport_summary", airport_query)
    )
    if airport_frame.empty:
        airport_figure = _empty_figure("Airport reliability and traffic")
    else:
        airport_frame = airport_frame.nlargest(40, "scheduled_arrivals")
        airport_figure = px.scatter(
            airport_frame,
            x="arrival_delay_rate",
            y="arrival_cancellation_rate",
            size="scheduled_arrivals",
            hover_name="airport",
            hover_data={"airport_name": True, "scheduled_arrivals": ":,"},
            size_max=45,
            title="Busiest airports: delay and cancellation risk",
            labels={
                "arrival_delay_rate": "Arrival delay rate",
                "arrival_cancellation_rate": "Arrival cancellation rate",
                "scheduled_arrivals": "Scheduled arrivals",
            },
        )
        airport_figure.update_xaxes(tickformat=".0%")
        airport_figure.update_yaxes(tickformat=".0%")
        _finish_figure(airport_figure)

    comparison_rows = _documents(database, "month_summary", {})
    comparisons = []
    for report_year, group in (
        pd.DataFrame(comparison_rows).groupby("report_year") if comparison_rows else []
    ):
        row = _period_summary(group.to_dict("records"))
        comparisons.extend(
            [
                {
                    "year": str(report_year),
                    "measure": "Arrival delay",
                    "rate": row["arrival_delay_rate"],
                },
                {
                    "year": str(report_year),
                    "measure": "Cancellation",
                    "rate": row["cancellation_rate"],
                },
            ]
        )
    if comparisons:
        comparison_figure = px.bar(
            pd.DataFrame(comparisons),
            x="year",
            y="rate",
            color="measure",
            barmode="group",
            title="All loaded months: yearly rate comparison",
            labels={"year": "Year", "rate": "Rate"},
        )
        _finish_figure(comparison_figure, percent_y=True)
    else:
        comparison_figure = _empty_figure("Yearly rate comparison")

    route_query = dict(query)
    if route:
        origin, destination = route.split("|", maxsplit=1)
        route_query.update({"origin": origin, "destination": destination})
    elif airport:
        route_query["$or"] = [{"origin": airport}, {"destination": airport}]
    route_frame = _route_frame(
        _documents(database, "route_summary", route_query, limit=100_000)
    )
    route_rows = (
        []
        if route_frame.empty
        else route_frame.nlargest(100, "scheduled_flights").to_dict("records")
    )
    map_figure = go.Figure()
    for row in route_rows:
        coordinates = (
            row.get("origin_latitude"),
            row.get("origin_longitude"),
            row.get("destination_latitude"),
            row.get("destination_longitude"),
        )
        if any(value is None for value in coordinates):
            continue
        map_figure.add_trace(
            go.Scattergeo(
                lat=[coordinates[0], coordinates[2]],
                lon=[coordinates[1], coordinates[3]],
                mode="lines",
                line={
                    "width": 1 + min(row.get("scheduled_flights", 0) / 500, 4),
                    "color": "#2563eb",
                },
                opacity=0.3,
                hoverinfo="text",
                text=f"{row['origin']} → {row['destination']}: {row.get('scheduled_flights', 0):,} flights",
                showlegend=False,
            )
        )
    map_figure.update_layout(
        title="100 busiest routes for the selected period",
        geo={
            "scope": "usa",
            "projection_type": "albers usa",
            "showland": True,
            "landcolor": "#eef2f7",
        },
        template="plotly_white",
        margin={"l": 20, "r": 20, "t": 70, "b": 20},
    )
    return (
        *kpis,
        coverage,
        carrier_figure,
        trend_figure,
        cancellation_figure,
        airport_figure,
        comparison_figure,
        map_figure,
    )


def create_dashboard_app(database: Any) -> Dash:
    """Create the dashboard with MongoDB as its only runtime data source."""
    years = sorted(database["month_summary"].distinct("report_year"))
    months = sorted(database["month_summary"].distinct("report_month"))
    seasons = sorted(database["season_summary"].distinct("season"))
    carriers = sorted(database["carrier_summary"].distinct("reporting_airline"))
    airports = sorted(database["airport_summary"].distinct("airport"))
    routes = sorted(
        f"{row['origin']}|{row['destination']}"
        for row in database["route_summary"].find(
            {}, {"_id": 0, "origin": 1, "destination": 1}
        )
    )
    latest_year = max(years) if years else None

    app = Dash(__name__)
    control_style = {"minWidth": "170px", "flex": "1"}
    app.layout = html.Div(
        [
            html.Div(
                [
                    html.H1("US Flight Performance", style={"marginBottom": "4px"}),
                    html.P(
                        "Delays, cancellations, airports, and routes with explicit denominators.",
                        style={"color": "#475569", "marginTop": 0},
                    ),
                ],
                style={"marginBottom": "20px"},
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.Label("Year"),
                            dcc.Dropdown(
                                _options(years),
                                value=latest_year,
                                id="year",
                                clearable=False,
                            ),
                        ],
                        style=control_style,
                    ),
                    html.Div(
                        [
                            html.Label("Period"),
                            dcc.Dropdown(
                                [
                                    {
                                        "label": "Whole year / all loaded months",
                                        "value": "all",
                                    },
                                    *_options(
                                        months,
                                        {value: month_abbr[value] for value in months},
                                    ),
                                ],
                                value="all",
                                id="month",
                                clearable=False,
                            ),
                        ],
                        style=control_style,
                    ),
                    html.Div(
                        [
                            html.Label("Season"),
                            dcc.Dropdown(
                                _options(seasons),
                                id="season",
                                placeholder="All seasons",
                            ),
                        ],
                        style=control_style,
                    ),
                    html.Div(
                        [
                            html.Label("Carrier"),
                            dcc.Dropdown(
                                _options(carriers),
                                id="carrier",
                                placeholder="All carriers",
                            ),
                        ],
                        style=control_style,
                    ),
                    html.Div(
                        [
                            html.Label("Airport"),
                            dcc.Dropdown(
                                _options(airports),
                                id="airport",
                                placeholder="All airports",
                            ),
                        ],
                        style=control_style,
                    ),
                    html.Div(
                        [
                            html.Label("Route"),
                            dcc.Dropdown(
                                _options(routes), id="route", placeholder="All routes"
                            ),
                        ],
                        style=control_style,
                    ),
                ],
                style={
                    "display": "flex",
                    "gap": "12px",
                    "flexWrap": "wrap",
                    "background": "white",
                    "padding": "18px",
                    "borderRadius": "12px",
                    "border": "1px solid #dbe4f0",
                },
            ),
            html.P(
                id="coverage",
                style={
                    "background": "#eff6ff",
                    "borderLeft": "4px solid #2563eb",
                    "padding": "10px 14px",
                    "color": "#1e3a8a",
                },
            ),
            html.Div(
                [
                    html.Div(
                        [html.Small("Scheduled flights"), html.H2(id="scheduled-kpi")],
                        style=CARD_STYLE,
                    ),
                    html.Div(
                        [
                            html.Small("Arrival delay rate"),
                            html.H2(id="delay-kpi"),
                            html.Small("Delayed ≥15 min / valid arrivals"),
                        ],
                        style=CARD_STYLE,
                    ),
                    html.Div(
                        [
                            html.Small("Cancellation rate"),
                            html.H2(id="cancellation-kpi"),
                            html.Small("Cancelled / scheduled"),
                        ],
                        style=CARD_STYLE,
                    ),
                    html.Div(
                        [
                            html.Small("Average arrival delay"),
                            html.H2(id="average-delay-kpi"),
                            html.Small("Valid arrival results only"),
                        ],
                        style=CARD_STYLE,
                    ),
                ],
                style={
                    "display": "flex",
                    "gap": "12px",
                    "flexWrap": "wrap",
                    "marginBottom": "12px",
                },
            ),
            html.Div(
                [
                    dcc.Graph(id="carrier-chart"),
                    dcc.Graph(id="trend-chart"),
                    dcc.Graph(id="cancellation-chart"),
                    dcc.Graph(id="airport-chart"),
                    dcc.Graph(id="comparison-chart"),
                    dcc.Graph(id="network-map"),
                ],
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(auto-fit, minmax(520px, 1fr))",
                    "gap": "12px",
                },
            ),
        ],
        style={
            "fontFamily": "Arial, sans-serif",
            "maxWidth": "1600px",
            "margin": "0 auto",
            "padding": "24px",
            "background": "#f8fafc",
            "color": "#0f172a",
        },
    )

    @app.callback(
        Output("scheduled-kpi", "children"),
        Output("delay-kpi", "children"),
        Output("cancellation-kpi", "children"),
        Output("average-delay-kpi", "children"),
        Output("coverage", "children"),
        Output("carrier-chart", "figure"),
        Output("trend-chart", "figure"),
        Output("cancellation-chart", "figure"),
        Output("airport-chart", "figure"),
        Output("comparison-chart", "figure"),
        Output("network-map", "figure"),
        Input("year", "value"),
        Input("month", "value"),
        Input("season", "value"),
        Input("carrier", "value"),
        Input("airport", "value"),
        Input("route", "value"),
    )
    def update_dashboard(year, month, season, carrier, airport, route):
        return build_dashboard_view(
            database, year, month, season, carrier, airport, route
        )

    return app
