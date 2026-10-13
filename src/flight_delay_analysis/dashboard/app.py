"""Interactive MongoDB-backed flight performance dashboard."""

from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html


def _options(values: list[Any]) -> list[dict[str, Any]]:
    return [{"label": str(value), "value": value} for value in values]


def _documents(
    database: Any,
    collection: str,
    query: dict[str, Any],
    *,
    sort: list[tuple[str, int]] | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    cursor = database[collection].find(query, {"_id": 0, "lineage": 0})
    if sort:
        cursor = cursor.sort(sort)
    return list(cursor.limit(limit))


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

    app = Dash(__name__)
    control_style = {"minWidth": "160px", "flex": "1"}
    app.layout = html.Div(
        [
            html.H1("US Flight Delay Analysis"),
            html.P(
                "Rates use explicit applicable-flight denominators. "
                "Cancelled flights are not treated as on-time arrivals."
            ),
            html.Div(
                [
                    html.Div(
                        [html.Label("Year"), dcc.Dropdown(_options(years), id="year")],
                        style=control_style,
                    ),
                    html.Div(
                        [
                            html.Label("Month"),
                            dcc.Dropdown(_options(months), id="month"),
                        ],
                        style=control_style,
                    ),
                    html.Div(
                        [
                            html.Label("Season"),
                            dcc.Dropdown(_options(seasons), id="season"),
                        ],
                        style=control_style,
                    ),
                    html.Div(
                        [
                            html.Label("Carrier"),
                            dcc.Dropdown(_options(carriers), id="carrier"),
                        ],
                        style=control_style,
                    ),
                    html.Div(
                        [
                            html.Label("Airport"),
                            dcc.Dropdown(_options(airports), id="airport"),
                        ],
                        style=control_style,
                    ),
                    html.Div(
                        [html.Label("Route"), dcc.Dropdown(_options(routes), id="route")],
                        style=control_style,
                    ),
                ],
                style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
            ),
            html.Div(
                [
                    dcc.Graph(id="carrier-chart"),
                    dcc.Graph(id="trend-chart"),
                    dcc.Graph(id="cancellation-chart"),
                    dcc.Graph(id="score-chart"),
                    dcc.Graph(id="network-map"),
                ]
            ),
        ],
        style={
            "fontFamily": "Arial, sans-serif",
            "maxWidth": "1500px",
            "margin": "0 auto",
            "padding": "24px",
        },
    )

    @app.callback(
        Output("carrier-chart", "figure"),
        Output("trend-chart", "figure"),
        Output("cancellation-chart", "figure"),
        Output("score-chart", "figure"),
        Output("network-map", "figure"),
        Input("year", "value"),
        Input("month", "value"),
        Input("season", "value"),
        Input("carrier", "value"),
        Input("airport", "value"),
        Input("route", "value"),
    )
    def update_figures(year, month, season, carrier, airport, route):
        period_query = {}
        if year is not None:
            period_query["report_year"] = year
        if month is not None:
            period_query["report_month"] = month

        carrier_query = dict(period_query)
        if carrier:
            carrier_query["reporting_airline"] = carrier
        carrier_rows = _documents(database, "carrier_summary", carrier_query)
        if carrier_rows:
            carrier_frame = pd.DataFrame(carrier_rows)
            carrier_figure = px.bar(
                carrier_frame,
                x="reporting_airline",
                y=["arrival_delay_rate", "cancellation_rate"],
                barmode="group",
                title="Carrier delay and cancellation rates",
                labels={"value": "Rate", "reporting_airline": "Carrier"},
            )
        else:
            carrier_figure = _empty_figure("Carrier performance")

        trend_query = {"report_year": year} if year is not None else {}
        trend_rows = _documents(
            database,
            "month_summary",
            trend_query,
            sort=[("report_year", 1), ("report_month", 1)],
        )
        if trend_rows:
            trend_frame = pd.DataFrame(trend_rows)
            trend_frame["period"] = (
                trend_frame["report_year"].astype(str)
                + "-"
                + trend_frame["report_month"].astype(str).str.zfill(2)
            )
            trend_figure = px.line(
                trend_frame,
                x="period",
                y=["arrival_delay_rate", "cancellation_rate"],
                markers=True,
                title="Monthly performance trend",
                labels={"value": "Rate", "period": "Month"},
            )
        else:
            trend_figure = _empty_figure("Monthly performance trend")

        cancellation_rows = _documents(
            database, "cancellation_summary", period_query
        )
        if cancellation_rows:
            cancellation_figure = px.bar(
                pd.DataFrame(cancellation_rows),
                x="cancellation_code",
                y="cancelled_flights",
                title="Cancellation reasons",
                labels={
                    "cancellation_code": "Reason code",
                    "cancelled_flights": "Cancelled flights",
                },
            )
        else:
            cancellation_figure = _empty_figure("Cancellation reasons")

        score_query = dict(period_query)
        if airport:
            score_query["airport"] = airport
        score_rows = _documents(
            database,
            "airport_score",
            score_query,
            sort=[("airport_score", -1)],
            limit=25,
        )
        if score_rows:
            score_figure = px.bar(
                pd.DataFrame(score_rows),
                x="airport",
                y="airport_score",
                title="Composite airport score",
                labels={"airport": "Airport", "airport_score": "Score"},
            )
        else:
            score_figure = _empty_figure("Composite airport score")

        route_query = dict(period_query)
        if route:
            origin, destination = route.split("|", maxsplit=1)
            route_query.update({"origin": origin, "destination": destination})
        elif airport:
            route_query["$or"] = [{"origin": airport}, {"destination": airport}]
        route_rows = _documents(
            database,
            "route_summary",
            route_query,
            sort=[("scheduled_flights", -1)],
            limit=100,
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
                    line={"width": 1, "color": "#3366cc"},
                    opacity=0.35,
                    hoverinfo="text",
                    text=f"{row['origin']} → {row['destination']}",
                    showlegend=False,
                )
            )
        map_figure.update_layout(
            title="Airport route network",
            geo={"scope": "usa", "projection_type": "albers usa"},
            template="plotly_white",
        )
        return (
            carrier_figure,
            trend_figure,
            cancellation_figure,
            score_figure,
            map_figure,
        )

    return app
