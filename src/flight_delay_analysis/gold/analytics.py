"""Build and validate the analytics Gold table bundle."""

from datetime import UTC, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flight_delay_analysis.gold.airports import (
    build_airport_connectivity,
    build_airport_summary,
)
from flight_delay_analysis.gold.cancellations import build_cancellation_summary
from flight_delay_analysis.gold.errors import GoldPipelineError
from flight_delay_analysis.gold.models import AnalyticsGoldMetrics
from flight_delay_analysis.gold.periods import (
    build_month_summary,
    build_season_summary,
)
from flight_delay_analysis.gold.routes import build_route_summary
from flight_delay_analysis.gold.scores import build_airport_scores, build_route_scores


def build_analytics_tables(
    flights: DataFrame,
    report_year: int,
    report_month: int,
    delay_threshold_minutes: int,
) -> dict[str, DataFrame]:
    """Build every non-carrier Gold table from one enriched Silver input."""
    month = build_month_summary(
        flights, report_year, report_month, delay_threshold_minutes
    )
    airport = build_airport_summary(
        flights, report_year, report_month, delay_threshold_minutes
    )
    route = build_route_summary(
        flights, report_year, report_month, delay_threshold_minutes
    )
    connectivity = build_airport_connectivity(flights, report_year, report_month)
    return {
        "month_summary": month,
        "season_summary": build_season_summary(month),
        "airport_summary": airport,
        "route_summary": route,
        "cancellation_summary": build_cancellation_summary(
            flights, report_year, report_month
        ),
        "airport_connectivity": connectivity,
        "airport_score": build_airport_scores(airport, connectivity),
        "route_score": build_route_scores(route),
    }


def _sum_value(dataframe: DataFrame, column: str) -> int:
    value = dataframe.agg(F.sum(column).alias("total")).first()["total"]
    return value or 0


def collect_analytics_gold_metrics(
    flights: DataFrame,
    tables: dict[str, DataFrame],
    expected_input_rows: int,
    flight_sha256: str,
    airport_sha256: str,
    delay_threshold_minutes: int,
    processed_at: datetime | None = None,
) -> AnalyticsGoldMetrics:
    """Reconcile every Gold subject table with enriched Silver."""
    expected_tables = {
        "month_summary",
        "season_summary",
        "airport_summary",
        "route_summary",
        "cancellation_summary",
        "airport_connectivity",
        "airport_score",
        "route_score",
    }
    if set(tables) != expected_tables:
        raise GoldPipelineError("Analytics Gold table set is incomplete")

    input_rows = flights.count()
    if input_rows != expected_input_rows:
        raise GoldPipelineError(
            "Enriched Silver row count does not match its quality metrics: "
            f"expected {expected_input_rows}, found {input_rows}"
        )
    cancelled_rows = flights.filter(F.col("Cancelled")).count()
    checks = {
        "month scheduled": _sum_value(tables["month_summary"], "scheduled_flights"),
        "season scheduled": _sum_value(
            tables["season_summary"], "scheduled_flights"
        ),
        "airport departures": _sum_value(
            tables["airport_summary"], "scheduled_departures"
        ),
        "airport arrivals": _sum_value(
            tables["airport_summary"], "scheduled_arrivals"
        ),
        "route scheduled": _sum_value(tables["route_summary"], "scheduled_flights"),
        "connectivity departures": _sum_value(
            tables["airport_connectivity"], "scheduled_departures"
        ),
    }
    invalid = {name: value for name, value in checks.items() if value != input_rows}
    if invalid:
        raise GoldPipelineError(
            f"Analytics Gold flight counts do not reconcile: {invalid}"
        )
    cancellation_total = _sum_value(
        tables["cancellation_summary"], "cancelled_flights"
    )
    if cancellation_total != cancelled_rows:
        raise GoldPipelineError(
            "Cancellation Gold count does not reconcile with enriched Silver"
        )

    table_rows = {name: table.count() for name, table in tables.items()}
    if table_rows["airport_score"] != table_rows["airport_summary"]:
        raise GoldPipelineError("Airport score row count does not reconcile")
    if table_rows["route_score"] != table_rows["route_summary"]:
        raise GoldPipelineError("Route score row count does not reconcile")

    timestamp = processed_at or datetime.now(UTC)
    if timestamp.tzinfo is None:
        raise ValueError("processed_at must include a timezone")
    return AnalyticsGoldMetrics(
        flight_source_sha256=flight_sha256,
        airport_source_sha256=airport_sha256,
        input_flight_rows=input_rows,
        cancelled_flights=cancelled_rows,
        delay_threshold_minutes=delay_threshold_minutes,
        table_rows=table_rows,
        route_score_formula="45% on-time + 35% completion + 20% volume percentile",
        airport_score_formula=(
            "35% on-time + 30% completion + 20% connectivity percentile "
            "+ 15% volume percentile"
        ),
        processed_at_utc=timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    )
