"""Gold measures, table builders, and quality contracts."""

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


class GoldPipelineError(RuntimeError):
    """Report invalid input or state for a Gold pipeline."""


def require_columns(dataframe: DataFrame, required: set[str], label: str) -> None:
    """Require named input columns before a Gold aggregation starts."""
    missing = sorted(required - set(dataframe.columns))
    if missing:
        raise GoldPipelineError(f"{label} is missing columns: {', '.join(missing)}")


def safe_rate(numerator: str | Column, denominator: str | Column) -> Column:
    """Divide only when the denominator is positive."""
    numerator_column = F.col(numerator) if isinstance(numerator, str) else numerator
    denominator_column = (
        F.col(denominator) if isinstance(denominator, str) else denominator
    )
    return F.when(denominator_column > 0, numerator_column / denominator_column)


def season_name(month: int) -> str:
    """Map a calendar month to a meteorological season."""
    if month in {12, 1, 2}:
        return "winter"
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    return "autumn"


@dataclass(frozen=True)
class CarrierGoldMetrics:
    """Record reconciled counts and lineage for one carrier Gold result."""

    flight_source_sha256: str
    airport_source_sha256: str
    input_flight_rows: int
    output_carrier_rows: int
    scheduled_flights: int
    operated_flights: int
    arrival_result_flights: int
    delayed_flights: int
    cancelled_flights: int
    delay_threshold_minutes: int
    arrival_delay_rate_denominator: str
    cancellation_rate_denominator: str
    processed_at_utc: str


@dataclass(frozen=True)
class CarrierGoldResult:
    """Describe whether carrier Gold output was written or reused."""

    status: str
    metrics: CarrierGoldMetrics


@dataclass(frozen=True)
class CarrierGoldReadback:
    """Store verification facts from persisted carrier Gold Parquet."""

    output_carrier_rows: int
    scheduled_flights: int
    cancelled_flights: int


@dataclass(frozen=True)
class AnalyticsGoldMetrics:
    """Record lineage and reconciled counts for all analytics Gold tables."""

    flight_source_sha256: str
    airport_source_sha256: str
    input_flight_rows: int
    cancelled_flights: int
    delay_threshold_minutes: int
    table_rows: dict[str, int]
    route_score_formula: str
    airport_score_formula: str
    processed_at_utc: str


@dataclass(frozen=True)
class AnalyticsGoldResult:
    """Describe whether the analytics Gold bundle was written or reused."""

    status: str
    metrics: AnalyticsGoldMetrics


def carrier_gold_metrics_to_json(metrics: CarrierGoldMetrics) -> str:
    """Serialize carrier Gold quality metrics in a stable format."""
    return json.dumps(asdict(metrics), indent=2, sort_keys=True) + "\n"


def carrier_gold_metrics_from_json(content: str) -> CarrierGoldMetrics:
    """Deserialize stored carrier Gold quality metrics."""
    try:
        values: dict[str, Any] = json.loads(content)
        return CarrierGoldMetrics(**values)
    except (json.JSONDecodeError, TypeError) as error:
        raise GoldPipelineError("Invalid carrier Gold quality metrics") from error


def analytics_gold_metrics_to_json(metrics: AnalyticsGoldMetrics) -> str:
    """Serialize analytics Gold quality metrics."""
    return json.dumps(asdict(metrics), indent=2, sort_keys=True) + "\n"


def analytics_gold_metrics_from_json(content: str) -> AnalyticsGoldMetrics:
    """Deserialize analytics Gold quality metrics."""
    try:
        values: dict[str, Any] = json.loads(content)
        return AnalyticsGoldMetrics(**values)
    except (json.JSONDecodeError, TypeError) as error:
        raise GoldPipelineError("Invalid analytics Gold quality metrics") from error


CARRIER_REQUIRED_COLUMNS = {"Reporting_Airline", "Cancelled", "ArrDelay"}


def build_carrier_summary(
    flights: DataFrame,
    report_year: int,
    report_month: int,
    delay_threshold_minutes: int,
) -> DataFrame:
    """Aggregate monthly delay and cancellation measures by carrier."""
    missing = sorted(CARRIER_REQUIRED_COLUMNS - set(flights.columns))
    if missing:
        raise GoldPipelineError(
            f"Enriched Silver is missing required columns: {', '.join(missing)}"
        )
    if delay_threshold_minutes <= 0:
        raise GoldPipelineError("Delay threshold must be positive")

    operated = ~F.col("Cancelled")
    has_arrival_result = operated & F.col("ArrDelay").isNotNull()
    is_delayed = has_arrival_result & (
        F.col("ArrDelay") >= F.lit(delay_threshold_minutes)
    )
    counts = flights.groupBy("Reporting_Airline").agg(
        F.count("*").alias("scheduled_flights"),
        F.sum(operated.cast("long")).alias("operated_flights"),
        F.sum(has_arrival_result.cast("long")).alias("arrival_result_flights"),
        F.sum(is_delayed.cast("long")).alias("delayed_flights"),
        F.sum(F.col("Cancelled").cast("long")).alias("cancelled_flights"),
        F.avg(F.when(has_arrival_result, F.col("ArrDelay"))).alias(
            "average_arrival_delay_minutes"
        ),
    )
    return counts.select(
        F.lit(report_year).cast("int").alias("report_year"),
        F.lit(report_month).cast("int").alias("report_month"),
        F.col("Reporting_Airline").alias("reporting_airline"),
        "scheduled_flights",
        "operated_flights",
        "arrival_result_flights",
        "delayed_flights",
        "cancelled_flights",
        "average_arrival_delay_minutes",
        F.when(
            F.col("arrival_result_flights") > 0,
            F.col("delayed_flights") / F.col("arrival_result_flights"),
        ).alias("arrival_delay_rate"),
        F.when(
            F.col("scheduled_flights") > 0,
            F.col("cancelled_flights") / F.col("scheduled_flights"),
        ).alias("cancellation_rate"),
    )


def build_airport_summary(
    flights: DataFrame,
    report_year: int,
    report_month: int,
    delay_threshold_minutes: int,
) -> DataFrame:
    """Build monthly departure and arrival measures for each airport."""
    required = {
        "Origin",
        "Dest",
        "OriginAirportName",
        "DestAirportName",
        "OriginLatitude",
        "OriginLongitude",
        "DestLatitude",
        "DestLongitude",
        "Cancelled",
        "DepDelay",
        "ArrDelay",
    }
    require_columns(flights, required, "Enriched Silver")
    operated = ~F.col("Cancelled")
    has_departure = operated & F.col("DepDelay").isNotNull()
    has_arrival = operated & F.col("ArrDelay").isNotNull()

    departures = flights.groupBy(
        F.col("Origin").alias("airport"),
        F.col("OriginAirportName").alias("departure_airport_name"),
        F.col("OriginLatitude").alias("departure_latitude"),
        F.col("OriginLongitude").alias("departure_longitude"),
    ).agg(
        F.count("*").alias("scheduled_departures"),
        F.sum(operated.cast("long")).alias("operated_departures"),
        F.sum(has_departure.cast("long")).alias("departure_result_flights"),
        F.sum(
            (has_departure & (F.col("DepDelay") >= delay_threshold_minutes)).cast(
                "long"
            )
        ).alias("delayed_departures"),
        F.sum(F.col("Cancelled").cast("long")).alias("cancelled_departures"),
        F.avg(F.when(has_departure, F.col("DepDelay"))).alias(
            "average_departure_delay_minutes"
        ),
    )
    arrivals = flights.groupBy(
        F.col("Dest").alias("airport"),
        F.col("DestAirportName").alias("arrival_airport_name"),
        F.col("DestLatitude").alias("arrival_latitude"),
        F.col("DestLongitude").alias("arrival_longitude"),
    ).agg(
        F.count("*").alias("scheduled_arrivals"),
        F.sum(operated.cast("long")).alias("operated_arrivals"),
        F.sum(has_arrival.cast("long")).alias("arrival_result_flights"),
        F.sum(
            (has_arrival & (F.col("ArrDelay") >= delay_threshold_minutes)).cast("long")
        ).alias("delayed_arrivals"),
        F.sum(F.col("Cancelled").cast("long")).alias("cancelled_arrivals"),
        F.avg(F.when(has_arrival, F.col("ArrDelay"))).alias(
            "average_arrival_delay_minutes"
        ),
    )
    joined = departures.join(arrivals, "airport", "full")
    count_columns = [
        "scheduled_departures",
        "operated_departures",
        "departure_result_flights",
        "delayed_departures",
        "cancelled_departures",
        "scheduled_arrivals",
        "operated_arrivals",
        "arrival_result_flights",
        "delayed_arrivals",
        "cancelled_arrivals",
    ]
    for column in count_columns:
        joined = joined.withColumn(column, F.coalesce(F.col(column), F.lit(0)))

    return joined.select(
        F.lit(report_year).cast("int").alias("report_year"),
        F.lit(report_month).cast("int").alias("report_month"),
        "airport",
        F.coalesce("departure_airport_name", "arrival_airport_name").alias(
            "airport_name"
        ),
        F.coalesce("departure_latitude", "arrival_latitude").alias("latitude"),
        F.coalesce("departure_longitude", "arrival_longitude").alias("longitude"),
        *count_columns,
        "average_departure_delay_minutes",
        "average_arrival_delay_minutes",
        safe_rate("delayed_departures", "departure_result_flights").alias(
            "departure_delay_rate"
        ),
        safe_rate("delayed_arrivals", "arrival_result_flights").alias(
            "arrival_delay_rate"
        ),
        safe_rate("cancelled_departures", "scheduled_departures").alias(
            "departure_cancellation_rate"
        ),
        safe_rate("cancelled_arrivals", "scheduled_arrivals").alias(
            "arrival_cancellation_rate"
        ),
    )


def build_airport_connectivity(
    flights: DataFrame,
    report_year: int,
    report_month: int,
) -> DataFrame:
    """Build monthly outbound connectivity measures for each origin airport."""
    required = {
        "Origin",
        "Dest",
        "Reporting_Airline",
        "OriginAirportName",
        "OriginLatitude",
        "OriginLongitude",
        "Cancelled",
    }
    require_columns(flights, required, "Enriched Silver")
    grouped = flights.groupBy(
        "Origin",
        "OriginAirportName",
        "OriginLatitude",
        "OriginLongitude",
    ).agg(
        F.count("*").alias("scheduled_departures"),
        F.sum((~F.col("Cancelled")).cast("long")).alias("operated_departures"),
        F.countDistinct("Dest").alias("unique_destinations"),
        F.countDistinct("Reporting_Airline").alias("unique_carriers"),
    )
    return grouped.select(
        F.lit(report_year).cast("int").alias("report_year"),
        F.lit(report_month).cast("int").alias("report_month"),
        F.col("Origin").alias("airport"),
        F.col("OriginAirportName").alias("airport_name"),
        F.col("OriginLatitude").alias("latitude"),
        F.col("OriginLongitude").alias("longitude"),
        "scheduled_departures",
        "operated_departures",
        "unique_destinations",
        "unique_carriers",
    )


def build_route_summary(
    flights: DataFrame,
    report_year: int,
    report_month: int,
    delay_threshold_minutes: int,
) -> DataFrame:
    """Build one monthly row per directed origin-to-destination route."""
    required = {
        "Origin",
        "Dest",
        "OriginAirportName",
        "DestAirportName",
        "OriginLatitude",
        "OriginLongitude",
        "DestLatitude",
        "DestLongitude",
        "Cancelled",
        "ArrDelay",
        "Distance",
    }
    require_columns(flights, required, "Enriched Silver")
    operated = ~F.col("Cancelled")
    has_arrival = operated & F.col("ArrDelay").isNotNull()
    delayed = has_arrival & (F.col("ArrDelay") >= delay_threshold_minutes)
    grouped = flights.groupBy(
        "Origin",
        "Dest",
        "OriginAirportName",
        "DestAirportName",
        "OriginLatitude",
        "OriginLongitude",
        "DestLatitude",
        "DestLongitude",
    ).agg(
        F.count("*").alias("scheduled_flights"),
        F.sum(operated.cast("long")).alias("operated_flights"),
        F.sum(has_arrival.cast("long")).alias("arrival_result_flights"),
        F.sum(delayed.cast("long")).alias("delayed_flights"),
        F.sum(F.col("Cancelled").cast("long")).alias("cancelled_flights"),
        F.avg(F.when(has_arrival, F.col("ArrDelay"))).alias(
            "average_arrival_delay_minutes"
        ),
        F.avg("Distance").alias("average_distance_miles"),
    )
    return grouped.select(
        F.lit(report_year).cast("int").alias("report_year"),
        F.lit(report_month).cast("int").alias("report_month"),
        F.col("Origin").alias("origin"),
        F.col("Dest").alias("destination"),
        F.col("OriginAirportName").alias("origin_airport_name"),
        F.col("DestAirportName").alias("destination_airport_name"),
        F.col("OriginLatitude").alias("origin_latitude"),
        F.col("OriginLongitude").alias("origin_longitude"),
        F.col("DestLatitude").alias("destination_latitude"),
        F.col("DestLongitude").alias("destination_longitude"),
        "scheduled_flights",
        "operated_flights",
        "arrival_result_flights",
        "delayed_flights",
        "cancelled_flights",
        "average_arrival_delay_minutes",
        "average_distance_miles",
        safe_rate("delayed_flights", "arrival_result_flights").alias(
            "arrival_delay_rate"
        ),
        safe_rate("cancelled_flights", "scheduled_flights").alias("cancellation_rate"),
    )


def build_cancellation_summary(
    flights: DataFrame,
    report_year: int,
    report_month: int,
) -> DataFrame:
    """Build one monthly row per cancellation reason code."""
    require_columns(
        flights,
        {"Cancelled", "CancellationCode"},
        "Enriched Silver",
    )
    cancelled = flights.filter(F.col("Cancelled")).withColumn(
        "cancellation_code",
        F.when(
            F.col("CancellationCode").isNull()
            | (F.trim(F.col("CancellationCode")) == ""),
            F.lit("UNKNOWN"),
        ).otherwise(F.trim(F.col("CancellationCode"))),
    )
    counts = cancelled.groupBy("cancellation_code").agg(
        F.count("*").alias("cancelled_flights")
    )
    total_window = Window.partitionBy()
    return (
        counts.withColumn(
            "all_cancelled_flights",
            F.sum("cancelled_flights").over(total_window),
        )
        .withColumn(
            "cancellation_share",
            safe_rate("cancelled_flights", "all_cancelled_flights"),
        )
        .select(
            F.lit(report_year).cast("int").alias("report_year"),
            F.lit(report_month).cast("int").alias("report_month"),
            "cancellation_code",
            "cancelled_flights",
            "all_cancelled_flights",
            "cancellation_share",
        )
    )


def build_month_summary(
    flights: DataFrame,
    report_year: int,
    report_month: int,
    delay_threshold_minutes: int,
) -> DataFrame:
    """Build one monthly operations and arrival-delay summary row."""
    require_columns(
        flights,
        {"Cancelled", "ArrDelay"},
        "Enriched Silver",
    )
    operated = ~F.col("Cancelled")
    has_arrival = operated & F.col("ArrDelay").isNotNull()
    delayed = has_arrival & (F.col("ArrDelay") >= delay_threshold_minutes)
    totals = flights.agg(
        F.count("*").alias("scheduled_flights"),
        F.sum(operated.cast("long")).alias("operated_flights"),
        F.sum(has_arrival.cast("long")).alias("arrival_result_flights"),
        F.sum(delayed.cast("long")).alias("delayed_flights"),
        F.sum(F.col("Cancelled").cast("long")).alias("cancelled_flights"),
        F.avg(F.when(has_arrival, F.col("ArrDelay"))).alias(
            "average_arrival_delay_minutes"
        ),
    )
    return totals.select(
        F.lit(report_year).cast("int").alias("report_year"),
        F.lit(report_month).cast("int").alias("report_month"),
        F.lit(season_name(report_month)).alias("season"),
        "scheduled_flights",
        "operated_flights",
        "arrival_result_flights",
        "delayed_flights",
        "cancelled_flights",
        "average_arrival_delay_minutes",
        safe_rate("delayed_flights", "arrival_result_flights").alias(
            "arrival_delay_rate"
        ),
        safe_rate("cancelled_flights", "scheduled_flights").alias("cancellation_rate"),
    )


def build_season_summary(months: DataFrame) -> DataFrame:
    """Roll monthly Gold rows into weighted seasonal summaries."""
    require_columns(
        months,
        {
            "report_year",
            "season",
            "scheduled_flights",
            "operated_flights",
            "arrival_result_flights",
            "delayed_flights",
            "cancelled_flights",
            "average_arrival_delay_minutes",
        },
        "Month Gold",
    )
    totals = months.groupBy("report_year", "season").agg(
        F.count("*").alias("included_months"),
        F.sum("scheduled_flights").alias("scheduled_flights"),
        F.sum("operated_flights").alias("operated_flights"),
        F.sum("arrival_result_flights").alias("arrival_result_flights"),
        F.sum("delayed_flights").alias("delayed_flights"),
        F.sum("cancelled_flights").alias("cancelled_flights"),
        F.sum(
            F.col("average_arrival_delay_minutes") * F.col("arrival_result_flights")
        ).alias("arrival_delay_minute_total"),
    )
    return totals.select(
        "report_year",
        "season",
        "included_months",
        "scheduled_flights",
        "operated_flights",
        "arrival_result_flights",
        "delayed_flights",
        "cancelled_flights",
        safe_rate("arrival_delay_minute_total", "arrival_result_flights").alias(
            "average_arrival_delay_minutes"
        ),
        safe_rate("delayed_flights", "arrival_result_flights").alias(
            "arrival_delay_rate"
        ),
        safe_rate("cancelled_flights", "scheduled_flights").alias("cancellation_rate"),
    )


def build_route_scores(routes: DataFrame) -> DataFrame:
    """Score routes by on-time rate, completion rate, and relative volume."""
    require_columns(
        routes,
        {
            "report_year",
            "report_month",
            "origin",
            "destination",
            "scheduled_flights",
            "arrival_result_flights",
            "arrival_delay_rate",
            "cancellation_rate",
        },
        "Route Gold",
    )
    rank_window = Window.partitionBy("report_year", "report_month").orderBy(
        "scheduled_flights"
    )
    return (
        routes.withColumn("volume_percentile", F.percent_rank().over(rank_window))
        .withColumn("on_time_rate", 1.0 - F.col("arrival_delay_rate"))
        .withColumn("completion_rate", 1.0 - F.col("cancellation_rate"))
        .withColumn(
            "route_score",
            F.when(
                F.col("arrival_result_flights") > 0,
                F.round(
                    100.0
                    * (
                        0.45 * F.col("on_time_rate")
                        + 0.35 * F.col("completion_rate")
                        + 0.20 * F.col("volume_percentile")
                    ),
                    2,
                ),
            ),
        )
        .select(
            "report_year",
            "report_month",
            "origin",
            "destination",
            "scheduled_flights",
            "arrival_result_flights",
            "on_time_rate",
            "completion_rate",
            "volume_percentile",
            "route_score",
        )
    )


def build_airport_scores(
    airports: DataFrame,
    connectivity: DataFrame,
) -> DataFrame:
    """Score airports by reliability, completion, connectivity, and volume."""
    require_columns(
        airports,
        {
            "report_year",
            "report_month",
            "airport",
            "airport_name",
            "latitude",
            "longitude",
            "scheduled_departures",
            "scheduled_arrivals",
            "departure_result_flights",
            "arrival_result_flights",
            "delayed_departures",
            "delayed_arrivals",
            "cancelled_departures",
            "cancelled_arrivals",
        },
        "Airport Gold",
    )
    require_columns(
        connectivity,
        {"report_year", "report_month", "airport", "unique_destinations"},
        "Connectivity Gold",
    )
    keys = ["report_year", "report_month", "airport"]
    joined = airports.join(
        connectivity.select(*keys, "unique_destinations"),
        keys,
        "left",
    ).fillna({"unique_destinations": 0})
    scored = (
        joined.withColumn(
            "scheduled_flights",
            F.col("scheduled_departures") + F.col("scheduled_arrivals"),
        )
        .withColumn(
            "delay_result_flights",
            F.col("departure_result_flights") + F.col("arrival_result_flights"),
        )
        .withColumn(
            "delayed_flights",
            F.col("delayed_departures") + F.col("delayed_arrivals"),
        )
        .withColumn(
            "cancelled_flights",
            F.col("cancelled_departures") + F.col("cancelled_arrivals"),
        )
        .withColumn(
            "on_time_rate",
            1.0 - safe_rate("delayed_flights", "delay_result_flights"),
        )
        .withColumn(
            "completion_rate",
            1.0 - safe_rate("cancelled_flights", "scheduled_flights"),
        )
    )
    partition = Window.partitionBy("report_year", "report_month")
    volume_window = partition.orderBy("scheduled_flights")
    connectivity_window = partition.orderBy("unique_destinations")
    return (
        scored.withColumn("volume_percentile", F.percent_rank().over(volume_window))
        .withColumn(
            "connectivity_percentile",
            F.percent_rank().over(connectivity_window),
        )
        .withColumn(
            "airport_score",
            F.when(
                F.col("delay_result_flights") > 0,
                F.round(
                    100.0
                    * (
                        0.35 * F.col("on_time_rate")
                        + 0.30 * F.col("completion_rate")
                        + 0.20 * F.col("connectivity_percentile")
                        + 0.15 * F.col("volume_percentile")
                    ),
                    2,
                ),
            ),
        )
        .select(
            "report_year",
            "report_month",
            "airport",
            "airport_name",
            "latitude",
            "longitude",
            "scheduled_flights",
            "delay_result_flights",
            "unique_destinations",
            "on_time_rate",
            "completion_rate",
            "connectivity_percentile",
            "volume_percentile",
            "airport_score",
        )
    )


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
        "season scheduled": _sum_value(tables["season_summary"], "scheduled_flights"),
        "airport departures": _sum_value(
            tables["airport_summary"], "scheduled_departures"
        ),
        "airport arrivals": _sum_value(tables["airport_summary"], "scheduled_arrivals"),
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
    cancellation_total = _sum_value(tables["cancellation_summary"], "cancelled_flights")
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


def collect_carrier_gold_metrics(
    flights: DataFrame,
    summary: DataFrame,
    expected_input_rows: int,
    flight_sha256: str,
    airport_sha256: str,
    delay_threshold_minutes: int,
    processed_at: datetime | None = None,
) -> CarrierGoldMetrics:
    """Validate carrier measures and collect their quality metrics."""
    input_rows = flights.count()
    if input_rows != expected_input_rows:
        raise GoldPipelineError(
            "Enriched Silver row count does not match its quality metrics: "
            f"expected {expected_input_rows}, found {input_rows}"
        )

    invalid_row = (
        F.col("reporting_airline").isNull()
        | (F.trim(F.col("reporting_airline")) == "")
        | (F.col("scheduled_flights") <= 0)
        | (F.col("operated_flights") < 0)
        | (F.col("arrival_result_flights") < 0)
        | (F.col("delayed_flights") < 0)
        | (F.col("cancelled_flights") < 0)
        | (
            F.col("operated_flights") + F.col("cancelled_flights")
            != F.col("scheduled_flights")
        )
        | (F.col("arrival_result_flights") > F.col("operated_flights"))
        | (F.col("delayed_flights") > F.col("arrival_result_flights"))
        | F.col("cancellation_rate").isNull()
        | ~F.col("cancellation_rate").between(0.0, 1.0)
        | (
            (F.col("arrival_result_flights") == 0)
            & (
                F.col("arrival_delay_rate").isNotNull()
                | F.col("average_arrival_delay_minutes").isNotNull()
            )
        )
        | (
            (F.col("arrival_result_flights") > 0)
            & (
                F.col("arrival_delay_rate").isNull()
                | ~F.col("arrival_delay_rate").between(0.0, 1.0)
                | F.col("average_arrival_delay_minutes").isNull()
            )
        )
    )
    totals = summary.agg(
        F.count("*").alias("output_carrier_rows"),
        F.sum("scheduled_flights").alias("scheduled_flights"),
        F.sum("operated_flights").alias("operated_flights"),
        F.sum("arrival_result_flights").alias("arrival_result_flights"),
        F.sum("delayed_flights").alias("delayed_flights"),
        F.sum("cancelled_flights").alias("cancelled_flights"),
        F.sum(F.when(invalid_row, 1).otherwise(0)).alias("invalid_rows"),
    ).first()
    output_rows = totals["output_carrier_rows"]
    scheduled_flights = totals["scheduled_flights"] or 0
    if output_rows == 0:
        raise GoldPipelineError("Carrier Gold result is empty")
    if totals["invalid_rows"]:
        raise GoldPipelineError("Carrier Gold result has invalid counts or rates")
    if scheduled_flights != input_rows:
        raise GoldPipelineError(
            "Carrier Gold scheduled flights do not reconcile with enriched Silver: "
            f"expected {input_rows}, found {scheduled_flights}"
        )

    timestamp = processed_at or datetime.now(UTC)
    if timestamp.tzinfo is None:
        raise ValueError("processed_at must include a timezone")
    return CarrierGoldMetrics(
        flight_source_sha256=flight_sha256,
        airport_source_sha256=airport_sha256,
        input_flight_rows=input_rows,
        output_carrier_rows=output_rows,
        scheduled_flights=scheduled_flights,
        operated_flights=totals["operated_flights"] or 0,
        arrival_result_flights=totals["arrival_result_flights"] or 0,
        delayed_flights=totals["delayed_flights"] or 0,
        cancelled_flights=totals["cancelled_flights"] or 0,
        delay_threshold_minutes=delay_threshold_minutes,
        arrival_delay_rate_denominator="arrival_result_flights",
        cancellation_rate_denominator="scheduled_flights",
        processed_at_utc=timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    )
