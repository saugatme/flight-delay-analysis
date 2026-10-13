"""Transparent composite scores for airports and routes."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from flight_delay_analysis.gold.common import require_columns, safe_rate


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
