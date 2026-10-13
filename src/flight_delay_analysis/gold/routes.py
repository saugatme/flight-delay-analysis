"""Route performance Gold aggregation."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flight_delay_analysis.gold.common import require_columns, safe_rate


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
        safe_rate("cancelled_flights", "scheduled_flights").alias(
            "cancellation_rate"
        ),
    )
