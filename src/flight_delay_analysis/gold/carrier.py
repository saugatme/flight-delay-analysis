"""Carrier delay and cancellation Gold aggregation."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flight_delay_analysis.gold.errors import GoldPipelineError

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
