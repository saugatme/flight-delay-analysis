"""Cancellation-code Gold aggregation."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from flight_delay_analysis.gold.common import require_columns, safe_rate


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
