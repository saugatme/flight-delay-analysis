"""Monthly and seasonal Gold aggregations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flight_delay_analysis.gold.common import require_columns, safe_rate, season_name


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
        safe_rate("cancelled_flights", "scheduled_flights").alias(
            "cancellation_rate"
        ),
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
            F.col("average_arrival_delay_minutes")
            * F.col("arrival_result_flights")
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
        safe_rate("cancelled_flights", "scheduled_flights").alias(
            "cancellation_rate"
        ),
    )
