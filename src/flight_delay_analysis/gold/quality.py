"""Quality validation for Gold datasets."""

from datetime import UTC, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flight_delay_analysis.gold.errors import GoldPipelineError
from flight_delay_analysis.gold.models import CarrierGoldMetrics


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
