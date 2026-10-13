"""Safe storage and readback checks for carrier Gold output."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flight_delay_analysis.config import PipelineConfig
from flight_delay_analysis.gold.errors import GoldPipelineError
from flight_delay_analysis.gold.models import (
    CarrierGoldMetrics,
    CarrierGoldReadback,
    CarrierGoldResult,
    carrier_gold_metrics_from_json,
    carrier_gold_metrics_to_json,
)
from flight_delay_analysis.gold.paths import (
    carrier_quality_metrics_uri,
    carrier_summary_uri,
)
from flight_delay_analysis.hdfs import HdfsClient


def completed_carrier_gold_run(
    config: PipelineConfig,
    hdfs: HdfsClient,
    flight_sha256: str,
    airport_sha256: str,
) -> CarrierGoldMetrics | None:
    """Return matching metrics or reject an incomplete carrier Gold run."""
    output_path = carrier_summary_uri(config, flight_sha256, airport_sha256)
    metrics_path = carrier_quality_metrics_uri(config, flight_sha256, airport_sha256)
    output_exists = hdfs.exists(output_path)
    metrics_exists = hdfs.exists(metrics_path)
    if not output_exists and not metrics_exists:
        return None
    if output_exists != metrics_exists:
        raise GoldPipelineError("Carrier Gold output is incomplete")

    metrics = carrier_gold_metrics_from_json(hdfs.read_text(metrics_path))
    if metrics.flight_source_sha256 != flight_sha256:
        raise GoldPipelineError("Carrier Gold has a different flight source")
    if metrics.airport_source_sha256 != airport_sha256:
        raise GoldPipelineError("Carrier Gold has a different airport source")
    return metrics


def write_carrier_gold_outputs(
    summary: DataFrame,
    metrics: CarrierGoldMetrics,
    config: PipelineConfig,
    hdfs: HdfsClient,
) -> CarrierGoldResult:
    """Write carrier Parquet and its quality record without overwriting."""
    output_path = carrier_summary_uri(
        config, metrics.flight_source_sha256, metrics.airport_source_sha256
    )
    metrics_path = carrier_quality_metrics_uri(
        config, metrics.flight_source_sha256, metrics.airport_source_sha256
    )
    if hdfs.exists(output_path) or hdfs.exists(metrics_path):
        raise GoldPipelineError("Carrier Gold output path already exists")

    summary.write.mode("errorifexists").parquet(output_path)
    hdfs.make_directory(metrics_path.rsplit("/", maxsplit=1)[0])
    hdfs.upload_text(carrier_gold_metrics_to_json(metrics), metrics_path)
    return CarrierGoldResult(status="written", metrics=metrics)


def validate_carrier_gold_readback(
    summary: DataFrame,
    metrics: CarrierGoldMetrics,
) -> CarrierGoldReadback:
    """Verify persisted carrier Gold types and reconciled counts."""
    required_types = {
        "report_year": "int",
        "report_month": "int",
        "reporting_airline": "string",
        "scheduled_flights": "bigint",
        "arrival_delay_rate": "double",
        "cancellation_rate": "double",
    }
    actual_types = {
        field.name: field.dataType.simpleString() for field in summary.schema.fields
    }
    errors = {
        name: (expected, actual_types.get(name))
        for name, expected in required_types.items()
        if actual_types.get(name) != expected
    }
    if errors:
        raise GoldPipelineError(f"Carrier Gold readback has unexpected types: {errors}")

    counts = summary.agg(
        F.count("*").alias("output_carrier_rows"),
        F.sum("scheduled_flights").alias("scheduled_flights"),
        F.sum("cancelled_flights").alias("cancelled_flights"),
    ).first()
    readback = CarrierGoldReadback(
        output_carrier_rows=counts["output_carrier_rows"],
        scheduled_flights=counts["scheduled_flights"] or 0,
        cancelled_flights=counts["cancelled_flights"] or 0,
    )
    expected = (
        metrics.output_carrier_rows,
        metrics.scheduled_flights,
        metrics.cancelled_flights,
    )
    actual = (
        readback.output_carrier_rows,
        readback.scheduled_flights,
        readback.cancelled_flights,
    )
    if actual != expected:
        raise GoldPipelineError(
            "Carrier Gold readback does not match quality metrics: "
            f"expected={expected}, found={actual}"
        )
    return readback
