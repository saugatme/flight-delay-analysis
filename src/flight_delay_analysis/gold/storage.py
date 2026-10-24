"""Checksum-linked Gold paths, HDFS writes, and readback checks."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flight_delay_analysis.config import PipelineConfig
from flight_delay_analysis.gold.calculations import (
    AnalyticsGoldMetrics,
    AnalyticsGoldResult,
    CarrierGoldMetrics,
    CarrierGoldReadback,
    CarrierGoldResult,
    GoldPipelineError,
    analytics_gold_metrics_from_json,
    analytics_gold_metrics_to_json,
    carrier_gold_metrics_from_json,
    carrier_gold_metrics_to_json,
)
from flight_delay_analysis.hdfs import HdfsClient

CARRIER_SUMMARY_DATASET = "carrier_summary"


CARRIER_QUALITY_DATASET = "quality_metrics/carrier_summary"


CARRIER_QUALITY_FILE_NAME = "quality_metrics.json"


ANALYTICS_QUALITY_DATASET = "quality_metrics/analytics_bundle"


ANALYTICS_DATASETS = {
    "month_summary": "month_summary",
    "season_summary": "season_summary",
    "airport_summary": "airport_summary",
    "route_summary": "route_summary",
    "cancellation_summary": "cancellation_code_summary",
    "airport_connectivity": "airport_connectivity",
    "airport_score": "airport_score",
    "route_score": "route_score",
}


def gold_run_uri(
    config: PipelineConfig,
    dataset: str,
    flight_sha256: str,
    airport_sha256: str,
) -> str:
    """Build one checksum-linked monthly Gold dataset path."""
    return (
        f"{config.hdfs.base_uri}/{config.hdfs.gold_dataset}/{dataset}"
        f"/year={config.run.year:04d}/month={config.run.month:02d}"
        f"/flight_source_sha256={flight_sha256}"
        f"/airport_source_sha256={airport_sha256}"
    )


def carrier_summary_uri(
    config: PipelineConfig,
    flight_sha256: str,
    airport_sha256: str,
) -> str:
    """Build the versioned carrier-summary Parquet path."""
    return gold_run_uri(
        config,
        CARRIER_SUMMARY_DATASET,
        flight_sha256,
        airport_sha256,
    )


def carrier_quality_metrics_uri(
    config: PipelineConfig,
    flight_sha256: str,
    airport_sha256: str,
) -> str:
    """Build the versioned carrier-summary quality JSON path."""
    directory = gold_run_uri(
        config,
        CARRIER_QUALITY_DATASET,
        flight_sha256,
        airport_sha256,
    )
    return f"{directory}/{CARRIER_QUALITY_FILE_NAME}"


def analytics_table_uri(
    config: PipelineConfig,
    table_name: str,
    flight_sha256: str,
    airport_sha256: str,
) -> str:
    """Build a checksum-linked path for one analytics Gold table."""
    try:
        dataset = ANALYTICS_DATASETS[table_name]
    except KeyError as error:
        raise ValueError(f"Unknown analytics Gold table: {table_name}") from error
    return gold_run_uri(config, dataset, flight_sha256, airport_sha256)


def analytics_quality_metrics_uri(
    config: PipelineConfig,
    flight_sha256: str,
    airport_sha256: str,
) -> str:
    """Build the analytics bundle quality JSON path."""
    directory = gold_run_uri(
        config,
        ANALYTICS_QUALITY_DATASET,
        flight_sha256,
        airport_sha256,
    )
    return f"{directory}/{CARRIER_QUALITY_FILE_NAME}"


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


def completed_analytics_gold_run(
    config: PipelineConfig,
    hdfs: HdfsClient,
    flight_sha256: str,
    airport_sha256: str,
) -> AnalyticsGoldMetrics | None:
    """Return metrics for a complete bundle or reject partial output."""
    paths = {
        name: analytics_table_uri(config, name, flight_sha256, airport_sha256)
        for name in ANALYTICS_DATASETS
    }
    metrics_path = analytics_quality_metrics_uri(config, flight_sha256, airport_sha256)
    states = {name: hdfs.exists(path) for name, path in paths.items()}
    metrics_exists = hdfs.exists(metrics_path)
    if not any(states.values()) and not metrics_exists:
        return None
    if not all(states.values()) or not metrics_exists:
        raise GoldPipelineError("Analytics Gold output is incomplete")

    metrics = analytics_gold_metrics_from_json(hdfs.read_text(metrics_path))
    if metrics.flight_source_sha256 != flight_sha256:
        raise GoldPipelineError("Analytics Gold has a different flight source")
    if metrics.airport_source_sha256 != airport_sha256:
        raise GoldPipelineError("Analytics Gold has a different airport source")
    return metrics


def write_analytics_gold_outputs(
    tables: dict[str, DataFrame],
    metrics: AnalyticsGoldMetrics,
    config: PipelineConfig,
    hdfs: HdfsClient,
) -> AnalyticsGoldResult:
    """Write all analytics tables and one shared quality record."""
    if set(tables) != set(ANALYTICS_DATASETS):
        raise GoldPipelineError("Analytics Gold table set is incomplete")
    paths = {
        name: analytics_table_uri(
            config,
            name,
            metrics.flight_source_sha256,
            metrics.airport_source_sha256,
        )
        for name in tables
    }
    metrics_path = analytics_quality_metrics_uri(
        config,
        metrics.flight_source_sha256,
        metrics.airport_source_sha256,
    )
    if any(hdfs.exists(path) for path in paths.values()) or hdfs.exists(metrics_path):
        raise GoldPipelineError("Analytics Gold output path already exists")

    for name, table in tables.items():
        table.write.mode("errorifexists").parquet(paths[name])
    hdfs.make_directory(metrics_path.rsplit("/", maxsplit=1)[0])
    hdfs.upload_text(analytics_gold_metrics_to_json(metrics), metrics_path)
    return AnalyticsGoldResult(status="written", metrics=metrics)


def validate_analytics_gold_readback(
    tables: dict[str, DataFrame],
    metrics: AnalyticsGoldMetrics,
) -> None:
    """Require stored table row counts to match the bundle metrics."""
    actual = {name: table.count() for name, table in tables.items()}
    if actual != metrics.table_rows:
        raise GoldPipelineError(
            "Analytics Gold readback row counts do not match quality metrics: "
            f"expected={metrics.table_rows}, found={actual}"
        )
