"""Read, validate, and classify HDFS Bronze data for Silver processing."""

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from flight_delay_analysis.bronze import (
    IngestionManifest,
    bronze_partition_uri,
    manifest_from_json,
    manifest_uri,
)
from flight_delay_analysis.config import PipelineConfig
from flight_delay_analysis.contracts import (
    DOUBLE_COLUMNS,
    FLAG_COLUMNS,
    REQUIRED_COLUMNS,
)
from flight_delay_analysis.hdfs import HdfsClient
from flight_delay_analysis.transforms import cast_columns


class SilverPipelineError(RuntimeError):
    """Report an invalid input or state for Silver processing."""


@dataclass(frozen=True)
class ClassifiedFlights:
    """Hold valid typed rows and invalid raw rows with reasons."""

    accepted: DataFrame
    rejected: DataFrame


@dataclass(frozen=True)
class SilverQualityMetrics:
    """Store reconciled counts and source lineage for one Silver partition."""

    source_file_name: str
    source_sha256: str
    input_rows: int
    accepted_rows: int
    rejected_rows: int
    cancelled_rows: int
    diverted_rows: int
    rejection_reason_counts: dict[str, int]
    processed_at_utc: str


@dataclass(frozen=True)
class SilverWriteResult:
    """Describe whether Silver outputs were written or already complete."""

    status: str
    metrics: SilverQualityMetrics


QUALITY_FILE_NAME = "quality_metrics.json"


def load_bronze_manifest(
    config: PipelineConfig, hdfs: HdfsClient
) -> IngestionManifest:
    """Load and validate the manifest for the configured Bronze partition."""
    stored_manifest_uri = manifest_uri(config)
    if not hdfs.exists(stored_manifest_uri):
        raise SilverPipelineError(f"Bronze manifest not found: {stored_manifest_uri}")

    manifest = manifest_from_json(hdfs.read_text(stored_manifest_uri))
    partition_uri = bronze_partition_uri(config)
    expected_prefix = f"{partition_uri}/"
    if not manifest.hdfs_file_uri.startswith(expected_prefix):
        raise SilverPipelineError(
            "Bronze manifest source is outside the configured monthly partition"
        )

    relative_name = manifest.hdfs_file_uri.removeprefix(expected_prefix)
    if "/" in relative_name or relative_name != manifest.source_file_name:
        raise SilverPipelineError(
            "Bronze manifest source name does not match its HDFS file URI"
        )
    if not hdfs.exists(manifest.hdfs_file_uri):
        raise SilverPipelineError(
            f"Bronze source file not found: {manifest.hdfs_file_uri}"
        )
    return manifest


def bronze_source_uri(config: PipelineConfig, hdfs: HdfsClient) -> str:
    """Return the exact Bronze source URI recorded by the partition manifest."""
    return load_bronze_manifest(config, hdfs).hdfs_file_uri


def _partition_uri(config: PipelineConfig, dataset: str) -> str:
    return (
        f"{config.hdfs.base_uri}/{dataset}"
        f"/year={config.run.year:04d}/month={config.run.month:02d}"
    )


def silver_partition_uri(config: PipelineConfig) -> str:
    """Build the configured HDFS Silver partition URI."""
    return _partition_uri(config, config.hdfs.silver_dataset)


def rejected_partition_uri(config: PipelineConfig) -> str:
    """Build the configured HDFS rejected-record partition URI."""
    return _partition_uri(config, config.hdfs.silver_rejected_dataset)


def quality_partition_uri(config: PipelineConfig) -> str:
    """Build the configured HDFS quality-metric partition URI."""
    return _partition_uri(config, config.hdfs.silver_quality_dataset)


def quality_metrics_uri(config: PipelineConfig) -> str:
    """Build the final quality-metric file URI."""
    return f"{quality_partition_uri(config)}/{QUALITY_FILE_NAME}"


def read_bronze_csv(spark: SparkSession, source_uri: str) -> DataFrame:
    """Read one manifested Bronze CSV with all inferred types disabled."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .csv(source_uri)
    )


def validate_source_contract(flights: DataFrame, config: PipelineConfig) -> None:
    """Reject a Bronze DataFrame that does not match the configured source shape."""
    duplicate_columns = sorted(
        column for column in set(flights.columns) if flights.columns.count(column) > 1
    )
    if duplicate_columns:
        names = ", ".join(duplicate_columns)
        raise SilverPipelineError(f"Duplicate source columns: {names}")

    missing_columns = sorted(set(REQUIRED_COLUMNS) - set(flights.columns))
    if missing_columns:
        names = ", ".join(missing_columns)
        raise SilverPipelineError(f"Missing required source columns: {names}")

    source_column_count = len(flights.columns)
    expected_column_count = config.quality.expected_source_columns
    if source_column_count != expected_column_count:
        raise SilverPipelineError(
            f"Expected {expected_column_count} source columns, "
            f"found {source_column_count}"
        )


def _blank(column: str):
    value = F.col(column)
    return value.isNull() | (F.trim(value) == "")


def _try_cast(column: str, data_type: str):
    return F.expr(f"try_cast(`{column}` AS {data_type})")


def _reason(condition, message: str):
    return F.when(condition, F.lit(message))


def classify_flights(flights: DataFrame, config: PipelineConfig) -> ClassifiedFlights:
    """Split raw rows into typed accepted rows and explained rejected rows."""
    year = _try_cast("Year", "INT")
    month = _try_cast("Month", "INT")
    reasons = [
        _reason(year.isNull(), "invalid_year"),
        _reason(year != config.run.year, "unexpected_year"),
        _reason(month.isNull(), "invalid_month"),
        _reason(month != config.run.month, "unexpected_month"),
        _reason(_try_cast("DayofMonth", "INT").isNull(), "invalid_day_of_month"),
        _reason(_try_cast("DayOfWeek", "INT").isNull(), "invalid_day_of_week"),
        _reason(_try_cast("FlightDate", "DATE").isNull(), "invalid_flight_date"),
        _reason(_blank("Reporting_Airline"), "missing_reporting_airline"),
        _reason(_blank("Origin"), "missing_origin"),
        _reason(_blank("Dest"), "missing_destination"),
    ]

    for column in (
        "DOT_ID_Reporting_Airline",
        "Flight_Number_Reporting_Airline",
        "OriginAirportID",
        "DestAirportID",
    ):
        reasons.append(
            _reason(_try_cast(column, "INT").isNull(), f"invalid_{column.lower()}")
        )

    for column in FLAG_COLUMNS:
        numeric_flag = _try_cast(column, "DOUBLE")
        reasons.append(
            _reason(
                numeric_flag.isNull() | ~numeric_flag.isin(0.0, 1.0),
                f"invalid_{column.lower()}",
            )
        )

    optional_numeric_columns = tuple(
        column for column in DOUBLE_COLUMNS if column not in {"Flights", "Distance"}
    )
    for column in optional_numeric_columns:
        reasons.append(
            _reason(
                ~_blank(column) & _try_cast(column, "DOUBLE").isNull(),
                f"invalid_{column.lower()}",
            )
        )

    flights_value = _try_cast("Flights", "DOUBLE")
    distance_value = _try_cast("Distance", "DOUBLE")
    reasons.extend(
        (
            _reason(
                flights_value.isNull() | (flights_value <= 0),
                "invalid_flights",
            ),
            _reason(
                distance_value.isNull() | (distance_value < 0),
                "invalid_distance",
            ),
        )
    )

    classified = flights.select(*REQUIRED_COLUMNS).withColumn(
        "rejection_reasons",
        F.array_compact(F.array(*reasons)),
    )
    accepted_raw = classified.filter(F.size("rejection_reasons") == 0).drop(
        "rejection_reasons"
    )
    rejected = classified.filter(F.size("rejection_reasons") > 0)
    return ClassifiedFlights(
        accepted=cast_columns(accepted_raw),
        rejected=rejected,
    )


def collect_quality_metrics(
    source: DataFrame,
    classified: ClassifiedFlights,
    config: PipelineConfig,
    manifest: IngestionManifest,
    processed_at: datetime | None = None,
) -> SilverQualityMetrics:
    """Count and reconcile one classified Silver partition."""
    input_rows = source.count()
    accepted_rows = classified.accepted.count()
    rejected_rows = classified.rejected.count()

    if (
        config.quality.expected_rows is not None
        and input_rows != config.quality.expected_rows
    ):
        raise SilverPipelineError(
            f"Expected {config.quality.expected_rows} input rows, found {input_rows}"
        )
    if input_rows != manifest.row_count:
        raise SilverPipelineError(
            f"Bronze manifest records {manifest.row_count} rows, found {input_rows}"
        )
    if input_rows != accepted_rows + rejected_rows:
        raise SilverPipelineError(
            "Silver row counts do not reconcile: "
            f"input={input_rows}, accepted={accepted_rows}, rejected={rejected_rows}"
        )

    reason_rows = (
        classified.rejected.select(F.explode("rejection_reasons").alias("reason"))
        .groupBy("reason")
        .count()
        .collect()
    )
    reason_counts = {row["reason"]: row["count"] for row in reason_rows}
    timestamp = processed_at or datetime.now(UTC)
    if timestamp.tzinfo is None:
        raise ValueError("processed_at must include a timezone")

    return SilverQualityMetrics(
        source_file_name=manifest.source_file_name,
        source_sha256=manifest.sha256,
        input_rows=input_rows,
        accepted_rows=accepted_rows,
        rejected_rows=rejected_rows,
        cancelled_rows=classified.accepted.filter(F.col("Cancelled")).count(),
        diverted_rows=classified.accepted.filter(F.col("Diverted")).count(),
        rejection_reason_counts=reason_counts,
        processed_at_utc=timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    )


def quality_metrics_to_json(metrics: SilverQualityMetrics) -> str:
    """Serialize Silver quality metrics in a stable, readable format."""
    return json.dumps(asdict(metrics), indent=2, sort_keys=True) + "\n"


def quality_metrics_from_json(content: str) -> SilverQualityMetrics:
    """Deserialize stored Silver quality metrics."""
    try:
        values: dict[str, Any] = json.loads(content)
        return SilverQualityMetrics(**values)
    except (json.JSONDecodeError, TypeError) as error:
        raise SilverPipelineError("Invalid Silver quality metrics") from error


def completed_silver_run(
    config: PipelineConfig,
    hdfs: HdfsClient,
    manifest: IngestionManifest,
) -> SilverQualityMetrics | None:
    """Return matching completed metrics or reject inconsistent final outputs."""
    metrics_path = quality_metrics_uri(config)
    accepted_exists = hdfs.exists(silver_partition_uri(config))
    rejected_exists = hdfs.exists(rejected_partition_uri(config))

    if not hdfs.exists(metrics_path):
        return None

    metrics = quality_metrics_from_json(hdfs.read_text(metrics_path))
    if metrics.source_sha256 != manifest.sha256:
        raise SilverPipelineError(
            "Silver quality metrics belong to a different Bronze source"
        )
    if not accepted_exists or not rejected_exists:
        raise SilverPipelineError(
            "Silver quality metrics exist, but a data output is missing"
        )
    return metrics


def write_silver_outputs(
    classified: ClassifiedFlights,
    metrics: SilverQualityMetrics,
    config: PipelineConfig,
    hdfs: HdfsClient,
) -> SilverWriteResult:
    """Write reproducible accepted, rejected, and quality outputs to HDFS."""
    accepted_output = classified.accepted.drop("Year", "Month")
    rejected_output = classified.rejected.withColumnsRenamed(
        {"Year": "SourceYear", "Month": "SourceMonth"}
    )

    accepted_output.write.mode("overwrite").parquet(silver_partition_uri(config))
    rejected_output.write.mode("overwrite").parquet(rejected_partition_uri(config))

    quality_directory = quality_partition_uri(config)
    hdfs.make_directory(quality_directory)
    hdfs.upload_text(
        quality_metrics_to_json(metrics),
        quality_metrics_uri(config),
    )
    return SilverWriteResult(status="written", metrics=metrics)
