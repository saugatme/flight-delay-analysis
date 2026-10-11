"""Read, validate, and write the manifested OpenFlights reference source."""

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from flight_delay_analysis.config import PipelineConfig
from flight_delay_analysis.hdfs import HdfsClient
from flight_delay_analysis.reference import (
    ReferenceManifest,
    reference_bronze_uri,
    reference_manifest_from_json,
    reference_manifest_uri,
)


class AirportReferenceError(RuntimeError):
    """Report an invalid OpenFlights Bronze input or manifest."""


@dataclass(frozen=True)
class ClassifiedAirports:
    """Hold join-ready typed airports and explained rejected source rows."""

    accepted: DataFrame
    rejected: DataFrame


@dataclass(frozen=True)
class AirportQualityMetrics:
    """Store reconciled reference counts and source lineage."""

    source_name: str
    source_file_name: str
    source_sha256: str
    input_rows: int
    accepted_rows: int
    rejected_rows: int
    rejection_reason_counts: dict[str, int]
    processed_at_utc: str


@dataclass(frozen=True)
class AirportSilverResult:
    """Describe whether reference Silver outputs were written or reused."""

    status: str
    metrics: AirportQualityMetrics


@dataclass(frozen=True)
class AirportSilverReadback:
    """Store verification facts collected from persisted Silver Parquet."""

    accepted_rows: int
    rejected_rows: int
    distinct_iata_codes: int
    accepted_columns: int


OPENFLIGHTS_COLUMNS = (
    "AirportID",
    "Name",
    "City",
    "Country",
    "IATA",
    "ICAO",
    "Latitude",
    "Longitude",
    "Altitude",
    "Timezone",
    "DST",
    "TzDatabaseTimezone",
    "Type",
    "Source",
)
AIRPORT_QUALITY_FILE_NAME = "quality_metrics.json"


def openflights_raw_schema() -> StructType:
    """Define the source field order without inferring or converting values."""
    return StructType(
        [
            StructField(column, StringType(), nullable=True)
            for column in OPENFLIGHTS_COLUMNS
        ]
    )


def load_reference_manifest(
    config: PipelineConfig,
    hdfs: HdfsClient,
) -> ReferenceManifest:
    """Load the reference manifest and verify that it matches configuration."""
    stored_manifest_uri = reference_manifest_uri(config)
    if not hdfs.exists(stored_manifest_uri):
        raise AirportReferenceError(
            f"Reference Bronze manifest not found: {stored_manifest_uri}"
        )

    manifest = reference_manifest_from_json(hdfs.read_text(stored_manifest_uri))
    expected_source_uri = f"{reference_bronze_uri(config)}/{config.reference.file_name}"
    if manifest.source_name != config.reference.name:
        raise AirportReferenceError(
            "Reference manifest source name does not match configuration"
        )
    if manifest.source_url != config.reference.source_url:
        raise AirportReferenceError(
            "Reference manifest source URL does not match configuration"
        )
    if manifest.source_file_name != config.reference.file_name:
        raise AirportReferenceError(
            "Reference manifest file name does not match configuration"
        )
    if manifest.hdfs_file_uri != expected_source_uri:
        raise AirportReferenceError(
            "Reference manifest file URI does not match its Bronze directory"
        )
    if manifest.column_count != config.reference.expected_columns:
        raise AirportReferenceError(
            "Reference manifest column count does not match configuration"
        )
    if not hdfs.exists(manifest.hdfs_file_uri):
        raise AirportReferenceError(
            f"Reference Bronze source not found: {manifest.hdfs_file_uri}"
        )
    return manifest


def read_reference_bronze(
    spark: SparkSession,
    source_uri: str,
    config: PipelineConfig,
) -> DataFrame:
    """Read headerless OpenFlights data as named text fields."""
    return (
        spark.read.schema(openflights_raw_schema())
        .option("header", False)
        .option("mode", "FAILFAST")
        .option("encoding", "UTF-8")
        .option("nullValue", config.reference.missing_value)
        .csv(source_uri)
    )


def validate_reference_row_count(
    airports: DataFrame,
    manifest: ReferenceManifest,
) -> int:
    """Require Spark's parsed row count to match the Bronze manifest."""
    parsed_rows = airports.count()
    if parsed_rows != manifest.row_count:
        raise AirportReferenceError(
            "Reference row count does not match its Bronze manifest: "
            f"expected {manifest.row_count}, found {parsed_rows}"
        )
    return parsed_rows


def _blank(column: str):
    value = F.col(column)
    return value.isNull() | (F.trim(value) == "")


def _try_cast(column: str, data_type: str):
    return F.expr(f"try_cast(`{column}` AS {data_type})")


def _reason(condition, message: str):
    return F.when(condition, F.lit(message))


def classify_airports(airports: DataFrame) -> ClassifiedAirports:
    """Split raw airports into typed join-ready and explained rejected rows."""
    airport_id = _try_cast("AirportID", "INT")
    latitude = _try_cast("Latitude", "DOUBLE")
    longitude = _try_cast("Longitude", "DOUBLE")
    altitude = _try_cast("Altitude", "INT")
    timezone = _try_cast("Timezone", "DOUBLE")

    reasons = [
        _reason(
            airport_id.isNull() | (airport_id <= 0),
            "invalid_airport_id",
        ),
        _reason(_blank("Name"), "missing_name"),
        _reason(_blank("IATA"), "missing_iata"),
        _reason(
            ~_blank("IATA") & ~F.trim(F.col("IATA")).rlike("^[A-Z]{3}$"),
            "invalid_iata",
        ),
        _reason(_blank("Latitude"), "missing_latitude"),
        _reason(
            ~_blank("Latitude") & latitude.isNull(),
            "invalid_latitude",
        ),
        _reason(
            latitude.isNotNull() & ((latitude < -90.0) | (latitude > 90.0)),
            "latitude_out_of_range",
        ),
        _reason(_blank("Longitude"), "missing_longitude"),
        _reason(
            ~_blank("Longitude") & longitude.isNull(),
            "invalid_longitude",
        ),
        _reason(
            longitude.isNotNull() & ((longitude < -180.0) | (longitude > 180.0)),
            "longitude_out_of_range",
        ),
        _reason(
            ~_blank("Altitude") & altitude.isNull(),
            "invalid_altitude",
        ),
        _reason(
            ~_blank("Timezone") & timezone.isNull(),
            "invalid_timezone",
        ),
        _reason(
            _blank("Type") | (F.lower(F.trim(F.col("Type"))) != "airport"),
            "invalid_type",
        ),
    ]

    classified = airports.withColumn(
        "rejection_reasons",
        F.array_compact(F.array(*reasons)),
    )
    accepted_raw = classified.filter(F.size("rejection_reasons") == 0).drop(
        "rejection_reasons"
    )
    rejected = classified.filter(F.size("rejection_reasons") > 0)

    accepted = accepted_raw.select(
        airport_id.alias("AirportID"),
        F.trim(F.col("Name")).alias("Name"),
        F.trim(F.col("City")).alias("City"),
        F.trim(F.col("Country")).alias("Country"),
        F.trim(F.col("IATA")).alias("IATA"),
        F.trim(F.col("ICAO")).alias("ICAO"),
        latitude.alias("Latitude"),
        longitude.alias("Longitude"),
        altitude.alias("Altitude"),
        timezone.alias("Timezone"),
        F.trim(F.col("DST")).alias("DST"),
        F.trim(F.col("TzDatabaseTimezone")).alias("TzDatabaseTimezone"),
        F.trim(F.col("Type")).alias("Type"),
        F.trim(F.col("Source")).alias("Source"),
    )
    return ClassifiedAirports(accepted=accepted, rejected=rejected)


def validate_unique_iata(airports: DataFrame) -> None:
    """Stop when an IATA code could create a one-to-many flight join."""
    duplicate_rows = (
        airports.groupBy("IATA")
        .count()
        .filter(F.col("count") > 1)
        .orderBy("IATA")
        .collect()
    )
    if duplicate_rows:
        details = ", ".join(f"{row['IATA']} ({row['count']})" for row in duplicate_rows)
        raise AirportReferenceError(f"Duplicate accepted IATA codes: {details}")


def _versioned_reference_uri(
    config: PipelineConfig,
    dataset: str,
    manifest: ReferenceManifest,
) -> str:
    return f"{config.hdfs.base_uri}/{dataset}/source_sha256={manifest.sha256}"


def airport_silver_uri(
    config: PipelineConfig,
    manifest: ReferenceManifest,
) -> str:
    """Build the checksum-versioned accepted airport path."""
    return _versioned_reference_uri(
        config,
        config.reference.hdfs_silver_dataset,
        manifest,
    )


def airport_rejected_uri(
    config: PipelineConfig,
    manifest: ReferenceManifest,
) -> str:
    """Build the checksum-versioned rejected airport path."""
    return _versioned_reference_uri(
        config,
        config.reference.hdfs_rejected_dataset,
        manifest,
    )


def airport_quality_uri(
    config: PipelineConfig,
    manifest: ReferenceManifest,
) -> str:
    """Build the checksum-versioned airport quality file path."""
    directory = _versioned_reference_uri(
        config,
        config.reference.hdfs_quality_dataset,
        manifest,
    )
    return f"{directory}/{AIRPORT_QUALITY_FILE_NAME}"


def collect_airport_quality_metrics(
    source: DataFrame,
    classified: ClassifiedAirports,
    manifest: ReferenceManifest,
    processed_at: datetime | None = None,
) -> AirportQualityMetrics:
    """Count, reconcile, and describe one airport-reference Silver run."""
    input_rows = source.count()
    accepted_rows = classified.accepted.count()
    rejected_rows = classified.rejected.count()

    if input_rows != manifest.row_count:
        raise AirportReferenceError(
            "Reference input count does not match its Bronze manifest: "
            f"expected {manifest.row_count}, found {input_rows}"
        )
    if input_rows != accepted_rows + rejected_rows:
        raise AirportReferenceError(
            "Airport reference counts do not reconcile: "
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

    return AirportQualityMetrics(
        source_name=manifest.source_name,
        source_file_name=manifest.source_file_name,
        source_sha256=manifest.sha256,
        input_rows=input_rows,
        accepted_rows=accepted_rows,
        rejected_rows=rejected_rows,
        rejection_reason_counts=reason_counts,
        processed_at_utc=timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    )


def airport_quality_to_json(metrics: AirportQualityMetrics) -> str:
    """Serialize airport quality metrics in a stable, readable format."""
    return json.dumps(asdict(metrics), indent=2, sort_keys=True) + "\n"


def airport_quality_from_json(content: str) -> AirportQualityMetrics:
    """Deserialize stored airport quality metrics."""
    try:
        values: dict[str, Any] = json.loads(content)
        return AirportQualityMetrics(**values)
    except (json.JSONDecodeError, TypeError) as error:
        raise AirportReferenceError("Invalid airport quality metrics") from error


def completed_airport_silver_run(
    config: PipelineConfig,
    hdfs: HdfsClient,
    manifest: ReferenceManifest,
) -> AirportQualityMetrics | None:
    """Return completed matching metrics or reject partial final outputs."""
    accepted_path = airport_silver_uri(config, manifest)
    rejected_path = airport_rejected_uri(config, manifest)
    metrics_path = airport_quality_uri(config, manifest)
    accepted_exists = hdfs.exists(accepted_path)
    rejected_exists = hdfs.exists(rejected_path)

    if not hdfs.exists(metrics_path):
        if accepted_exists or rejected_exists:
            raise AirportReferenceError(
                "Airport Silver data exists without completed quality metrics"
            )
        return None

    metrics = airport_quality_from_json(hdfs.read_text(metrics_path))
    if metrics.source_sha256 != manifest.sha256:
        raise AirportReferenceError(
            "Airport Silver quality metrics belong to a different Bronze source"
        )
    if not accepted_exists or not rejected_exists:
        raise AirportReferenceError(
            "Airport Silver quality metrics exist, but a data output is missing"
        )
    return metrics


def write_airport_silver_outputs(
    classified: ClassifiedAirports,
    metrics: AirportQualityMetrics,
    config: PipelineConfig,
    manifest: ReferenceManifest,
    hdfs: HdfsClient,
) -> AirportSilverResult:
    """Write typed, rejected, and quality airport reference outputs."""
    accepted_path = airport_silver_uri(config, manifest)
    rejected_path = airport_rejected_uri(config, manifest)
    metrics_path = airport_quality_uri(config, manifest)
    if (
        hdfs.exists(accepted_path)
        or hdfs.exists(rejected_path)
        or hdfs.exists(metrics_path)
    ):
        raise AirportReferenceError(
            "Airport Silver output path already exists without a completed run"
        )

    classified.accepted.write.mode("errorifexists").parquet(accepted_path)
    classified.rejected.write.mode("errorifexists").parquet(rejected_path)

    metrics_directory = metrics_path.rsplit("/", maxsplit=1)[0]
    hdfs.make_directory(metrics_directory)
    hdfs.upload_text(airport_quality_to_json(metrics), metrics_path)
    return AirportSilverResult(status="written", metrics=metrics)


def validate_airport_silver_readback(
    accepted: DataFrame,
    rejected: DataFrame,
    metrics: AirportQualityMetrics,
) -> AirportSilverReadback:
    """Verify persisted airport Parquet counts, keys, and important types."""
    accepted_rows = accepted.count()
    rejected_rows = rejected.count()
    if accepted_rows != metrics.accepted_rows:
        raise AirportReferenceError(
            "Accepted airport readback count does not match quality metrics: "
            f"expected {metrics.accepted_rows}, found {accepted_rows}"
        )
    if rejected_rows != metrics.rejected_rows:
        raise AirportReferenceError(
            "Rejected airport readback count does not match quality metrics: "
            f"expected {metrics.rejected_rows}, found {rejected_rows}"
        )
    if accepted_rows + rejected_rows != metrics.input_rows:
        raise AirportReferenceError(
            "Airport Silver readback counts do not reconcile with input rows"
        )

    expected_types = {
        "AirportID": "int",
        "Latitude": "double",
        "Longitude": "double",
        "Altitude": "int",
        "Timezone": "double",
    }
    actual_types = {
        field.name: field.dataType.simpleString() for field in accepted.schema.fields
    }
    type_errors = {
        column: (expected_type, actual_types.get(column))
        for column, expected_type in expected_types.items()
        if actual_types.get(column) != expected_type
    }
    if type_errors:
        raise AirportReferenceError(
            f"Airport Silver readback has unexpected types: {type_errors}"
        )
    if "rejection_reasons" not in rejected.columns:
        raise AirportReferenceError(
            "Rejected airport readback is missing rejection_reasons"
        )

    distinct_iata_codes = accepted.select("IATA").distinct().count()
    if distinct_iata_codes != accepted_rows:
        raise AirportReferenceError(
            "Accepted airport readback contains duplicate IATA codes"
        )

    return AirportSilverReadback(
        accepted_rows=accepted_rows,
        rejected_rows=rejected_rows,
        distinct_iata_codes=distinct_iata_codes,
        accepted_columns=len(accepted.columns),
    )
