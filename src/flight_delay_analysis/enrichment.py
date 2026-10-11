"""Enrich typed Silver flights with validated airport reference data."""

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flight_delay_analysis.airport_reference import (
    AirportReferenceError,
    validate_unique_iata,
)
from flight_delay_analysis.config import PipelineConfig
from flight_delay_analysis.hdfs import HdfsClient


class EnrichmentError(RuntimeError):
    """Report an invalid or inconsistent flight-enrichment run."""


@dataclass(frozen=True)
class EnrichmentMetrics:
    """Record join coverage and lineage for one enriched flight partition."""

    flight_source_sha256: str
    airport_source_sha256: str
    airport_reference_rows: int
    input_flight_rows: int
    output_flight_rows: int
    origin_matched_rows: int
    origin_unmatched_rows: int
    destination_matched_rows: int
    destination_unmatched_rows: int
    both_matched_rows: int
    either_unmatched_rows: int
    processed_at_utc: str


@dataclass(frozen=True)
class EnrichmentResult:
    """Describe whether enriched Silver output was written or reused."""

    status: str
    metrics: EnrichmentMetrics


@dataclass(frozen=True)
class EnrichmentReadback:
    """Store verification facts collected from enriched Silver Parquet."""

    output_rows: int
    output_columns: int
    origin_matched_rows: int
    destination_matched_rows: int
    both_matched_rows: int


ENRICHMENT_METRICS_FILE_NAME = "join_quality_metrics.json"
AIRPORT_REQUIRED_COLUMNS = {
    "IATA",
    "Name",
    "Country",
    "Latitude",
    "Longitude",
    "TzDatabaseTimezone",
}


def _require_columns(dataframe: DataFrame, required: set[str], label: str) -> None:
    missing = sorted(required - set(dataframe.columns))
    if missing:
        raise EnrichmentError(
            f"{label} is missing required columns: {', '.join(missing)}"
        )


def _airport_endpoint(airports: DataFrame, prefix: str) -> DataFrame:
    return airports.select(
        F.col("IATA").alias(f"_{prefix.lower()}_reference_iata"),
        F.col("Name").alias(f"{prefix}AirportName"),
        F.col("Country").alias(f"{prefix}AirportCountry"),
        F.col("Latitude").alias(f"{prefix}Latitude"),
        F.col("Longitude").alias(f"{prefix}Longitude"),
        F.col("TzDatabaseTimezone").alias(f"{prefix}TzDatabaseTimezone"),
    )


def enrich_flights(flights: DataFrame, airports: DataFrame) -> DataFrame:
    """Left join unique airport details to both flight endpoints."""
    _require_columns(flights, {"Origin", "Dest"}, "Silver flights")
    _require_columns(airports, AIRPORT_REQUIRED_COLUMNS, "Silver airports")
    try:
        validate_unique_iata(airports)
    except AirportReferenceError as error:
        raise EnrichmentError(str(error)) from error

    origin_airports = F.broadcast(_airport_endpoint(airports, "Origin"))
    destination_airports = F.broadcast(_airport_endpoint(airports, "Dest"))

    with_origin = flights.join(
        origin_airports,
        F.col("Origin") == F.col("_origin_reference_iata"),
        "left",
    )
    enriched = with_origin.join(
        destination_airports,
        F.col("Dest") == F.col("_dest_reference_iata"),
        "left",
    )
    return (
        enriched.withColumn(
            "OriginAirportMatched",
            F.col("_origin_reference_iata").isNotNull(),
        )
        .withColumn(
            "DestAirportMatched",
            F.col("_dest_reference_iata").isNotNull(),
        )
        .drop("_origin_reference_iata", "_dest_reference_iata")
    )


def collect_enrichment_metrics(
    flights: DataFrame,
    airports: DataFrame,
    enriched: DataFrame,
    expected_flight_rows: int,
    expected_airport_rows: int,
    flight_source_sha256: str,
    airport_source_sha256: str,
    processed_at: datetime | None = None,
) -> EnrichmentMetrics:
    """Count join coverage and require preservation of every flight row."""
    input_rows = flights.count()
    airport_rows = airports.count()
    if input_rows != expected_flight_rows:
        raise EnrichmentError(
            "Silver flight count does not match its quality metrics: "
            f"expected {expected_flight_rows}, found {input_rows}"
        )
    if airport_rows != expected_airport_rows:
        raise EnrichmentError(
            "Silver airport count does not match its quality metrics: "
            f"expected {expected_airport_rows}, found {airport_rows}"
        )

    counts = enriched.agg(
        F.count("*").alias("output_rows"),
        F.sum(F.col("OriginAirportMatched").cast("long")).alias("origin_matched"),
        F.sum(F.col("DestAirportMatched").cast("long")).alias("dest_matched"),
        F.sum(
            (F.col("OriginAirportMatched") & F.col("DestAirportMatched")).cast("long")
        ).alias("both_matched"),
    ).first()
    output_rows = counts["output_rows"]
    origin_matched = counts["origin_matched"] or 0
    destination_matched = counts["dest_matched"] or 0
    both_matched = counts["both_matched"] or 0
    if output_rows != input_rows:
        raise EnrichmentError(
            "Airport joins changed the flight row count: "
            f"input={input_rows}, output={output_rows}"
        )

    timestamp = processed_at or datetime.now(UTC)
    if timestamp.tzinfo is None:
        raise ValueError("processed_at must include a timezone")

    return EnrichmentMetrics(
        flight_source_sha256=flight_source_sha256,
        airport_source_sha256=airport_source_sha256,
        airport_reference_rows=airport_rows,
        input_flight_rows=input_rows,
        output_flight_rows=output_rows,
        origin_matched_rows=origin_matched,
        origin_unmatched_rows=output_rows - origin_matched,
        destination_matched_rows=destination_matched,
        destination_unmatched_rows=output_rows - destination_matched,
        both_matched_rows=both_matched,
        either_unmatched_rows=output_rows - both_matched,
        processed_at_utc=timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    )


def _versioned_run_uri(
    config: PipelineConfig,
    dataset: str,
    flight_sha256: str,
    airport_sha256: str,
) -> str:
    return (
        f"{config.hdfs.base_uri}/{dataset}"
        f"/year={config.run.year:04d}/month={config.run.month:02d}"
        f"/flight_source_sha256={flight_sha256}"
        f"/airport_source_sha256={airport_sha256}"
    )


def enriched_flights_uri(
    config: PipelineConfig,
    flight_sha256: str,
    airport_sha256: str,
) -> str:
    """Build the versioned enriched-flight Parquet path."""
    return _versioned_run_uri(
        config,
        config.hdfs.enriched_silver_dataset,
        flight_sha256,
        airport_sha256,
    )


def enrichment_metrics_uri(
    config: PipelineConfig,
    flight_sha256: str,
    airport_sha256: str,
) -> str:
    """Build the versioned join-quality JSON path."""
    directory = _versioned_run_uri(
        config,
        config.hdfs.enrichment_quality_dataset,
        flight_sha256,
        airport_sha256,
    )
    return f"{directory}/{ENRICHMENT_METRICS_FILE_NAME}"


def enrichment_metrics_to_json(metrics: EnrichmentMetrics) -> str:
    """Serialize join-quality metrics in a stable, readable format."""
    return json.dumps(asdict(metrics), indent=2, sort_keys=True) + "\n"


def enrichment_metrics_from_json(content: str) -> EnrichmentMetrics:
    """Deserialize stored join-quality metrics."""
    try:
        values: dict[str, Any] = json.loads(content)
        return EnrichmentMetrics(**values)
    except (json.JSONDecodeError, TypeError) as error:
        raise EnrichmentError("Invalid enrichment quality metrics") from error


def completed_enrichment_run(
    config: PipelineConfig,
    hdfs: HdfsClient,
    flight_sha256: str,
    airport_sha256: str,
) -> EnrichmentMetrics | None:
    """Return completed metrics or reject inconsistent enrichment outputs."""
    output_path = enriched_flights_uri(
        config,
        flight_sha256,
        airport_sha256,
    )
    metrics_path = enrichment_metrics_uri(
        config,
        flight_sha256,
        airport_sha256,
    )
    output_exists = hdfs.exists(output_path)
    if not hdfs.exists(metrics_path):
        if output_exists:
            raise EnrichmentError(
                "Enriched flights exist without completed quality metrics"
            )
        return None

    metrics = enrichment_metrics_from_json(hdfs.read_text(metrics_path))
    if metrics.flight_source_sha256 != flight_sha256:
        raise EnrichmentError("Enrichment metrics have a different flight source")
    if metrics.airport_source_sha256 != airport_sha256:
        raise EnrichmentError("Enrichment metrics have a different airport source")
    if not output_exists:
        raise EnrichmentError(
            "Enrichment quality metrics exist, but enriched flights are missing"
        )
    return metrics


def write_enrichment_outputs(
    enriched: DataFrame,
    metrics: EnrichmentMetrics,
    config: PipelineConfig,
    hdfs: HdfsClient,
) -> EnrichmentResult:
    """Write enriched Parquet and its join-quality record."""
    output_path = enriched_flights_uri(
        config,
        metrics.flight_source_sha256,
        metrics.airport_source_sha256,
    )
    metrics_path = enrichment_metrics_uri(
        config,
        metrics.flight_source_sha256,
        metrics.airport_source_sha256,
    )
    if hdfs.exists(output_path) or hdfs.exists(metrics_path):
        raise EnrichmentError(
            "Enrichment output path already exists without a completed run"
        )

    enriched.write.mode("errorifexists").parquet(output_path)
    metrics_directory = metrics_path.rsplit("/", maxsplit=1)[0]
    hdfs.make_directory(metrics_directory)
    hdfs.upload_text(enrichment_metrics_to_json(metrics), metrics_path)
    return EnrichmentResult(status="written", metrics=metrics)


def validate_enrichment_readback(
    enriched: DataFrame,
    metrics: EnrichmentMetrics,
) -> EnrichmentReadback:
    """Verify persisted enriched-flight counts, coverage, and field types."""
    required_types = {
        "OriginLatitude": "double",
        "OriginLongitude": "double",
        "DestLatitude": "double",
        "DestLongitude": "double",
        "OriginAirportMatched": "boolean",
        "DestAirportMatched": "boolean",
    }
    actual_types = {
        field.name: field.dataType.simpleString() for field in enriched.schema.fields
    }
    type_errors = {
        column: (expected_type, actual_types.get(column))
        for column, expected_type in required_types.items()
        if actual_types.get(column) != expected_type
    }
    if type_errors:
        raise EnrichmentError(
            f"Enriched Silver readback has unexpected types: {type_errors}"
        )

    counts = enriched.agg(
        F.count("*").alias("output_rows"),
        F.sum(F.col("OriginAirportMatched").cast("long")).alias("origin_matched"),
        F.sum(F.col("DestAirportMatched").cast("long")).alias("dest_matched"),
        F.sum(
            (F.col("OriginAirportMatched") & F.col("DestAirportMatched")).cast("long")
        ).alias("both_matched"),
    ).first()
    readback = EnrichmentReadback(
        output_rows=counts["output_rows"],
        output_columns=len(enriched.columns),
        origin_matched_rows=counts["origin_matched"] or 0,
        destination_matched_rows=counts["dest_matched"] or 0,
        both_matched_rows=counts["both_matched"] or 0,
    )

    expected = (
        metrics.output_flight_rows,
        metrics.origin_matched_rows,
        metrics.destination_matched_rows,
        metrics.both_matched_rows,
    )
    actual = (
        readback.output_rows,
        readback.origin_matched_rows,
        readback.destination_matched_rows,
        readback.both_matched_rows,
    )
    if actual != expected:
        raise EnrichmentError(
            "Enriched Silver readback does not match join-quality metrics: "
            f"expected={expected}, found={actual}"
        )
    return readback
