from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.enrichment import (
    EnrichmentError,
    collect_enrichment_metrics,
    completed_enrichment_run,
    enrich_flights,
    enriched_flights_uri,
    enrichment_metrics_from_json,
    enrichment_metrics_to_json,
    enrichment_metrics_uri,
    validate_enrichment_readback,
    write_enrichment_outputs,
)


class MemoryHdfsClient:
    def __init__(self) -> None:
        self.files: dict[str, str] = {}
        self.directories: set[str] = set()

    def exists(self, uri: str) -> bool:
        return uri in self.files or uri in self.directories

    def read_text(self, uri: str) -> str:
        return self.files[uri]

    def make_directory(self, uri: str) -> None:
        self.directories.add(uri)

    def upload_text(self, content: str, uri: str) -> None:
        self.files[uri] = content


@pytest.fixture
def pipeline_config():
    return load_pipeline_config(Path("config/pipeline.yml"))


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("enrichment-tests")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def small_flights(spark: SparkSession):
    return spark.createDataFrame(
        [
            (1, "AAA", "BBB"),
            (2, "AAA", "ZZZ"),
        ],
        ["FlightID", "Origin", "Dest"],
    )


def small_airports(spark: SparkSession):
    return spark.createDataFrame(
        [
            ("AAA", "Airport A", "Country A", 10.0, 20.0, "Zone/A"),
            ("BBB", "Airport B", "Country B", 30.0, 40.0, "Zone/B"),
        ],
        [
            "IATA",
            "Name",
            "Country",
            "Latitude",
            "Longitude",
            "TzDatabaseTimezone",
        ],
    )


def test_enrich_flights_preserves_rows_and_matches_both_endpoints(
    spark: SparkSession,
) -> None:
    enriched = enrich_flights(small_flights(spark), small_airports(spark))
    rows = {row["FlightID"]: row for row in enriched.collect()}

    assert len(rows) == 2
    assert rows[1]["OriginAirportName"] == "Airport A"
    assert rows[1]["DestAirportName"] == "Airport B"
    assert rows[1]["OriginAirportMatched"] is True
    assert rows[1]["DestAirportMatched"] is True
    assert rows[2]["OriginAirportMatched"] is True
    assert rows[2]["DestAirportMatched"] is False
    assert rows[2]["DestAirportName"] is None


def test_enrich_flights_rejects_duplicate_reference_key(
    spark: SparkSession,
) -> None:
    airports = small_airports(spark).union(small_airports(spark).limit(1))

    with pytest.raises(EnrichmentError, match="Duplicate accepted IATA codes"):
        enrich_flights(small_flights(spark), airports)


def test_collect_enrichment_metrics_records_join_coverage(
    spark: SparkSession,
) -> None:
    flights = small_flights(spark).cache()
    airports = small_airports(spark).cache()
    enriched = enrich_flights(flights, airports).cache()

    metrics = collect_enrichment_metrics(
        flights,
        airports,
        enriched,
        expected_flight_rows=2,
        expected_airport_rows=2,
        flight_source_sha256="flight-sha",
        airport_source_sha256="airport-sha",
        processed_at=datetime(2026, 10, 11, 18, 0, tzinfo=UTC),
    )

    assert metrics.input_flight_rows == 2
    assert metrics.output_flight_rows == 2
    assert metrics.origin_matched_rows == 2
    assert metrics.origin_unmatched_rows == 0
    assert metrics.destination_matched_rows == 1
    assert metrics.destination_unmatched_rows == 1
    assert metrics.both_matched_rows == 1
    assert metrics.either_unmatched_rows == 1
    assert enrichment_metrics_from_json(enrichment_metrics_to_json(metrics)) == metrics


def test_enrichment_paths_include_run_and_both_source_checksums(
    pipeline_config,
) -> None:
    output = enriched_flights_uri(
        pipeline_config,
        "flight-sha",
        "airport-sha",
    )
    quality = enrichment_metrics_uri(
        pipeline_config,
        "flight-sha",
        "airport-sha",
    )

    assert "/year=2025/month=01/" in output
    assert output.endswith(
        "/flight_source_sha256=flight-sha/airport_source_sha256=airport-sha"
    )
    assert quality.endswith(
        "/flight_source_sha256=flight-sha/airport_source_sha256=airport-sha/"
        "join_quality_metrics.json"
    )


def test_write_and_reuse_enrichment_output(
    pipeline_config,
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = replace(
        pipeline_config,
        hdfs=replace(pipeline_config.hdfs, base_uri=tmp_path.as_uri()),
    )
    flights = small_flights(spark).cache()
    airports = small_airports(spark).cache()
    enriched = enrich_flights(flights, airports).cache()
    metrics = collect_enrichment_metrics(
        flights,
        airports,
        enriched,
        expected_flight_rows=2,
        expected_airport_rows=2,
        flight_source_sha256="flight-sha",
        airport_source_sha256="airport-sha",
        processed_at=datetime(2026, 10, 11, 18, 0, tzinfo=UTC),
    )
    hdfs = MemoryHdfsClient()

    result = write_enrichment_outputs(enriched, metrics, config, hdfs)

    output_path = enriched_flights_uri(config, "flight-sha", "airport-sha")
    stored = spark.read.parquet(output_path)
    assert result.status == "written"
    assert stored.count() == 2
    assert stored.schema["OriginLatitude"].dataType.simpleString() == "double"

    hdfs.directories.add(output_path)
    assert (
        completed_enrichment_run(
            config,
            hdfs,
            "flight-sha",
            "airport-sha",
        )
        == metrics
    )


def test_completed_enrichment_run_rejects_partial_output(
    pipeline_config,
) -> None:
    hdfs = MemoryHdfsClient()
    hdfs.directories.add(
        enriched_flights_uri(pipeline_config, "flight-sha", "airport-sha")
    )

    with pytest.raises(EnrichmentError, match="without completed"):
        completed_enrichment_run(
            pipeline_config,
            hdfs,
            "flight-sha",
            "airport-sha",
        )


def test_validate_enrichment_readback_matches_quality_metrics(
    spark: SparkSession,
) -> None:
    flights = small_flights(spark).cache()
    airports = small_airports(spark).cache()
    enriched = enrich_flights(flights, airports).cache()
    metrics = collect_enrichment_metrics(
        flights,
        airports,
        enriched,
        expected_flight_rows=2,
        expected_airport_rows=2,
        flight_source_sha256="flight-sha",
        airport_source_sha256="airport-sha",
        processed_at=datetime(2026, 10, 11, 18, 0, tzinfo=UTC),
    )

    readback = validate_enrichment_readback(enriched, metrics)

    assert readback.output_rows == 2
    assert readback.origin_matched_rows == 2
    assert readback.destination_matched_rows == 1
    assert readback.both_matched_rows == 1


def test_validate_enrichment_readback_rejects_changed_count(
    spark: SparkSession,
) -> None:
    flights = small_flights(spark).cache()
    airports = small_airports(spark).cache()
    enriched = enrich_flights(flights, airports).cache()
    metrics = collect_enrichment_metrics(
        flights,
        airports,
        enriched,
        expected_flight_rows=2,
        expected_airport_rows=2,
        flight_source_sha256="flight-sha",
        airport_source_sha256="airport-sha",
        processed_at=datetime(2026, 10, 11, 18, 0, tzinfo=UTC),
    )

    with pytest.raises(EnrichmentError, match="does not match"):
        validate_enrichment_readback(enriched.limit(1), metrics)
