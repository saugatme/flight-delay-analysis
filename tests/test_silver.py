from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from flight_delay_analysis.bronze import (
    IngestionManifest,
    bronze_partition_uri,
    manifest_to_json,
    manifest_uri,
)
from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.contracts import REQUIRED_COLUMNS
from flight_delay_analysis.silver import (
    SilverPipelineError,
    bronze_source_uri,
    classify_flights,
    collect_quality_metrics,
    completed_silver_run,
    quality_metrics_from_json,
    quality_metrics_to_json,
    quality_metrics_uri,
    quality_partition_uri,
    rejected_partition_uri,
    silver_partition_uri,
    validate_source_contract,
    write_silver_outputs,
)


class ReadOnlyMemoryHdfs:
    def __init__(self, files: dict[str, str]) -> None:
        self.files = files
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


def stored_manifest(pipeline_config) -> IngestionManifest:
    source_uri = f"{bronze_partition_uri(pipeline_config)}/flights.csv"
    return IngestionManifest(
        source_file_name="flights.csv",
        size_bytes=100,
        sha256="abc123",
        row_count=2,
        ingested_at_utc="2026-10-10T12:00:00Z",
        hdfs_file_uri=source_uri,
    )


def hdfs_with_manifest(pipeline_config, manifest: IngestionManifest):
    return ReadOnlyMemoryHdfs(
        {
            manifest_uri(pipeline_config): manifest_to_json(manifest),
            manifest.hdfs_file_uri: "source bytes are not read by this adapter",
        }
    )


def test_bronze_source_uri_returns_manifested_file(pipeline_config) -> None:
    manifest = stored_manifest(pipeline_config)

    assert bronze_source_uri(
        pipeline_config, hdfs_with_manifest(pipeline_config, manifest)
    ) == f"{bronze_partition_uri(pipeline_config)}/flights.csv"


def test_bronze_source_uri_rejects_missing_manifest(pipeline_config) -> None:
    with pytest.raises(SilverPipelineError, match="Bronze manifest not found"):
        bronze_source_uri(pipeline_config, ReadOnlyMemoryHdfs({}))


def test_bronze_source_uri_rejects_source_outside_partition(pipeline_config) -> None:
    manifest = replace(
        stored_manifest(pipeline_config),
        hdfs_file_uri="hdfs://localhost:9000/other/flights.csv",
    )

    with pytest.raises(SilverPipelineError, match="outside"):
        bronze_source_uri(
            pipeline_config, hdfs_with_manifest(pipeline_config, manifest)
        )


def test_bronze_source_uri_rejects_name_mismatch(pipeline_config) -> None:
    manifest = replace(stored_manifest(pipeline_config), source_file_name="other.csv")

    with pytest.raises(SilverPipelineError, match="source name"):
        bronze_source_uri(
            pipeline_config, hdfs_with_manifest(pipeline_config, manifest)
        )


def test_bronze_source_uri_rejects_missing_source_file(pipeline_config) -> None:
    manifest = stored_manifest(pipeline_config)
    hdfs = ReadOnlyMemoryHdfs(
        {manifest_uri(pipeline_config): manifest_to_json(manifest)}
    )

    with pytest.raises(SilverPipelineError, match="source file not found"):
        bronze_source_uri(pipeline_config, hdfs)


def test_silver_partition_uri_uses_configured_run(pipeline_config) -> None:
    assert silver_partition_uri(pipeline_config) == (
        "hdfs://localhost:9000/flight-delay/silver/flights/year=2025/month=01"
    )


def test_rejected_and_quality_uris_use_separate_datasets(pipeline_config) -> None:
    assert rejected_partition_uri(pipeline_config).endswith(
        "/silver/rejected_flights/year=2025/month=01"
    )
    assert quality_partition_uri(pipeline_config).endswith(
        "/silver/quality_metrics/year=2025/month=01"
    )
    assert quality_metrics_uri(pipeline_config).endswith(
        "/silver/quality_metrics/year=2025/month=01/quality_metrics.json"
    )


class DataFrameColumns:
    def __init__(self, columns: list[str]) -> None:
        self.columns = columns


def complete_source_columns() -> list[str]:
    extra_count = 110 - len(REQUIRED_COLUMNS)
    return [*REQUIRED_COLUMNS, *(f"Extra_{index}" for index in range(extra_count))]


def test_validate_source_contract_accepts_expected_shape(pipeline_config) -> None:
    validate_source_contract(DataFrameColumns(complete_source_columns()), pipeline_config)


def test_validate_source_contract_rejects_duplicate_columns(pipeline_config) -> None:
    columns = complete_source_columns()
    columns[-1] = "Origin"

    with pytest.raises(SilverPipelineError, match="Duplicate source columns: Origin"):
        validate_source_contract(DataFrameColumns(columns), pipeline_config)


def test_validate_source_contract_rejects_missing_required_column(
    pipeline_config,
) -> None:
    columns = complete_source_columns()
    columns.remove("Dest")
    columns.append("Replacement")

    with pytest.raises(SilverPipelineError, match="Missing.*Dest"):
        validate_source_contract(DataFrameColumns(columns), pipeline_config)


def test_validate_source_contract_rejects_wrong_source_width(pipeline_config) -> None:
    columns = complete_source_columns()
    columns.append("Unexpected")

    with pytest.raises(SilverPipelineError, match="Expected 110.*found 111"):
        validate_source_contract(DataFrameColumns(columns), pipeline_config)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("silver-tests").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def valid_source_row() -> dict[str, str]:
    return {
        "Year": "2025",
        "Month": "1",
        "DayofMonth": "1",
        "DayOfWeek": "3",
        "FlightDate": "2025-01-01",
        "Reporting_Airline": "AA",
        "DOT_ID_Reporting_Airline": "19805",
        "Flight_Number_Reporting_Airline": "1",
        "OriginAirportID": "12478",
        "Origin": "JFK",
        "OriginCityName": "New York, NY",
        "OriginState": "NY",
        "DestAirportID": "12892",
        "Dest": "LAX",
        "DestCityName": "Los Angeles, CA",
        "DestState": "CA",
        "CRSDepTime": "0900",
        "DepTimeBlk": "0900-0959",
        "DepDelay": "2.0",
        "DepDel15": "0.0",
        "ArrDelay": "5.0",
        "ArrDelayMinutes": "5.0",
        "ArrDel15": "0.0",
        "Cancelled": "0.0",
        "CancellationCode": "",
        "Diverted": "0.0",
        "Flights": "1.0",
        "Distance": "2475.0",
    }


def test_classify_flights_returns_typed_accepted_rows(
    pipeline_config, spark: SparkSession
) -> None:
    flights = spark.createDataFrame([valid_source_row()])

    result = classify_flights(flights, pipeline_config)
    accepted = result.accepted.first()

    assert result.accepted.count() == 1
    assert result.rejected.count() == 0
    assert result.accepted.schema["FlightDate"].dataType.simpleString() == "date"
    assert result.accepted.schema["Cancelled"].dataType.simpleString() == "boolean"
    assert accepted["Cancelled"] is False


def test_classify_flights_keeps_raw_rejected_values_and_reasons(
    pipeline_config, spark: SparkSession
) -> None:
    invalid = valid_source_row()
    invalid.update(
        {
            "Month": "2",
            "Origin": "",
            "ArrDelay": "not-a-number",
            "Cancelled": "2.0",
        }
    )
    flights = spark.createDataFrame([invalid])

    result = classify_flights(flights, pipeline_config)
    rejected = result.rejected.first()

    assert result.accepted.count() == 0
    assert rejected["ArrDelay"] == "not-a-number"
    assert set(rejected["rejection_reasons"]) == {
        "unexpected_month",
        "missing_origin",
        "invalid_arrdelay",
        "invalid_cancelled",
    }


def test_classify_flights_allows_empty_optional_delays(
    pipeline_config, spark: SparkSession
) -> None:
    cancelled = valid_source_row()
    cancelled.update(
        {
            "DepDelay": "",
            "DepDel15": "",
            "ArrDelay": "",
            "ArrDelayMinutes": "",
            "ArrDel15": "",
            "Cancelled": "1.0",
            "CancellationCode": "A",
        }
    )
    flights = spark.createDataFrame([cancelled])

    result = classify_flights(flights, pipeline_config)

    assert result.accepted.count() == 1
    assert result.rejected.count() == 0


def test_collect_quality_metrics_reconciles_rows(
    pipeline_config, spark: SparkSession
) -> None:
    valid = valid_source_row()
    invalid = valid_source_row()
    invalid["Origin"] = ""
    source = spark.createDataFrame([valid, invalid]).cache()
    config = replace(
        pipeline_config,
        quality=replace(pipeline_config.quality, expected_rows=2),
    )
    manifest = stored_manifest(config)
    classified = classify_flights(source, config)

    metrics = collect_quality_metrics(
        source,
        classified,
        config,
        manifest,
        datetime(2026, 10, 11, 8, 0, tzinfo=UTC),
    )

    assert metrics.input_rows == 2
    assert metrics.accepted_rows == 1
    assert metrics.rejected_rows == 1
    assert metrics.rejection_reason_counts == {"missing_origin": 1}
    assert metrics.processed_at_utc == "2026-10-11T08:00:00Z"


def test_quality_metrics_json_round_trip(
    pipeline_config, spark: SparkSession
) -> None:
    source = spark.createDataFrame([valid_source_row()])
    config = replace(
        pipeline_config,
        quality=replace(pipeline_config.quality, expected_rows=1),
    )
    manifest = replace(stored_manifest(config), row_count=1)
    metrics = collect_quality_metrics(
        source,
        classify_flights(source, config),
        config,
        manifest,
        datetime(2026, 10, 11, 8, 0, tzinfo=UTC),
    )

    assert quality_metrics_from_json(quality_metrics_to_json(metrics)) == metrics


def test_write_silver_outputs_writes_all_datasets(
    pipeline_config, spark: SparkSession, tmp_path: Path
) -> None:
    valid = valid_source_row()
    invalid = valid_source_row()
    invalid["Origin"] = ""
    source = spark.createDataFrame([valid, invalid])
    config = replace(
        pipeline_config,
        hdfs=replace(pipeline_config.hdfs, base_uri=tmp_path.as_uri()),
        quality=replace(pipeline_config.quality, expected_rows=2),
    )
    manifest = stored_manifest(config)
    classified = classify_flights(source, config)
    metrics = collect_quality_metrics(
        source,
        classified,
        config,
        manifest,
        datetime(2026, 10, 11, 8, 0, tzinfo=UTC),
    )
    hdfs = ReadOnlyMemoryHdfs({})

    result = write_silver_outputs(classified, metrics, config, hdfs)

    accepted = spark.read.parquet(silver_partition_uri(config))
    rejected = spark.read.parquet(rejected_partition_uri(config))
    assert result.status == "written"
    assert accepted.count() == 1
    assert "Year" not in accepted.columns
    assert "Month" not in accepted.columns
    assert rejected.count() == 1
    assert "SourceYear" in rejected.columns
    assert quality_metrics_uri(config) in hdfs.files


def test_completed_silver_run_returns_matching_metrics(
    pipeline_config, spark: SparkSession
) -> None:
    source = spark.createDataFrame([valid_source_row()])
    config = replace(
        pipeline_config,
        quality=replace(pipeline_config.quality, expected_rows=1),
    )
    manifest = replace(stored_manifest(config), row_count=1)
    metrics = collect_quality_metrics(
        source,
        classify_flights(source, config),
        config,
        manifest,
        datetime(2026, 10, 11, 8, 0, tzinfo=UTC),
    )
    hdfs = ReadOnlyMemoryHdfs(
        {quality_metrics_uri(config): quality_metrics_to_json(metrics)}
    )
    hdfs.directories.update(
        {silver_partition_uri(config), rejected_partition_uri(config)}
    )

    assert completed_silver_run(config, hdfs, manifest) == metrics
