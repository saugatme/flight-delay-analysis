from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from flight_delay_analysis.airport_reference import (
    OPENFLIGHTS_COLUMNS,
    AirportQualityMetrics,
    AirportReferenceError,
    airport_quality_from_json,
    airport_quality_to_json,
    airport_quality_uri,
    airport_rejected_uri,
    airport_silver_uri,
    classify_airports,
    collect_airport_quality_metrics,
    completed_airport_silver_run,
    load_reference_manifest,
    openflights_raw_schema,
    read_reference_bronze,
    validate_airport_silver_readback,
    validate_reference_row_count,
    validate_unique_iata,
    write_airport_silver_outputs,
)
from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.reference import (
    ReferenceManifest,
    reference_bronze_uri,
    reference_manifest_to_json,
    reference_manifest_uri,
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


def stored_manifest(pipeline_config) -> ReferenceManifest:
    return ReferenceManifest(
        source_name=pipeline_config.reference.name,
        source_url=pipeline_config.reference.source_url,
        source_file_name="airports.dat",
        size_bytes=200,
        sha256="abc123",
        row_count=2,
        column_count=14,
        retrieved_at_utc="2026-10-11T12:00:00Z",
        ingested_at_utc="2026-10-11T12:00:00Z",
        license=pipeline_config.reference.license,
        license_url=pipeline_config.reference.license_url,
        hdfs_file_uri=(f"{reference_bronze_uri(pipeline_config)}/airports.dat"),
    )


def hdfs_with_manifest(pipeline_config, manifest: ReferenceManifest):
    return ReadOnlyMemoryHdfs(
        {
            reference_manifest_uri(pipeline_config): reference_manifest_to_json(
                manifest
            ),
            manifest.hdfs_file_uri: "source bytes are not read by this adapter",
        }
    )


def test_openflights_raw_schema_has_official_field_order() -> None:
    schema = openflights_raw_schema()

    assert tuple(schema.fieldNames()) == OPENFLIGHTS_COLUMNS
    assert len(schema) == 14
    assert {field.dataType.simpleString() for field in schema} == {"string"}


def test_load_reference_manifest_accepts_matching_source(pipeline_config) -> None:
    manifest = stored_manifest(pipeline_config)

    assert (
        load_reference_manifest(
            pipeline_config,
            hdfs_with_manifest(pipeline_config, manifest),
        )
        == manifest
    )


def test_load_reference_manifest_rejects_wrong_source_uri(pipeline_config) -> None:
    manifest = replace(
        stored_manifest(pipeline_config),
        hdfs_file_uri="hdfs://localhost:9000/other/airports.dat",
    )

    with pytest.raises(AirportReferenceError, match="file URI"):
        load_reference_manifest(
            pipeline_config,
            hdfs_with_manifest(pipeline_config, manifest),
        )


def test_load_reference_manifest_rejects_wrong_column_count(pipeline_config) -> None:
    manifest = replace(stored_manifest(pipeline_config), column_count=13)

    with pytest.raises(AirportReferenceError, match="column count"):
        load_reference_manifest(
            pipeline_config,
            hdfs_with_manifest(pipeline_config, manifest),
        )


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("airport-reference-tests")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_read_reference_bronze_names_fields_and_converts_missing_marker(
    pipeline_config,
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    source = tmp_path / "airports.dat"
    source.write_text(
        '1,"Airport, One",City,Country,AAA,AAAA,1.0,2.0,3,4,E,Zone,airport,OurAirports\n'
        "2,Airport Two,City,Country,\\N,BBBB,5.0,6.0,7,8,U,Zone,airport,OurAirports\n",
        encoding="utf-8",
    )

    airports = read_reference_bronze(spark, source.as_uri(), pipeline_config)
    rows = airports.collect()

    assert airports.columns == list(OPENFLIGHTS_COLUMNS)
    assert rows[0]["Name"] == "Airport, One"
    assert rows[1]["IATA"] is None


def test_validate_reference_row_count_accepts_manifest_count(
    pipeline_config,
    spark: SparkSession,
) -> None:
    airports = spark.createDataFrame([("1",), ("2",)], ["AirportID"])

    assert (
        validate_reference_row_count(
            airports,
            stored_manifest(pipeline_config),
        )
        == 2
    )


def test_validate_reference_row_count_rejects_mismatch(
    pipeline_config,
    spark: SparkSession,
) -> None:
    airports = spark.createDataFrame([("1",)], ["AirportID"])

    with pytest.raises(AirportReferenceError, match="expected 2, found 1"):
        validate_reference_row_count(
            airports,
            stored_manifest(pipeline_config),
        )


def valid_airport_row() -> tuple[str | None, ...]:
    return (
        "1",
        "Airport One",
        "City",
        "Country",
        "AAA",
        "AAAA",
        "45.5",
        "-73.5",
        "100",
        "-5",
        "A",
        "Region/City",
        "airport",
        "OurAirports",
    )


def test_classify_airports_returns_typed_join_ready_row(
    spark: SparkSession,
) -> None:
    source = spark.createDataFrame(
        [valid_airport_row()],
        openflights_raw_schema(),
    )

    result = classify_airports(source)
    accepted = result.accepted.first()

    assert result.accepted.count() == 1
    assert result.rejected.count() == 0
    assert result.accepted.schema["AirportID"].dataType.simpleString() == "int"
    assert result.accepted.schema["Latitude"].dataType.simpleString() == "double"
    assert accepted["Latitude"] == 45.5


@pytest.mark.parametrize(
    ("column_index", "value", "reason"),
    [
        (4, None, "missing_iata"),
        (4, "A1", "invalid_iata"),
        (6, "north", "invalid_latitude"),
        (6, "91", "latitude_out_of_range"),
        (7, "west", "invalid_longitude"),
        (7, "-181", "longitude_out_of_range"),
    ],
)
def test_classify_airports_records_specific_rejection_reason(
    spark: SparkSession,
    column_index: int,
    value: str | None,
    reason: str,
) -> None:
    row = list(valid_airport_row())
    row[column_index] = value
    source = spark.createDataFrame([tuple(row)], openflights_raw_schema())

    result = classify_airports(source)

    assert result.accepted.count() == 0
    assert reason in result.rejected.first()["rejection_reasons"]


def test_classify_airports_allows_missing_optional_numeric_values(
    spark: SparkSession,
) -> None:
    row = list(valid_airport_row())
    row[8] = None
    row[9] = None
    source = spark.createDataFrame([tuple(row)], openflights_raw_schema())

    result = classify_airports(source)
    accepted = result.accepted.first()

    assert result.rejected.count() == 0
    assert accepted["Altitude"] is None
    assert accepted["Timezone"] is None


def test_validate_unique_iata_accepts_one_row_per_code(spark: SparkSession) -> None:
    airports = spark.createDataFrame([("AAA",), ("BBB",)], ["IATA"])

    validate_unique_iata(airports)


def test_validate_unique_iata_rejects_duplicate_code(spark: SparkSession) -> None:
    airports = spark.createDataFrame([("AAA",), ("AAA",)], ["IATA"])

    with pytest.raises(AirportReferenceError, match=r"AAA \(2\)"):
        validate_unique_iata(airports)


def two_classified_airports(spark: SparkSession):
    invalid = list(valid_airport_row())
    invalid[0] = "2"
    invalid[4] = None
    source = spark.createDataFrame(
        [valid_airport_row(), tuple(invalid)],
        openflights_raw_schema(),
    ).cache()
    classified = classify_airports(source)
    classified.accepted.cache()
    classified.rejected.cache()
    return source, classified


def two_row_manifest(pipeline_config) -> ReferenceManifest:
    return stored_manifest(pipeline_config)


def test_reference_silver_paths_are_versioned_by_source_checksum(
    pipeline_config,
) -> None:
    manifest = stored_manifest(pipeline_config)

    assert airport_silver_uri(pipeline_config, manifest).endswith(
        "/silver/reference/openflights/airports/source_sha256=abc123"
    )
    assert airport_rejected_uri(pipeline_config, manifest).endswith(
        "/silver/reference/openflights/rejected_airports/source_sha256=abc123"
    )
    assert airport_quality_uri(pipeline_config, manifest).endswith(
        "/silver/reference/openflights/quality_metrics/"
        "source_sha256=abc123/quality_metrics.json"
    )


def test_collect_airport_quality_metrics_reconciles_rows(
    pipeline_config,
    spark: SparkSession,
) -> None:
    source, classified = two_classified_airports(spark)
    timestamp = datetime(2026, 10, 11, 17, 0, tzinfo=UTC)

    metrics = collect_airport_quality_metrics(
        source,
        classified,
        two_row_manifest(pipeline_config),
        timestamp,
    )

    assert metrics.input_rows == 2
    assert metrics.accepted_rows == 1
    assert metrics.rejected_rows == 1
    assert metrics.rejection_reason_counts == {"missing_iata": 1}
    assert metrics.processed_at_utc == "2026-10-11T17:00:00Z"
    assert airport_quality_from_json(airport_quality_to_json(metrics)) == metrics


def test_write_airport_silver_outputs_writes_parquet_and_metrics(
    pipeline_config,
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = replace(
        pipeline_config,
        hdfs=replace(pipeline_config.hdfs, base_uri=tmp_path.as_uri()),
    )
    manifest = two_row_manifest(config)
    source, classified = two_classified_airports(spark)
    metrics = collect_airport_quality_metrics(
        source,
        classified,
        manifest,
        datetime(2026, 10, 11, 17, 0, tzinfo=UTC),
    )
    hdfs = ReadOnlyMemoryHdfs({})

    result = write_airport_silver_outputs(
        classified,
        metrics,
        config,
        manifest,
        hdfs,
    )

    accepted = spark.read.parquet(airport_silver_uri(config, manifest))
    rejected = spark.read.parquet(airport_rejected_uri(config, manifest))
    assert result.status == "written"
    assert accepted.count() == 1
    assert accepted.schema["Latitude"].dataType.simpleString() == "double"
    assert rejected.count() == 1
    assert "rejection_reasons" in rejected.columns
    assert airport_quality_uri(config, manifest) in hdfs.files


def test_completed_airport_silver_run_returns_matching_metrics(
    pipeline_config,
    spark: SparkSession,
) -> None:
    manifest = two_row_manifest(pipeline_config)
    source, classified = two_classified_airports(spark)
    metrics = collect_airport_quality_metrics(
        source,
        classified,
        manifest,
        datetime(2026, 10, 11, 17, 0, tzinfo=UTC),
    )
    hdfs = ReadOnlyMemoryHdfs(
        {
            airport_quality_uri(pipeline_config, manifest): airport_quality_to_json(
                metrics
            )
        }
    )
    hdfs.directories.update(
        {
            airport_silver_uri(pipeline_config, manifest),
            airport_rejected_uri(pipeline_config, manifest),
        }
    )

    assert completed_airport_silver_run(pipeline_config, hdfs, manifest) == metrics


def test_completed_airport_silver_run_rejects_partial_output(
    pipeline_config,
) -> None:
    manifest = two_row_manifest(pipeline_config)
    hdfs = ReadOnlyMemoryHdfs({})
    hdfs.directories.add(airport_silver_uri(pipeline_config, manifest))

    with pytest.raises(AirportReferenceError, match="without completed"):
        completed_airport_silver_run(pipeline_config, hdfs, manifest)


def test_validate_airport_silver_readback_accepts_typed_outputs(
    pipeline_config,
    spark: SparkSession,
) -> None:
    source, classified = two_classified_airports(spark)
    metrics = collect_airport_quality_metrics(
        source,
        classified,
        two_row_manifest(pipeline_config),
        datetime(2026, 10, 11, 17, 0, tzinfo=UTC),
    )

    readback = validate_airport_silver_readback(
        classified.accepted,
        classified.rejected,
        metrics,
    )

    assert readback.accepted_rows == 1
    assert readback.rejected_rows == 1
    assert readback.distinct_iata_codes == 1
    assert readback.accepted_columns == 14


def test_validate_airport_silver_readback_rejects_duplicate_iata(
    spark: SparkSession,
) -> None:
    second_row = list(valid_airport_row())
    second_row[0] = "2"
    second_row[1] = "Airport Two"
    accepted = classify_airports(
        spark.createDataFrame(
            [valid_airport_row(), tuple(second_row)],
            openflights_raw_schema(),
        )
    ).accepted
    rejected = spark.createDataFrame(
        [],
        "rejection_reasons array<string>",
    )
    metrics = AirportQualityMetrics(
        source_name="test",
        source_file_name="airports.dat",
        source_sha256="abc123",
        input_rows=2,
        accepted_rows=2,
        rejected_rows=0,
        rejection_reason_counts={},
        processed_at_utc="2026-10-11T17:00:00Z",
    )

    with pytest.raises(AirportReferenceError, match="duplicate IATA"):
        validate_airport_silver_readback(accepted, rejected, metrics)
