from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.gold import (
    CarrierGoldMetrics,
    GoldPipelineError,
    build_carrier_summary,
    carrier_gold_metrics_from_json,
    carrier_gold_metrics_to_json,
    carrier_quality_metrics_uri,
    carrier_summary_uri,
    collect_carrier_gold_metrics,
    completed_carrier_gold_run,
    validate_carrier_gold_readback,
    write_carrier_gold_outputs,
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


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("gold-tests").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_build_carrier_summary_uses_documented_denominators(
    spark: SparkSession,
) -> None:
    flights = spark.createDataFrame(
        [
            ("AA", False, 15.0),
            ("AA", False, 5.0),
            ("AA", False, None),
            ("AA", True, 100.0),
            ("BB", True, None),
        ],
        "Reporting_Airline string, Cancelled boolean, ArrDelay double",
    )

    rows = {
        row["reporting_airline"]: row
        for row in build_carrier_summary(flights, 2025, 1, 15).collect()
    }

    aa = rows["AA"]
    assert aa["report_year"] == 2025
    assert aa["report_month"] == 1
    assert aa["scheduled_flights"] == 4
    assert aa["operated_flights"] == 3
    assert aa["arrival_result_flights"] == 2
    assert aa["delayed_flights"] == 1
    assert aa["cancelled_flights"] == 1
    assert aa["average_arrival_delay_minutes"] == pytest.approx(10.0)
    assert aa["arrival_delay_rate"] == pytest.approx(0.5)
    assert aa["cancellation_rate"] == pytest.approx(0.25)

    bb = rows["BB"]
    assert bb["scheduled_flights"] == 1
    assert bb["operated_flights"] == 0
    assert bb["arrival_result_flights"] == 0
    assert bb["delayed_flights"] == 0
    assert bb["cancelled_flights"] == 1
    assert bb["average_arrival_delay_minutes"] is None
    assert bb["arrival_delay_rate"] is None
    assert bb["cancellation_rate"] == pytest.approx(1.0)


def test_build_carrier_summary_requires_gold_columns(spark: SparkSession) -> None:
    flights = spark.createDataFrame([("AA",)], ["Reporting_Airline"])

    with pytest.raises(GoldPipelineError, match="ArrDelay, Cancelled"):
        build_carrier_summary(flights, 2025, 1, 15)


def test_carrier_gold_paths_include_run_and_both_source_checksums() -> None:
    config = load_pipeline_config(Path("config/pipeline.yml"))

    summary = carrier_summary_uri(config, "flight-sha", "airport-sha")
    quality = carrier_quality_metrics_uri(config, "flight-sha", "airport-sha")

    lineage = (
        "/year=2025/month=01/flight_source_sha256=flight-sha/"
        "airport_source_sha256=airport-sha"
    )
    assert summary.endswith(f"/gold/carrier_summary{lineage}")
    assert quality.endswith(
        f"/gold/quality_metrics/carrier_summary{lineage}/quality_metrics.json"
    )


def quality_test_flights(spark: SparkSession):
    return spark.createDataFrame(
        [
            ("AA", False, 15.0),
            ("AA", False, 5.0),
            ("AA", False, None),
            ("AA", True, None),
            ("BB", True, None),
        ],
        "Reporting_Airline string, Cancelled boolean, ArrDelay double",
    )


def test_collect_carrier_gold_metrics_reconciles_counts(
    spark: SparkSession,
) -> None:
    flights = quality_test_flights(spark).cache()
    summary = build_carrier_summary(flights, 2025, 1, 15)

    metrics = collect_carrier_gold_metrics(
        flights,
        summary,
        expected_input_rows=5,
        flight_sha256="flight-sha",
        airport_sha256="airport-sha",
        delay_threshold_minutes=15,
        processed_at=datetime(2026, 10, 13, 12, 0, tzinfo=UTC),
    )

    assert metrics.input_flight_rows == 5
    assert metrics.output_carrier_rows == 2
    assert metrics.scheduled_flights == 5
    assert metrics.operated_flights == 3
    assert metrics.arrival_result_flights == 2
    assert metrics.delayed_flights == 1
    assert metrics.cancelled_flights == 2
    assert metrics.arrival_delay_rate_denominator == "arrival_result_flights"
    assert metrics.cancellation_rate_denominator == "scheduled_flights"
    assert metrics.processed_at_utc == "2026-10-13T12:00:00Z"


def test_collect_carrier_gold_metrics_rejects_rate_outside_range(
    spark: SparkSession,
) -> None:
    flights = quality_test_flights(spark)
    summary = build_carrier_summary(flights, 2025, 1, 15).withColumn(
        "arrival_delay_rate",
        F.when(F.col("reporting_airline") == "AA", 1.5).otherwise(
            F.col("arrival_delay_rate")
        ),
    )

    with pytest.raises(GoldPipelineError, match="invalid counts or rates"):
        collect_carrier_gold_metrics(
            flights,
            summary,
            expected_input_rows=5,
            flight_sha256="flight-sha",
            airport_sha256="airport-sha",
            delay_threshold_minutes=15,
        )


def test_collect_carrier_gold_metrics_rejects_input_row_mismatch(
    spark: SparkSession,
) -> None:
    flights = quality_test_flights(spark)
    summary = build_carrier_summary(flights, 2025, 1, 15)

    with pytest.raises(GoldPipelineError, match="row count does not match"):
        collect_carrier_gold_metrics(
            flights,
            summary,
            expected_input_rows=6,
            flight_sha256="flight-sha",
            airport_sha256="airport-sha",
            delay_threshold_minutes=15,
        )


def test_carrier_gold_metrics_json_round_trip() -> None:
    metrics = CarrierGoldMetrics(
        flight_source_sha256="flight-sha",
        airport_source_sha256="airport-sha",
        input_flight_rows=5,
        output_carrier_rows=2,
        scheduled_flights=5,
        operated_flights=3,
        arrival_result_flights=2,
        delayed_flights=1,
        cancelled_flights=2,
        delay_threshold_minutes=15,
        arrival_delay_rate_denominator="arrival_result_flights",
        cancellation_rate_denominator="scheduled_flights",
        processed_at_utc="2026-10-13T12:00:00Z",
    )

    stored = carrier_gold_metrics_to_json(metrics)

    assert stored.endswith("\n")
    assert carrier_gold_metrics_from_json(stored) == metrics


def test_carrier_gold_metrics_json_rejects_invalid_content() -> None:
    with pytest.raises(GoldPipelineError, match="Invalid carrier Gold"):
        carrier_gold_metrics_from_json("not-json")


def test_write_reuse_and_readback_carrier_gold(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = load_pipeline_config(Path("config/pipeline.yml"))
    config = replace(
        config,
        hdfs=replace(config.hdfs, base_uri=tmp_path.as_uri()),
    )
    flights = quality_test_flights(spark).cache()
    summary = build_carrier_summary(flights, 2025, 1, 15).cache()
    metrics = collect_carrier_gold_metrics(
        flights,
        summary,
        expected_input_rows=5,
        flight_sha256="flight-sha",
        airport_sha256="airport-sha",
        delay_threshold_minutes=15,
        processed_at=datetime(2026, 10, 13, 12, 0, tzinfo=UTC),
    )
    hdfs = MemoryHdfsClient()

    result = write_carrier_gold_outputs(summary, metrics, config, hdfs)
    output_path = carrier_summary_uri(config, "flight-sha", "airport-sha")
    stored = spark.read.parquet(output_path)
    readback = validate_carrier_gold_readback(stored, metrics)

    assert result.status == "written"
    assert readback.output_carrier_rows == 2
    assert readback.scheduled_flights == 5
    assert readback.cancelled_flights == 2

    hdfs.directories.add(output_path)
    assert (
        completed_carrier_gold_run(
            config,
            hdfs,
            "flight-sha",
            "airport-sha",
        )
        == metrics
    )


def test_completed_carrier_gold_rejects_partial_output() -> None:
    config = load_pipeline_config(Path("config/pipeline.yml"))
    hdfs = MemoryHdfsClient()
    hdfs.directories.add(carrier_summary_uri(config, "flight-sha", "airport-sha"))

    with pytest.raises(GoldPipelineError, match="incomplete"):
        completed_carrier_gold_run(
            config,
            hdfs,
            "flight-sha",
            "airport-sha",
        )
