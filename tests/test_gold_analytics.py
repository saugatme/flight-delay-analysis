from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.gold import (
    GoldPipelineError,
    analytics_quality_metrics_uri,
    analytics_table_uri,
    build_analytics_tables,
    collect_analytics_gold_metrics,
    completed_analytics_gold_run,
)
from flight_delay_analysis.gold.models import (
    analytics_gold_metrics_from_json,
    analytics_gold_metrics_to_json,
)


class MemoryHdfsClient:
    def __init__(self) -> None:
        self.files: dict[str, str] = {}
        self.directories: set[str] = set()

    def exists(self, uri: str) -> bool:
        return uri in self.files or uri in self.directories

    def read_text(self, uri: str) -> str:
        return self.files[uri]


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("gold-analytics-tests")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def enriched_flights(spark: SparkSession):
    schema = (
        "Reporting_Airline string, Origin string, Dest string, "
        "OriginAirportName string, DestAirportName string, "
        "OriginLatitude double, OriginLongitude double, "
        "DestLatitude double, DestLongitude double, "
        "Cancelled boolean, CancellationCode string, "
        "DepDelay double, ArrDelay double, Distance double"
    )
    return spark.createDataFrame(
        [
            (
                "AA",
                "AAA",
                "BBB",
                "Airport A",
                "Airport B",
                10.0,
                20.0,
                30.0,
                40.0,
                False,
                None,
                5.0,
                15.0,
                100.0,
            ),
            (
                "AA",
                "AAA",
                "CCC",
                "Airport A",
                "Airport C",
                10.0,
                20.0,
                50.0,
                60.0,
                False,
                None,
                20.0,
                5.0,
                200.0,
            ),
            (
                "BB",
                "BBB",
                "AAA",
                "Airport B",
                "Airport A",
                30.0,
                40.0,
                10.0,
                20.0,
                True,
                "A",
                None,
                None,
                100.0,
            ),
            (
                "BB",
                "CCC",
                "AAA",
                "Airport C",
                "Airport A",
                50.0,
                60.0,
                10.0,
                20.0,
                False,
                None,
                10.0,
                20.0,
                200.0,
            ),
        ],
        schema,
    )


def test_build_analytics_tables_uses_consistent_denominators(
    spark: SparkSession,
) -> None:
    flights = enriched_flights(spark).cache()
    tables = build_analytics_tables(flights, 2025, 1, 15)

    month = tables["month_summary"].first()
    assert month["season"] == "winter"
    assert month["scheduled_flights"] == 4
    assert month["operated_flights"] == 3
    assert month["arrival_result_flights"] == 3
    assert month["delayed_flights"] == 2
    assert month["cancelled_flights"] == 1
    assert month["arrival_delay_rate"] == pytest.approx(2 / 3)
    assert month["cancellation_rate"] == pytest.approx(0.25)

    assert tables["season_summary"].first()["scheduled_flights"] == 4
    assert tables["airport_summary"].count() == 3
    assert tables["route_summary"].count() == 4
    assert tables["airport_connectivity"].count() == 3
    assert tables["airport_score"].count() == 3
    assert tables["route_score"].count() == 4

    cancellation = tables["cancellation_summary"].first()
    assert cancellation["cancellation_code"] == "A"
    assert cancellation["cancelled_flights"] == 1
    assert cancellation["cancellation_share"] == pytest.approx(1.0)


def test_collect_analytics_metrics_reconciles_all_tables(
    spark: SparkSession,
) -> None:
    flights = enriched_flights(spark).cache()
    tables = {
        name: table.cache()
        for name, table in build_analytics_tables(flights, 2025, 1, 15).items()
    }

    metrics = collect_analytics_gold_metrics(
        flights,
        tables,
        expected_input_rows=4,
        flight_sha256="flight-sha",
        airport_sha256="airport-sha",
        delay_threshold_minutes=15,
        processed_at=datetime(2026, 10, 13, 15, 0, tzinfo=UTC),
    )

    assert metrics.input_flight_rows == 4
    assert metrics.cancelled_flights == 1
    assert metrics.table_rows["airport_summary"] == 3
    assert metrics.table_rows["route_summary"] == 4
    assert metrics.processed_at_utc == "2026-10-13T15:00:00Z"
    assert analytics_gold_metrics_from_json(
        analytics_gold_metrics_to_json(metrics)
    ) == metrics


def test_analytics_paths_and_partial_output_detection() -> None:
    config = load_pipeline_config(Path("config/pipeline.yml"))
    route_path = analytics_table_uri(
        config, "route_summary", "flight-sha", "airport-sha"
    )
    quality_path = analytics_quality_metrics_uri(
        config, "flight-sha", "airport-sha"
    )

    assert route_path.endswith(
        "/gold/route_summary/year=2025/month=01/"
        "flight_source_sha256=flight-sha/airport_source_sha256=airport-sha"
    )
    assert quality_path.endswith("/quality_metrics.json")

    hdfs = MemoryHdfsClient()
    hdfs.directories.add(route_path)
    with pytest.raises(GoldPipelineError, match="incomplete"):
        completed_analytics_gold_run(
            config, hdfs, "flight-sha", "airport-sha"
        )
