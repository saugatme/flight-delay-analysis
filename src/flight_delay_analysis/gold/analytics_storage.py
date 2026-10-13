"""Safe storage for the analytics Gold bundle."""

from pyspark.sql import DataFrame

from flight_delay_analysis.config import PipelineConfig
from flight_delay_analysis.gold.errors import GoldPipelineError
from flight_delay_analysis.gold.models import (
    AnalyticsGoldMetrics,
    AnalyticsGoldResult,
    analytics_gold_metrics_from_json,
    analytics_gold_metrics_to_json,
)
from flight_delay_analysis.gold.paths import (
    ANALYTICS_DATASETS,
    analytics_quality_metrics_uri,
    analytics_table_uri,
)
from flight_delay_analysis.hdfs import HdfsClient


def completed_analytics_gold_run(
    config: PipelineConfig,
    hdfs: HdfsClient,
    flight_sha256: str,
    airport_sha256: str,
) -> AnalyticsGoldMetrics | None:
    """Return metrics for a complete bundle or reject partial output."""
    paths = {
        name: analytics_table_uri(
            config, name, flight_sha256, airport_sha256
        )
        for name in ANALYTICS_DATASETS
    }
    metrics_path = analytics_quality_metrics_uri(
        config, flight_sha256, airport_sha256
    )
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
