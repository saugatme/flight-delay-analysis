"""Checksum-linked HDFS paths for Gold datasets."""

from flight_delay_analysis.config import PipelineConfig

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
