"""Build all non-carrier Gold tables from enriched Silver."""

import argparse
from pathlib import Path

from flight_delay_analysis.airport_reference import load_reference_manifest
from flight_delay_analysis.build_parquet import create_spark_session
from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.enrichment import (
    completed_enrichment_run,
    enriched_flights_uri,
)
from flight_delay_analysis.gold import (
    AnalyticsGoldResult,
    GoldPipelineError,
    analytics_table_uri,
    build_analytics_tables,
    collect_analytics_gold_metrics,
    completed_analytics_gold_run,
    validate_analytics_gold_readback,
    write_analytics_gold_outputs,
)
from flight_delay_analysis.gold.models import analytics_gold_metrics_to_json
from flight_delay_analysis.hdfs import HdfsClient
from flight_delay_analysis.run_options import add_run_arguments, apply_run_arguments
from flight_delay_analysis.silver import load_bronze_manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build monthly analytics Gold tables."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/pipeline.yml"),
        help="Path to the shared pipeline YAML file.",
    )
    add_run_arguments(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = apply_run_arguments(load_pipeline_config(args.config), args)
    hdfs = HdfsClient()
    flight_manifest = load_bronze_manifest(config, hdfs)
    airport_manifest = load_reference_manifest(config, hdfs)
    enrichment = completed_enrichment_run(
        config,
        hdfs,
        flight_manifest.sha256,
        airport_manifest.sha256,
    )
    if enrichment is None:
        raise GoldPipelineError("Completed enriched Silver data was not found")

    existing = completed_analytics_gold_run(
        config,
        hdfs,
        flight_manifest.sha256,
        airport_manifest.sha256,
    )
    if existing is not None:
        result = AnalyticsGoldResult(status="already_written", metrics=existing)
    else:
        spark = create_spark_session()
        try:
            source_path = enriched_flights_uri(
                config,
                flight_manifest.sha256,
                airport_manifest.sha256,
            )
            flights = spark.read.parquet(source_path).cache()
            tables = {
                name: table.cache()
                for name, table in build_analytics_tables(
                    flights,
                    config.run.year,
                    config.run.month,
                    config.quality.delay_threshold_minutes,
                ).items()
            }
            metrics = collect_analytics_gold_metrics(
                flights,
                tables,
                enrichment.output_flight_rows,
                flight_manifest.sha256,
                airport_manifest.sha256,
                config.quality.delay_threshold_minutes,
            )
            result = write_analytics_gold_outputs(tables, metrics, config, hdfs)
            stored = {
                name: spark.read.parquet(
                    analytics_table_uri(
                        config,
                        name,
                        flight_manifest.sha256,
                        airport_manifest.sha256,
                    )
                )
                for name in tables
            }
            validate_analytics_gold_readback(stored, metrics)
        finally:
            spark.stop()

    print(f"Analytics Gold status: {result.status}")
    print(analytics_gold_metrics_to_json(result.metrics), end="")


if __name__ == "__main__":
    main()
