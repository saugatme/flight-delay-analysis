"""Build the checksum-linked carrier Gold result from enriched Silver."""

import argparse
from pathlib import Path

from flight_delay_analysis.airport_reference import (
    load_reference_manifest,
)
from flight_delay_analysis.build_parquet import create_spark_session
from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.enrichment import (
    completed_enrichment_run,
    enriched_flights_uri,
)
from flight_delay_analysis.gold import (
    CarrierGoldResult,
    GoldPipelineError,
    build_carrier_summary,
    carrier_gold_metrics_to_json,
    carrier_summary_uri,
    collect_carrier_gold_metrics,
    completed_carrier_gold_run,
    validate_carrier_gold_readback,
    write_carrier_gold_outputs,
)
from flight_delay_analysis.hdfs import HdfsClient
from flight_delay_analysis.run_options import add_run_arguments, apply_run_arguments
from flight_delay_analysis.silver import load_bronze_manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build carrier delay and cancellation Gold output."
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

    existing = completed_carrier_gold_run(
        config,
        hdfs,
        flight_manifest.sha256,
        airport_manifest.sha256,
    )
    if existing is not None:
        result = CarrierGoldResult(status="already_written", metrics=existing)
    else:
        spark = create_spark_session()
        try:
            source_path = enriched_flights_uri(
                config,
                flight_manifest.sha256,
                airport_manifest.sha256,
            )
            flights = spark.read.parquet(source_path).cache()
            summary = build_carrier_summary(
                flights,
                config.run.year,
                config.run.month,
                config.quality.delay_threshold_minutes,
            ).cache()
            metrics = collect_carrier_gold_metrics(
                flights,
                summary,
                enrichment.output_flight_rows,
                flight_manifest.sha256,
                airport_manifest.sha256,
                config.quality.delay_threshold_minutes,
            )
            result = write_carrier_gold_outputs(summary, metrics, config, hdfs)
            stored = spark.read.parquet(
                carrier_summary_uri(
                    config,
                    flight_manifest.sha256,
                    airport_manifest.sha256,
                )
            )
            validate_carrier_gold_readback(stored, metrics)
        finally:
            spark.stop()

    print(f"Carrier Gold status: {result.status}")
    print(carrier_gold_metrics_to_json(result.metrics), end="")


if __name__ == "__main__":
    main()
