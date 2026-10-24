"""Command-line entry point for the HDFS Bronze-to-Silver pipeline."""

import argparse
from pathlib import Path

from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.hdfs import HdfsClient
from flight_delay_analysis.silver import (
    SilverWriteResult,
    classify_flights,
    collect_quality_metrics,
    completed_silver_run,
    load_bronze_manifest,
    quality_metrics_to_json,
    read_bronze_csv,
    validate_source_contract,
    write_silver_outputs,
)
from flight_delay_analysis.utilities import (
    add_run_arguments,
    apply_run_arguments,
    create_spark_session,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build validated HDFS Silver data from one Bronze partition."
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
    manifest = load_bronze_manifest(config, hdfs)

    existing_metrics = completed_silver_run(config, hdfs, manifest)
    if existing_metrics is not None:
        result = SilverWriteResult(
            status="already_written",
            metrics=existing_metrics,
        )
    else:
        spark = create_spark_session()
        try:
            source = read_bronze_csv(spark, manifest.hdfs_file_uri).cache()
            validate_source_contract(source, config)
            classified = classify_flights(source, config)
            classified.accepted.cache()
            classified.rejected.cache()
            metrics = collect_quality_metrics(
                source,
                classified,
                config,
                manifest,
            )
            result = write_silver_outputs(classified, metrics, config, hdfs)
        finally:
            spark.stop()

    print(f"Silver pipeline status: {result.status}")
    print(quality_metrics_to_json(result.metrics), end="")


if __name__ == "__main__":
    main()
