"""Build typed HDFS Silver outputs for OpenFlights airport reference data."""

import argparse
from pathlib import Path

from flight_delay_analysis.airport_reference import (
    AirportSilverResult,
    airport_quality_to_json,
    classify_airports,
    collect_airport_quality_metrics,
    completed_airport_silver_run,
    load_reference_manifest,
    read_reference_bronze,
    validate_reference_row_count,
    validate_unique_iata,
    write_airport_silver_outputs,
)
from flight_delay_analysis.build_parquet import create_spark_session
from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.hdfs import HdfsClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build typed OpenFlights airport reference data in HDFS Silver."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/pipeline.yml"),
        help="Path to the shared pipeline YAML file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_pipeline_config(args.config)
    hdfs = HdfsClient()
    manifest = load_reference_manifest(config, hdfs)
    existing_metrics = completed_airport_silver_run(config, hdfs, manifest)

    if existing_metrics is not None:
        result = AirportSilverResult(
            status="already_written",
            metrics=existing_metrics,
        )
    else:
        spark = create_spark_session()
        try:
            source = read_reference_bronze(
                spark,
                manifest.hdfs_file_uri,
                config,
            ).cache()
            validate_reference_row_count(source, manifest)
            classified = classify_airports(source)
            classified.accepted.cache()
            classified.rejected.cache()
            validate_unique_iata(classified.accepted)
            metrics = collect_airport_quality_metrics(
                source,
                classified,
                manifest,
            )
            result = write_airport_silver_outputs(
                classified,
                metrics,
                config,
                manifest,
                hdfs,
            )
        finally:
            spark.stop()

    print(f"Airport Silver status: {result.status}")
    print(airport_quality_to_json(result.metrics), end="")


if __name__ == "__main__":
    main()
