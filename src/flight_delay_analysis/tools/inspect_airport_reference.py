"""Inspect explicit parsing of the OpenFlights HDFS Bronze source."""

import argparse
from pathlib import Path

from flight_delay_analysis.airport_reference import (
    load_reference_manifest,
    read_reference_bronze,
    validate_reference_row_count,
)
from flight_delay_analysis.build_parquet import create_spark_session
from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.hdfs import HdfsClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse and inspect the manifested OpenFlights Bronze source."
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
    manifest = load_reference_manifest(config, HdfsClient())
    spark = create_spark_session()

    try:
        airports = read_reference_bronze(
            spark,
            manifest.hdfs_file_uri,
            config,
        ).cache()
        parsed_rows = validate_reference_row_count(airports, manifest)

        print(f"Reference Bronze source: {manifest.hdfs_file_uri}")
        print(f"Rows read from HDFS: {parsed_rows:,}")
        print(f"Manifest rows: {manifest.row_count:,}")
        print(f"Parsed columns: {len(airports.columns)}")
        print(f"Missing marker converted to null: {config.reference.missing_value}")
        airports.printSchema()
        airports.select(
            "AirportID",
            "Name",
            "Country",
            "IATA",
            "Latitude",
            "Longitude",
        ).show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
