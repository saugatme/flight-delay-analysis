"""Read back and verify persisted OpenFlights Silver Parquet."""

import argparse
from pathlib import Path

from flight_delay_analysis.airport_reference import (
    AirportReferenceError,
    airport_rejected_uri,
    airport_silver_uri,
    completed_airport_silver_run,
    load_reference_manifest,
    validate_airport_silver_readback,
)
from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.hdfs import HdfsClient
from flight_delay_analysis.utilities import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read back and verify OpenFlights HDFS Silver outputs."
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
    metrics = completed_airport_silver_run(config, hdfs, manifest)
    if metrics is None:
        raise AirportReferenceError(
            "Completed airport Silver outputs were not found for this source"
        )

    accepted_path = airport_silver_uri(config, manifest)
    rejected_path = airport_rejected_uri(config, manifest)
    spark = create_spark_session()
    try:
        accepted = spark.read.parquet(accepted_path).cache()
        rejected = spark.read.parquet(rejected_path).cache()
        readback = validate_airport_silver_readback(accepted, rejected, metrics)

        print(f"Accepted Silver path: {accepted_path}")
        print(f"Rejected Silver path: {rejected_path}")
        print(f"Accepted rows read back: {readback.accepted_rows:,}")
        print(f"Rejected rows read back: {readback.rejected_rows:,}")
        print(f"Accepted columns: {readback.accepted_columns}")
        print(f"Distinct accepted IATA codes: {readback.distinct_iata_codes:,}")
        print("Readback reconciled: True")
        print(f"AirportID type: {accepted.schema['AirportID'].dataType.simpleString()}")
        print(f"Latitude type: {accepted.schema['Latitude'].dataType.simpleString()}")
        print(f"Longitude type: {accepted.schema['Longitude'].dataType.simpleString()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
