"""Validate join-ready OpenFlights airport reference records."""

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from flight_delay_analysis.airport_reference import (
    classify_airports,
    load_reference_manifest,
    read_reference_bronze,
    validate_reference_row_count,
    validate_unique_iata,
)
from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.hdfs import HdfsClient
from flight_delay_analysis.utilities import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate OpenFlights records for BTS airport-code joins."
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
        source = read_reference_bronze(
            spark,
            manifest.hdfs_file_uri,
            config,
        ).cache()
        input_rows = validate_reference_row_count(source, manifest)
        classified = classify_airports(source)
        classified.accepted.cache()
        classified.rejected.cache()

        accepted_rows = classified.accepted.count()
        rejected_rows = classified.rejected.count()
        if input_rows != accepted_rows + rejected_rows:
            raise RuntimeError(
                "Airport reference counts do not reconcile: "
                f"input={input_rows}, accepted={accepted_rows}, "
                f"rejected={rejected_rows}"
            )
        validate_unique_iata(classified.accepted)

        reason_rows = (
            classified.rejected.select(F.explode("rejection_reasons").alias("reason"))
            .groupBy("reason")
            .count()
            .orderBy("reason")
            .collect()
        )
        reason_counts = {row["reason"]: row["count"] for row in reason_rows}

        print(f"Input rows: {input_rows:,}")
        print(f"Join-ready rows: {accepted_rows:,}")
        print(f"Rejected rows: {rejected_rows:,}")
        print(f"Reconciled: {input_rows == accepted_rows + rejected_rows}")
        print("Accepted IATA codes are unique: True")
        print(f"Rejection reasons: {reason_counts}")
        classified.accepted.printSchema()
        classified.rejected.select(
            "AirportID",
            "Name",
            "IATA",
            "Latitude",
            "Longitude",
            "rejection_reasons",
        ).show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
