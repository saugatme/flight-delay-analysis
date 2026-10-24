"""Read back and verify airport-enriched HDFS Silver flights."""

import argparse
from pathlib import Path

from flight_delay_analysis.airport_reference import load_reference_manifest
from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.enrichment import (
    EnrichmentError,
    completed_enrichment_run,
    enriched_flights_uri,
    validate_enrichment_readback,
)
from flight_delay_analysis.hdfs import HdfsClient
from flight_delay_analysis.silver import load_bronze_manifest
from flight_delay_analysis.utilities import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read back and verify airport-enriched HDFS Silver flights."
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
    flight_manifest = load_bronze_manifest(config, hdfs)
    airport_manifest = load_reference_manifest(config, hdfs)
    metrics = completed_enrichment_run(
        config,
        hdfs,
        flight_manifest.sha256,
        airport_manifest.sha256,
    )
    if metrics is None:
        raise EnrichmentError("Completed enriched Silver flights were not found")

    output_path = enriched_flights_uri(
        config,
        flight_manifest.sha256,
        airport_manifest.sha256,
    )
    spark = create_spark_session()
    try:
        enriched = spark.read.parquet(output_path).cache()
        readback = validate_enrichment_readback(enriched, metrics)

        print(f"Enriched Silver path: {output_path}")
        print(f"Rows read back: {readback.output_rows:,}")
        print(f"Columns read back: {readback.output_columns}")
        print(f"Origin matches read back: {readback.origin_matched_rows:,}")
        print(f"Destination matches read back: {readback.destination_matched_rows:,}")
        print(f"Both endpoints matched: {readback.both_matched_rows:,}")
        print("Readback matches quality metrics: True")
        print(
            "OriginAirportMatched type: "
            f"{enriched.schema['OriginAirportMatched'].dataType.simpleString()}"
        )
        print(
            "DestAirportMatched type: "
            f"{enriched.schema['DestAirportMatched'].dataType.simpleString()}"
        )
        print(
            "OriginLatitude type: "
            f"{enriched.schema['OriginLatitude'].dataType.simpleString()}"
        )
        print(
            "DestLatitude type: "
            f"{enriched.schema['DestLatitude'].dataType.simpleString()}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
