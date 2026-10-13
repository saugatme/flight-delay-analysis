"""Build HDFS Silver flights enriched with both airport endpoints."""

import argparse
from pathlib import Path

from flight_delay_analysis.airport_reference import (
    airport_silver_uri,
    completed_airport_silver_run,
    load_reference_manifest,
)
from flight_delay_analysis.build_parquet import create_spark_session
from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.enrichment import (
    EnrichmentError,
    EnrichmentResult,
    collect_enrichment_metrics,
    completed_enrichment_run,
    enrich_flights,
    enrichment_metrics_to_json,
    write_enrichment_outputs,
)
from flight_delay_analysis.hdfs import HdfsClient
from flight_delay_analysis.run_options import add_run_arguments, apply_run_arguments
from flight_delay_analysis.silver import (
    completed_silver_run,
    load_bronze_manifest,
    silver_partition_uri,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Join validated airport details to both Silver flight endpoints."
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
    flight_metrics = completed_silver_run(config, hdfs, flight_manifest)
    if flight_metrics is None:
        raise EnrichmentError("Completed flight Silver data was not found")

    airport_manifest = load_reference_manifest(config, hdfs)
    airport_metrics = completed_airport_silver_run(config, hdfs, airport_manifest)
    if airport_metrics is None:
        raise EnrichmentError("Completed airport Silver data was not found")

    existing = completed_enrichment_run(
        config,
        hdfs,
        flight_manifest.sha256,
        airport_manifest.sha256,
    )
    if existing is not None:
        result = EnrichmentResult(status="already_written", metrics=existing)
    else:
        spark = create_spark_session()
        try:
            flights = spark.read.parquet(silver_partition_uri(config)).cache()
            airports = spark.read.parquet(
                airport_silver_uri(config, airport_manifest)
            ).cache()
            enriched = enrich_flights(flights, airports).cache()
            metrics = collect_enrichment_metrics(
                flights,
                airports,
                enriched,
                flight_metrics.accepted_rows,
                airport_metrics.accepted_rows,
                flight_manifest.sha256,
                airport_manifest.sha256,
            )
            result = write_enrichment_outputs(enriched, metrics, config, hdfs)
        finally:
            spark.stop()

    print(f"Flight enrichment status: {result.status}")
    print(enrichment_metrics_to_json(result.metrics), end="")


if __name__ == "__main__":
    main()
