"""Load checksum-linked Gold tables into authenticated MongoDB collections."""

import argparse
import json
from pathlib import Path

from pymongo import MongoClient

from flight_delay_analysis.airport_reference import load_reference_manifest
from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.gold import (
    GoldPipelineError,
    analytics_table_uri,
    carrier_summary_uri,
    completed_analytics_gold_run,
    completed_carrier_gold_run,
)
from flight_delay_analysis.hdfs import HdfsClient
from flight_delay_analysis.serving import (
    create_query_indexes,
    load_dataframe,
    mongodb_uri_from_environment,
    verify_collection_counts,
)
from flight_delay_analysis.silver import load_bronze_manifest
from flight_delay_analysis.utilities import (
    add_run_arguments,
    apply_run_arguments,
    create_spark_session,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load all Gold tables into MongoDB.")
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
    flight_sha = flight_manifest.sha256
    airport_sha = airport_manifest.sha256
    carrier_metrics = completed_carrier_gold_run(
        config, hdfs, flight_sha, airport_sha
    )
    analytics_metrics = completed_analytics_gold_run(
        config, hdfs, flight_sha, airport_sha
    )
    if carrier_metrics is None or analytics_metrics is None:
        raise GoldPipelineError("Completed Gold tables were not found")

    lineage = {
        "flight_source_sha256": flight_sha,
        "airport_source_sha256": airport_sha,
    }
    spark = create_spark_session()
    client = MongoClient(mongodb_uri_from_environment())
    try:
        client.admin.command("ping")
        database = client[config.mongodb.database]
        tables = {
            "carrier_summary": spark.read.parquet(
                carrier_summary_uri(config, flight_sha, airport_sha)
            ),
            **{
                name: spark.read.parquet(
                    analytics_table_uri(config, name, flight_sha, airport_sha)
                )
                for name in analytics_metrics.table_rows
            },
        }
        loaded_rows = {
            name: load_dataframe(database, name, table, lineage)
            for name, table in tables.items()
        }
        create_query_indexes(database)
        expected_rows = {
            "carrier_summary": carrier_metrics.output_carrier_rows,
            **analytics_metrics.table_rows,
        }
        verify_collection_counts(database, expected_rows, lineage)
    finally:
        client.close()
        spark.stop()

    print("MongoDB Gold load status: loaded")
    print(json.dumps(loaded_rows, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
