"""Command-line entry point for OpenFlights Bronze ingestion."""

import argparse
from pathlib import Path

from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.hdfs import HdfsClient
from flight_delay_analysis.reference import (
    ingest_reference,
    reference_manifest_to_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download and preserve OpenFlights airport data in HDFS Bronze."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/pipeline.yml"),
        help="Path to the shared pipeline YAML file.",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path.cwd(),
        help="Project root used to resolve the configured local reference path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_pipeline_config(args.config)
    result = ingest_reference(config, args.project_root.resolve(), HdfsClient())

    print(f"Local reference status: {result.local_status}")
    print(f"HDFS Bronze status: {result.hdfs_status}")
    print(reference_manifest_to_json(result.manifest), end="")


if __name__ == "__main__":
    main()
