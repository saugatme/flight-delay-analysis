"""Command-line entry point for configuration-driven Bronze ingestion."""

import argparse
from pathlib import Path

from flight_delay_analysis.bronze import ingest_bronze, manifest_to_json
from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.hdfs import HdfsClient
from flight_delay_analysis.run_options import add_run_arguments, apply_run_arguments


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy one unchanged monthly BTS CSV into HDFS Bronze."
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
        help="Project root used to resolve the configured local source root.",
    )
    add_run_arguments(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = apply_run_arguments(load_pipeline_config(args.config), args)
    result = ingest_bronze(config, args.project_root.resolve(), HdfsClient())

    print(f"Bronze ingestion status: {result.status}")
    print(manifest_to_json(result.manifest), end="")


if __name__ == "__main__":
    main()
