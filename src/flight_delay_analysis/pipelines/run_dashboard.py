"""Run the MongoDB-backed Plotly Dash application."""

import argparse
import os
from pathlib import Path

from pymongo import MongoClient

from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.dashboard import create_dashboard_app
from flight_delay_analysis.serving import mongodb_uri_from_environment


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the flight-delay dashboard.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/pipeline.yml"),
        help="Path to the shared pipeline YAML file.",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8050)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_pipeline_config(args.config)
    client = MongoClient(mongodb_uri_from_environment())
    client.admin.command("ping")
    app = create_dashboard_app(client[config.mongodb.database])
    app.run(
        host=args.host,
        port=args.port,
        debug=os.environ.get("DASH_DEBUG", "").lower() == "true",
    )


if __name__ == "__main__":
    main()
