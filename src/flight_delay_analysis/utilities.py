"""Shared Spark and command-line helpers for the pipeline."""

import argparse
from dataclasses import replace

from pyspark.sql import SparkSession

from flight_delay_analysis.config import PipelineConfig, RunConfig


def create_spark_session() -> SparkSession:
    """Create the local Spark session used by pipeline commands."""
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("flight-delay-analysis")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def add_run_arguments(parser: argparse.ArgumentParser) -> None:
    """Add optional year, month, and row-count command-line options."""
    parser.add_argument("--year", type=int, help="Source and output year.")
    parser.add_argument("--month", type=int, help="Source and output month.")
    parser.add_argument(
        "--expected-rows",
        type=int,
        help="Optional known source row count for this monthly file.",
    )


def apply_run_arguments(
    config: PipelineConfig,
    args: argparse.Namespace,
) -> PipelineConfig:
    """Return a validated configuration for the requested month."""
    year = args.year if args.year is not None else config.run.year
    month = args.month if args.month is not None else config.run.month
    if year <= 0:
        raise ValueError("year must be positive")
    if not 1 <= month <= 12:
        raise ValueError("month must be between 1 and 12")
    if args.expected_rows is not None and args.expected_rows <= 0:
        raise ValueError("expected rows must be positive")

    period_changed = (year, month) != (config.run.year, config.run.month)
    expected_rows = args.expected_rows
    if expected_rows is None and not period_changed:
        expected_rows = config.quality.expected_rows
    return replace(
        config,
        run=RunConfig(year=year, month=month),
        quality=replace(config.quality, expected_rows=expected_rows),
    )
