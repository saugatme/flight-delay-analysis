from argparse import Namespace
from pathlib import Path

import pytest

from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.run_options import apply_run_arguments


def args(year=None, month=None, expected_rows=None):
    return Namespace(year=year, month=month, expected_rows=expected_rows)


def test_changed_period_uses_manifest_row_count_by_default() -> None:
    config = load_pipeline_config(Path("config/pipeline.yml"))

    changed = apply_run_arguments(config, args(year=2024, month=12))

    assert changed.run.year == 2024
    assert changed.run.month == 12
    assert changed.quality.expected_rows is None


def test_period_can_enforce_a_known_row_count() -> None:
    config = load_pipeline_config(Path("config/pipeline.yml"))

    changed = apply_run_arguments(
        config,
        args(year=2024, month=12, expected_rows=500_000),
    )

    assert changed.quality.expected_rows == 500_000


def test_run_override_rejects_invalid_month() -> None:
    config = load_pipeline_config(Path("config/pipeline.yml"))

    with pytest.raises(ValueError, match="between 1 and 12"):
        apply_run_arguments(config, args(month=13))
