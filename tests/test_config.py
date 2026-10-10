from pathlib import Path

import pytest
import yaml

from flight_delay_analysis.config import ConfigurationError, load_pipeline_config


def valid_config() -> dict:
    return {
        "project": {"name": "flight-delay-analysis"},
        "run": {"year": 2025, "month": 1},
        "source": {
            "format": "csv",
            "local_root": "data/raw",
            "file_pattern": "*.csv",
        },
        "hdfs": {
            "base_uri": "hdfs://localhost:9000/flight-delay",
            "bronze_dataset": "bronze/bts",
            "silver_dataset": "silver/flights",
            "gold_dataset": "gold",
        },
        "quality": {
            "expected_rows": 539747,
            "expected_source_columns": 110,
            "delay_threshold_minutes": 15,
        },
        "mongodb": {
            "host": "localhost",
            "port": 27017,
            "database": "flight_delay",
        },
    }


def write_config(tmp_path: Path, config: dict) -> Path:
    config_path = tmp_path / "pipeline.yml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return config_path


def test_load_pipeline_config_returns_typed_settings(tmp_path: Path) -> None:
    config = load_pipeline_config(write_config(tmp_path, valid_config()))

    assert config.run.year == 2025
    assert config.run.month == 1
    assert config.source.local_root == Path("data/raw")
    assert config.hdfs.bronze_dataset == "bronze/bts"
    assert config.quality.expected_rows == 539747
    assert config.mongodb.port == 27017


def test_load_pipeline_config_rejects_missing_section(tmp_path: Path) -> None:
    raw = valid_config()
    raw.pop("hdfs")

    with pytest.raises(ConfigurationError, match="hdfs.*YAML mapping"):
        load_pipeline_config(write_config(tmp_path, raw))


def test_load_pipeline_config_rejects_invalid_month(tmp_path: Path) -> None:
    raw = valid_config()
    raw["run"]["month"] = 13

    with pytest.raises(ConfigurationError, match="between 1 and 12"):
        load_pipeline_config(write_config(tmp_path, raw))


def test_load_pipeline_config_rejects_absolute_dataset_path(tmp_path: Path) -> None:
    raw = valid_config()
    raw["hdfs"]["bronze_dataset"] = "/bronze/bts"

    with pytest.raises(ConfigurationError, match="relative path"):
        load_pipeline_config(write_config(tmp_path, raw))


def test_load_pipeline_config_rejects_parent_source_path(tmp_path: Path) -> None:
    raw = valid_config()
    raw["source"]["local_root"] = "../data/raw"

    with pytest.raises(ConfigurationError, match="relative path"):
        load_pipeline_config(write_config(tmp_path, raw))


def test_repository_pipeline_config_is_valid() -> None:
    config = load_pipeline_config(Path("config/pipeline.yml"))

    assert config.project_name == "flight-delay-analysis"
