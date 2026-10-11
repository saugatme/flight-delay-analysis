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
            "silver_rejected_dataset": "silver/rejected_flights",
            "silver_quality_dataset": "silver/quality_metrics",
            "enriched_silver_dataset": "silver/enriched_flights",
            "enrichment_quality_dataset": "silver/enrichment_quality_metrics",
            "gold_dataset": "gold",
        },
        "quality": {
            "expected_rows": 539747,
            "expected_source_columns": 110,
            "delay_threshold_minutes": 15,
        },
        "reference": {
            "name": "openflights-airports",
            "source_url": (
                "https://raw.githubusercontent.com/jpatokal/openflights/"
                "master/data/airports.dat"
            ),
            "local_root": "data/reference/openflights",
            "file_name": "airports.dat",
            "format": "csv_no_header",
            "expected_columns": 14,
            "missing_value": "\\N",
            "license": "ODbL-1.0",
            "license_url": "https://opendatacommons.org/licenses/odbl/1-0/",
            "hdfs_bronze_dataset": "bronze/reference/openflights/airports",
            "hdfs_silver_dataset": "silver/reference/openflights/airports",
            "hdfs_rejected_dataset": ("silver/reference/openflights/rejected_airports"),
            "hdfs_quality_dataset": ("silver/reference/openflights/quality_metrics"),
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
    assert config.hdfs.silver_rejected_dataset == "silver/rejected_flights"
    assert config.hdfs.silver_quality_dataset == "silver/quality_metrics"
    assert config.hdfs.enriched_silver_dataset == "silver/enriched_flights"
    assert config.quality.expected_rows == 539747
    assert config.reference.expected_columns == 14
    assert config.reference.missing_value == "\\N"
    assert config.reference.local_root == Path("data/reference/openflights")
    assert config.reference.hdfs_rejected_dataset.endswith("rejected_airports")
    assert config.reference.hdfs_quality_dataset.endswith("quality_metrics")
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


def test_load_pipeline_config_rejects_insecure_reference_url(tmp_path: Path) -> None:
    raw = valid_config()
    raw["reference"]["source_url"] = "http://example.com/airports.dat"

    with pytest.raises(ConfigurationError, match="reference.source_url.*https"):
        load_pipeline_config(write_config(tmp_path, raw))


def test_load_pipeline_config_rejects_nested_reference_filename(
    tmp_path: Path,
) -> None:
    raw = valid_config()
    raw["reference"]["file_name"] = "nested/airports.dat"

    with pytest.raises(ConfigurationError, match="one file name"):
        load_pipeline_config(write_config(tmp_path, raw))
