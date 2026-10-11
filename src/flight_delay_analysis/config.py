"""Load and validate shared pipeline configuration."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml


class ConfigurationError(ValueError):
    """Report an invalid pipeline configuration."""


@dataclass(frozen=True)
class RunConfig:
    year: int
    month: int


@dataclass(frozen=True)
class SourceConfig:
    format: str
    local_root: Path
    file_pattern: str


@dataclass(frozen=True)
class HdfsConfig:
    base_uri: str
    bronze_dataset: str
    silver_dataset: str
    silver_rejected_dataset: str
    silver_quality_dataset: str
    enriched_silver_dataset: str
    enrichment_quality_dataset: str
    gold_dataset: str


@dataclass(frozen=True)
class QualityConfig:
    expected_rows: int
    expected_source_columns: int
    delay_threshold_minutes: int


@dataclass(frozen=True)
class ReferenceConfig:
    name: str
    source_url: str
    local_root: Path
    file_name: str
    format: str
    expected_columns: int
    missing_value: str
    license: str
    license_url: str
    hdfs_bronze_dataset: str
    hdfs_silver_dataset: str
    hdfs_rejected_dataset: str
    hdfs_quality_dataset: str


@dataclass(frozen=True)
class MongoDbConfig:
    host: str
    port: int
    database: str


@dataclass(frozen=True)
class PipelineConfig:
    project_name: str
    run: RunConfig
    source: SourceConfig
    hdfs: HdfsConfig
    quality: QualityConfig
    reference: ReferenceConfig
    mongodb: MongoDbConfig


def _mapping(parent: dict[str, Any], key: str) -> dict[str, Any]:
    value = parent.get(key)
    if not isinstance(value, dict):
        raise ConfigurationError(f"'{key}' must be a YAML mapping")
    return value


def _required(section: dict[str, Any], key: str, section_name: str) -> Any:
    value = section.get(key)
    if value is None or value == "":
        raise ConfigurationError(f"'{section_name}.{key}' is required")
    return value


def _positive_int(section: dict[str, Any], key: str, section_name: str) -> int:
    value = _required(section, key, section_name)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigurationError(f"'{section_name}.{key}' must be a positive integer")
    return value


def _dataset_path(section: dict[str, Any], key: str, section_name: str = "hdfs") -> str:
    value = str(_required(section, key, section_name))
    if value.startswith("/") or ".." in Path(value).parts:
        raise ConfigurationError(
            f"'{section_name}.{key}' must be a relative path without '..'"
        )
    return value.rstrip("/")


def load_pipeline_config(path: str | Path) -> PipelineConfig:
    """Read one YAML file and return validated, typed pipeline settings."""
    config_path = Path(path)
    try:
        with config_path.open(encoding="utf-8") as config_file:
            raw = yaml.safe_load(config_file)
    except FileNotFoundError as error:
        raise ConfigurationError(
            f"Configuration file not found: {config_path}"
        ) from error
    except yaml.YAMLError as error:
        raise ConfigurationError(f"Invalid YAML in {config_path}: {error}") from error

    if not isinstance(raw, dict):
        raise ConfigurationError("The YAML root must be a mapping")

    project = _mapping(raw, "project")
    run = _mapping(raw, "run")
    source = _mapping(raw, "source")
    hdfs = _mapping(raw, "hdfs")
    quality = _mapping(raw, "quality")
    reference = _mapping(raw, "reference")
    mongodb = _mapping(raw, "mongodb")

    year = _positive_int(run, "year", "run")
    month = _positive_int(run, "month", "run")
    if month > 12:
        raise ConfigurationError("'run.month' must be between 1 and 12")

    source_format = str(_required(source, "format", "source")).lower()
    if source_format != "csv":
        raise ConfigurationError("'source.format' must be 'csv'")

    local_root = Path(_required(source, "local_root", "source"))
    if local_root.is_absolute() or ".." in local_root.parts:
        raise ConfigurationError(
            "'source.local_root' must be a relative path without '..'"
        )

    file_pattern = str(_required(source, "file_pattern", "source"))
    pattern_path = Path(file_pattern)
    if pattern_path.is_absolute() or ".." in pattern_path.parts:
        raise ConfigurationError(
            "'source.file_pattern' must be a relative pattern without '..'"
        )

    base_uri = str(_required(hdfs, "base_uri", "hdfs")).rstrip("/")
    parsed_uri = urlparse(base_uri)
    if parsed_uri.scheme != "hdfs" or not parsed_uri.netloc or not parsed_uri.path:
        raise ConfigurationError("'hdfs.base_uri' must be an absolute hdfs:// URI")

    port = _positive_int(mongodb, "port", "mongodb")
    if port > 65535:
        raise ConfigurationError("'mongodb.port' must be at most 65535")

    return PipelineConfig(
        project_name=str(_required(project, "name", "project")),
        run=RunConfig(year=year, month=month),
        source=SourceConfig(
            format=source_format,
            local_root=local_root,
            file_pattern=file_pattern,
        ),
        hdfs=HdfsConfig(
            base_uri=base_uri,
            bronze_dataset=_dataset_path(hdfs, "bronze_dataset"),
            silver_dataset=_dataset_path(hdfs, "silver_dataset"),
            silver_rejected_dataset=_dataset_path(hdfs, "silver_rejected_dataset"),
            silver_quality_dataset=_dataset_path(hdfs, "silver_quality_dataset"),
            enriched_silver_dataset=_dataset_path(hdfs, "enriched_silver_dataset"),
            enrichment_quality_dataset=_dataset_path(
                hdfs, "enrichment_quality_dataset"
            ),
            gold_dataset=_dataset_path(hdfs, "gold_dataset"),
        ),
        quality=QualityConfig(
            expected_rows=_positive_int(quality, "expected_rows", "quality"),
            expected_source_columns=_positive_int(
                quality, "expected_source_columns", "quality"
            ),
            delay_threshold_minutes=_positive_int(
                quality, "delay_threshold_minutes", "quality"
            ),
        ),
        reference=_load_reference_config(reference),
        mongodb=MongoDbConfig(
            host=str(_required(mongodb, "host", "mongodb")),
            port=port,
            database=str(_required(mongodb, "database", "mongodb")),
        ),
    )


def _load_reference_config(reference: dict[str, Any]) -> ReferenceConfig:
    """Validate the OpenFlights reference-data settings."""
    source_url = str(_required(reference, "source_url", "reference"))
    parsed_source_url = urlparse(source_url)
    if parsed_source_url.scheme != "https" or not parsed_source_url.netloc:
        raise ConfigurationError("'reference.source_url' must be an https:// URL")

    license_url = str(_required(reference, "license_url", "reference"))
    parsed_license_url = urlparse(license_url)
    if parsed_license_url.scheme != "https" or not parsed_license_url.netloc:
        raise ConfigurationError("'reference.license_url' must be an https:// URL")

    local_root = Path(_required(reference, "local_root", "reference"))
    file_name = Path(_required(reference, "file_name", "reference"))
    if local_root.is_absolute() or ".." in local_root.parts:
        raise ConfigurationError(
            "'reference.local_root' must be a relative path without '..'"
        )
    if (
        file_name.is_absolute()
        or len(file_name.parts) != 1
        or file_name.name in {".", ".."}
    ):
        raise ConfigurationError("'reference.file_name' must be one file name")

    reference_format = str(_required(reference, "format", "reference"))
    if reference_format != "csv_no_header":
        raise ConfigurationError("'reference.format' must be 'csv_no_header'")

    return ReferenceConfig(
        name=str(_required(reference, "name", "reference")),
        source_url=source_url,
        local_root=local_root,
        file_name=file_name.name,
        format=reference_format,
        expected_columns=_positive_int(reference, "expected_columns", "reference"),
        missing_value=str(_required(reference, "missing_value", "reference")),
        license=str(_required(reference, "license", "reference")),
        license_url=license_url,
        hdfs_bronze_dataset=_dataset_path(
            reference, "hdfs_bronze_dataset", "reference"
        ),
        hdfs_silver_dataset=_dataset_path(
            reference, "hdfs_silver_dataset", "reference"
        ),
        hdfs_rejected_dataset=_dataset_path(
            reference, "hdfs_rejected_dataset", "reference"
        ),
        hdfs_quality_dataset=_dataset_path(
            reference, "hdfs_quality_dataset", "reference"
        ),
    )
