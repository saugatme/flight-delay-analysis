"""Resolve and describe local source data for Bronze ingestion."""

import csv
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from flight_delay_analysis.config import PipelineConfig
from flight_delay_analysis.hdfs import HdfsClient


class SourceResolutionError(ValueError):
    """Report missing or ambiguous source input."""


class BronzeIngestionError(RuntimeError):
    """Report a source conflict or inconsistent Bronze state."""


@dataclass(frozen=True)
class IngestionManifest:
    """Facts that identify one source file and its Bronze destination."""

    source_file_name: str
    size_bytes: int
    sha256: str
    row_count: int
    ingested_at_utc: str
    hdfs_file_uri: str


@dataclass(frozen=True)
class IngestionResult:
    """Describe whether a Bronze ingestion wrote data or was a safe rerun."""

    status: str
    manifest: IngestionManifest


MANIFEST_FILE_NAME = "_ingestion_manifest.json"


def source_partition(config: PipelineConfig, project_root: Path) -> Path:
    """Build the local monthly input directory from configuration."""
    month_partition = f"{config.run.year:04d}-{config.run.month:02d}"
    return project_root / config.source.local_root / month_partition


def resolve_source_file(config: PipelineConfig, project_root: Path) -> Path:
    """Return the single regular source file selected by the configured pattern."""
    partition = source_partition(config, project_root)
    matches = sorted(
        path.resolve()
        for path in partition.glob(config.source.file_pattern)
        if path.is_file()
    )

    if not matches:
        raise SourceResolutionError(
            f"No source file matched {config.source.file_pattern!r} in {partition}"
        )
    if len(matches) > 1:
        names = ", ".join(path.name for path in matches)
        raise SourceResolutionError(
            f"Expected one source file in {partition}, found {len(matches)}: {names}"
        )

    return matches[0]


def bronze_partition_uri(config: PipelineConfig) -> str:
    """Build the configured HDFS Bronze partition URI."""
    return (
        f"{config.hdfs.base_uri}/{config.hdfs.bronze_dataset}"
        f"/year={config.run.year:04d}/month={config.run.month:02d}"
    )


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Calculate a file checksum without loading the whole file into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def count_csv_rows(path: Path) -> int:
    """Count CSV data records, excluding the header record."""
    with path.open(encoding="utf-8-sig", newline="") as source:
        rows = csv.reader(source)
        try:
            next(rows)
        except StopIteration as error:
            raise SourceResolutionError(f"Source CSV is empty: {path}") from error
        return sum(1 for _ in rows)


def create_manifest(
    source_file: Path,
    config: PipelineConfig,
    ingested_at: datetime | None = None,
) -> IngestionManifest:
    """Collect reproducible source facts and the intended HDFS file URI."""
    timestamp = ingested_at or datetime.now(UTC)
    if timestamp.tzinfo is None:
        raise ValueError("ingested_at must include a timezone")

    partition_uri = bronze_partition_uri(config)
    return IngestionManifest(
        source_file_name=source_file.name,
        size_bytes=source_file.stat().st_size,
        sha256=sha256_file(source_file),
        row_count=count_csv_rows(source_file),
        ingested_at_utc=timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        hdfs_file_uri=f"{partition_uri}/{source_file.name}",
    )


def manifest_to_json(manifest: IngestionManifest) -> str:
    """Serialize a manifest in a stable, readable format."""
    return json.dumps(asdict(manifest), indent=2, sort_keys=True) + "\n"


def manifest_from_json(content: str) -> IngestionManifest:
    """Deserialize a stored manifest and reject missing or extra fields."""
    try:
        values: dict[str, Any] = json.loads(content)
        return IngestionManifest(**values)
    except (json.JSONDecodeError, TypeError) as error:
        raise ValueError("Invalid ingestion manifest") from error


def same_source(
    existing: IngestionManifest,
    candidate: IngestionManifest,
) -> bool:
    """Compare source identity while ignoring the new attempt's timestamp."""
    return (
        existing.source_file_name == candidate.source_file_name
        and existing.size_bytes == candidate.size_bytes
        and existing.sha256 == candidate.sha256
        and existing.row_count == candidate.row_count
        and existing.hdfs_file_uri == candidate.hdfs_file_uri
    )


def manifest_uri(config: PipelineConfig) -> str:
    """Return the final HDFS manifest URI for one monthly partition."""
    return f"{bronze_partition_uri(config)}/{MANIFEST_FILE_NAME}"


def ingest_bronze(
    config: PipelineConfig,
    project_root: Path,
    hdfs: HdfsClient,
) -> IngestionResult:
    """Copy one unchanged monthly source and its manifest into HDFS Bronze."""
    source_file = resolve_source_file(config, project_root)
    candidate = create_manifest(source_file, config)
    if candidate.row_count != config.quality.expected_rows:
        raise BronzeIngestionError(
            "Source row count does not match configuration: "
            f"expected {config.quality.expected_rows}, found {candidate.row_count}"
        )

    partition_uri = bronze_partition_uri(config)
    final_manifest_uri = manifest_uri(config)
    final_file_exists = hdfs.exists(candidate.hdfs_file_uri)

    if hdfs.exists(final_manifest_uri):
        existing = manifest_from_json(hdfs.read_text(final_manifest_uri))
        if not same_source(existing, candidate):
            raise BronzeIngestionError(
                "Bronze partition already contains a different source manifest"
            )
        if not final_file_exists:
            raise BronzeIngestionError(
                "Bronze manifest exists, but its source file is missing"
            )
        return IngestionResult(status="already_ingested", manifest=existing)

    checksum_tag = candidate.sha256[:12]
    staged_file_uri = (
        f"{partition_uri}/.{candidate.source_file_name}.{checksum_tag}.uploading"
    )
    staged_manifest_uri = (
        f"{partition_uri}/.{MANIFEST_FILE_NAME}.{checksum_tag}.uploading"
    )

    if final_file_exists:
        if hdfs.exists(staged_manifest_uri):
            staged_manifest = manifest_from_json(hdfs.read_text(staged_manifest_uri))
            if same_source(staged_manifest, candidate):
                hdfs.rename(staged_manifest_uri, final_manifest_uri)
                return IngestionResult(status="recovered", manifest=candidate)
        raise BronzeIngestionError(
            "Bronze source file exists without a matching final manifest"
        )

    hdfs.make_directory(partition_uri)
    if not hdfs.exists(staged_file_uri):
        hdfs.upload_file(source_file, staged_file_uri)
    if not hdfs.exists(staged_manifest_uri):
        hdfs.upload_text(manifest_to_json(candidate), staged_manifest_uri)

    hdfs.rename(staged_file_uri, candidate.hdfs_file_uri)
    hdfs.rename(staged_manifest_uri, final_manifest_uri)
    return IngestionResult(status="ingested", manifest=candidate)
