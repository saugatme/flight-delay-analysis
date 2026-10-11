"""Download, validate, and preserve OpenFlights reference data in Bronze."""

import csv
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from flight_delay_analysis.bronze import sha256_file
from flight_delay_analysis.config import PipelineConfig
from flight_delay_analysis.hdfs import HdfsClient


class ReferenceIngestionError(RuntimeError):
    """Report an invalid reference file or inconsistent Bronze state."""


@dataclass(frozen=True)
class ReferenceFileStats:
    """Describe the validated shape of a headerless reference file."""

    row_count: int
    column_count: int


@dataclass(frozen=True)
class ReferenceManifest:
    """Record the identity, origin, and Bronze location of reference data."""

    source_name: str
    source_url: str
    source_file_name: str
    size_bytes: int
    sha256: str
    row_count: int
    column_count: int
    retrieved_at_utc: str
    ingested_at_utc: str
    license: str
    license_url: str
    hdfs_file_uri: str


@dataclass(frozen=True)
class ReferenceIngestionResult:
    """Describe local retrieval and HDFS ingestion outcomes."""

    local_status: str
    hdfs_status: str
    manifest: ReferenceManifest


REFERENCE_MANIFEST_FILE_NAME = "_ingestion_manifest.json"
DOWNLOAD_CHUNK_SIZE = 1024 * 1024


def reference_local_path(config: PipelineConfig, project_root: Path) -> Path:
    """Build the local reference path from configuration."""
    return project_root / config.reference.local_root / config.reference.file_name


def reference_bronze_uri(config: PipelineConfig) -> str:
    """Build the configured HDFS Bronze reference directory URI."""
    return f"{config.hdfs.base_uri}/{config.reference.hdfs_bronze_dataset}"


def reference_manifest_uri(config: PipelineConfig) -> str:
    """Return the final HDFS manifest URI for the reference dataset."""
    return f"{reference_bronze_uri(config)}/{REFERENCE_MANIFEST_FILE_NAME}"


def validate_reference_file(path: Path, expected_columns: int) -> ReferenceFileStats:
    """Require a non-empty UTF-8 CSV with the expected fields on every row."""
    row_count = 0
    with path.open(encoding="utf-8", newline="") as source:
        for row_number, row in enumerate(csv.reader(source), start=1):
            if len(row) != expected_columns:
                raise ReferenceIngestionError(
                    "Reference row has an unexpected field count: "
                    f"row {row_number}, expected {expected_columns}, found {len(row)}"
                )
            row_count += 1

    if row_count == 0:
        raise ReferenceIngestionError(f"Reference file is empty: {path}")

    return ReferenceFileStats(
        row_count=row_count,
        column_count=expected_columns,
    )


def ensure_local_reference(
    config: PipelineConfig,
    project_root: Path,
    opener: Any = urlopen,
) -> tuple[Path, str, ReferenceFileStats]:
    """Reuse a valid local source or download and atomically accept a new one."""
    target = reference_local_path(config, project_root)
    if target.is_file():
        stats = validate_reference_file(target, config.reference.expected_columns)
        return target, "already_downloaded", stats
    if target.exists():
        raise ReferenceIngestionError(
            f"Configured reference path is not a regular file: {target}"
        )

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.downloading")
    request = Request(
        config.reference.source_url,
        headers={"User-Agent": "flight-delay-analysis/0.1"},
    )

    try:
        with opener(request, timeout=60) as response, temporary.open("wb") as output:
            while chunk := response.read(DOWNLOAD_CHUNK_SIZE):
                output.write(chunk)
        stats = validate_reference_file(
            temporary,
            config.reference.expected_columns,
        )
        temporary.replace(target)
    except Exception as error:
        temporary.unlink(missing_ok=True)
        if isinstance(error, ReferenceIngestionError):
            raise
        raise ReferenceIngestionError(
            f"Reference download failed from {config.reference.source_url}: {error}"
        ) from error

    return target, "downloaded", stats


def create_reference_manifest(
    source_file: Path,
    stats: ReferenceFileStats,
    config: PipelineConfig,
    recorded_at: datetime | None = None,
) -> ReferenceManifest:
    """Collect provenance and identity facts for one reference source."""
    timestamp = recorded_at or datetime.now(UTC)
    if timestamp.tzinfo is None:
        raise ValueError("recorded_at must include a timezone")
    timestamp_text = timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z")
    hdfs_file_uri = f"{reference_bronze_uri(config)}/{source_file.name}"

    return ReferenceManifest(
        source_name=config.reference.name,
        source_url=config.reference.source_url,
        source_file_name=source_file.name,
        size_bytes=source_file.stat().st_size,
        sha256=sha256_file(source_file),
        row_count=stats.row_count,
        column_count=stats.column_count,
        retrieved_at_utc=timestamp_text,
        ingested_at_utc=timestamp_text,
        license=config.reference.license,
        license_url=config.reference.license_url,
        hdfs_file_uri=hdfs_file_uri,
    )


def reference_manifest_to_json(manifest: ReferenceManifest) -> str:
    """Serialize a reference manifest in a stable, readable format."""
    return json.dumps(asdict(manifest), indent=2, sort_keys=True) + "\n"


def reference_manifest_from_json(content: str) -> ReferenceManifest:
    """Deserialize a stored reference manifest."""
    try:
        values: dict[str, Any] = json.loads(content)
        return ReferenceManifest(**values)
    except (json.JSONDecodeError, TypeError) as error:
        raise ValueError("Invalid reference ingestion manifest") from error


def same_reference(
    existing: ReferenceManifest,
    candidate: ReferenceManifest,
) -> bool:
    """Compare reference identity while ignoring processing timestamps."""
    ignored_fields = {"retrieved_at_utc", "ingested_at_utc"}
    existing_values = asdict(existing)
    candidate_values = asdict(candidate)
    return all(
        existing_values[field] == candidate_values[field]
        for field in existing_values
        if field not in ignored_fields
    )


def ingest_reference(
    config: PipelineConfig,
    project_root: Path,
    hdfs: HdfsClient,
    opener: Any = urlopen,
) -> ReferenceIngestionResult:
    """Download, validate, and copy unchanged reference bytes into HDFS Bronze."""
    source_file, local_status, stats = ensure_local_reference(
        config,
        project_root,
        opener,
    )
    candidate = create_reference_manifest(source_file, stats, config)
    final_manifest_uri = reference_manifest_uri(config)
    final_file_exists = hdfs.exists(candidate.hdfs_file_uri)

    if hdfs.exists(final_manifest_uri):
        existing = reference_manifest_from_json(hdfs.read_text(final_manifest_uri))
        if not same_reference(existing, candidate):
            raise ReferenceIngestionError(
                "Reference Bronze directory contains a different source manifest"
            )
        if not final_file_exists:
            raise ReferenceIngestionError(
                "Reference manifest exists, but its source file is missing"
            )
        return ReferenceIngestionResult(
            local_status=local_status,
            hdfs_status="already_ingested",
            manifest=existing,
        )

    directory_uri = reference_bronze_uri(config)
    checksum_tag = candidate.sha256[:12]
    staged_file_uri = (
        f"{directory_uri}/.{candidate.source_file_name}.{checksum_tag}.uploading"
    )
    staged_manifest_uri = (
        f"{directory_uri}/.{REFERENCE_MANIFEST_FILE_NAME}.{checksum_tag}.uploading"
    )

    if final_file_exists:
        if hdfs.exists(staged_manifest_uri):
            staged_manifest = reference_manifest_from_json(
                hdfs.read_text(staged_manifest_uri)
            )
            if same_reference(staged_manifest, candidate):
                hdfs.rename(staged_manifest_uri, final_manifest_uri)
                return ReferenceIngestionResult(
                    local_status=local_status,
                    hdfs_status="recovered",
                    manifest=candidate,
                )
        raise ReferenceIngestionError(
            "Reference source exists in Bronze without a matching final manifest"
        )

    hdfs.make_directory(directory_uri)
    if not hdfs.exists(staged_file_uri):
        hdfs.upload_file(source_file, staged_file_uri)
    if not hdfs.exists(staged_manifest_uri):
        hdfs.upload_text(reference_manifest_to_json(candidate), staged_manifest_uri)

    hdfs.rename(staged_file_uri, candidate.hdfs_file_uri)
    hdfs.rename(staged_manifest_uri, final_manifest_uri)
    return ReferenceIngestionResult(
        local_status=local_status,
        hdfs_status="ingested",
        manifest=candidate,
    )
