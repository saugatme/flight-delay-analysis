from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from flight_delay_analysis.bronze import (
    BronzeIngestionError,
    IngestionManifest,
    SourceResolutionError,
    bronze_partition_uri,
    count_csv_rows,
    create_manifest,
    ingest_bronze,
    manifest_from_json,
    manifest_to_json,
    manifest_uri,
    resolve_source_file,
    same_source,
    sha256_file,
    source_partition,
)
from flight_delay_analysis.config import load_pipeline_config


class MemoryHdfsClient:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.directories: set[str] = set()

    def exists(self, uri: str) -> bool:
        return uri in self.files or uri in self.directories

    def make_directory(self, uri: str) -> None:
        self.directories.add(uri)

    def upload_file(self, local_path: Path, hdfs_uri: str) -> None:
        self.files[hdfs_uri] = local_path.read_bytes()

    def upload_text(self, content: str, hdfs_uri: str) -> None:
        self.files[hdfs_uri] = content.encode()

    def read_text(self, hdfs_uri: str) -> str:
        return self.files[hdfs_uri].decode()

    def rename(self, source_uri: str, destination_uri: str) -> None:
        self.files[destination_uri] = self.files.pop(source_uri)


@pytest.fixture
def pipeline_config():
    return load_pipeline_config(Path("config/pipeline.yml"))


def test_source_partition_uses_configured_month(pipeline_config, tmp_path: Path) -> None:
    partition = source_partition(pipeline_config, tmp_path)

    assert partition == tmp_path / "data/raw/2025-01"


def test_resolve_source_file_returns_only_match(pipeline_config, tmp_path: Path) -> None:
    partition = source_partition(pipeline_config, tmp_path)
    partition.mkdir(parents=True)
    source_file = partition / "flights.csv"
    source_file.write_text("FlightDate\n2025-01-01\n", encoding="utf-8")

    assert resolve_source_file(pipeline_config, tmp_path) == source_file.resolve()


def test_resolve_source_file_rejects_missing_input(
    pipeline_config, tmp_path: Path
) -> None:
    with pytest.raises(SourceResolutionError, match="No source file matched"):
        resolve_source_file(pipeline_config, tmp_path)


def test_resolve_source_file_rejects_multiple_inputs(
    pipeline_config, tmp_path: Path
) -> None:
    partition = source_partition(pipeline_config, tmp_path)
    partition.mkdir(parents=True)
    (partition / "first.csv").touch()
    (partition / "second.csv").touch()

    with pytest.raises(SourceResolutionError, match="found 2"):
        resolve_source_file(pipeline_config, tmp_path)


def test_resolve_source_file_uses_configured_pattern(
    pipeline_config, tmp_path: Path
) -> None:
    partition = source_partition(pipeline_config, tmp_path)
    partition.mkdir(parents=True)
    expected = partition / "flights.zip"
    expected.touch()
    (partition / "flights.csv").touch()
    source = replace(
        pipeline_config.source,
        file_pattern="*.zip",
    )
    zip_config = replace(pipeline_config, source=source)

    assert resolve_source_file(zip_config, tmp_path) == expected.resolve()


def test_bronze_partition_uri_uses_hive_style_partitions(pipeline_config) -> None:
    assert bronze_partition_uri(pipeline_config) == (
        "hdfs://localhost:9000/flight-delay/bronze/bts/year=2025/month=01"
    )


def test_create_manifest_records_source_facts(pipeline_config, tmp_path: Path) -> None:
    source_file = tmp_path / "flights.csv"
    source_file.write_text(
        'FlightDate,Origin,Note\n2025-01-01,JFK,"contains, comma"\n'
        "2025-01-02,LAX,clear\n",
        encoding="utf-8",
    )
    timestamp = datetime(2026, 10, 10, 12, 0, tzinfo=UTC)

    manifest = create_manifest(source_file, pipeline_config, timestamp)

    assert manifest.source_file_name == "flights.csv"
    assert manifest.size_bytes == source_file.stat().st_size
    assert manifest.sha256 == sha256_file(source_file)
    assert manifest.row_count == 2
    assert manifest.ingested_at_utc == "2026-10-10T12:00:00Z"
    assert manifest.hdfs_file_uri.endswith(
        "/bronze/bts/year=2025/month=01/flights.csv"
    )


def test_count_csv_rows_rejects_empty_file(tmp_path: Path) -> None:
    source_file = tmp_path / "empty.csv"
    source_file.touch()

    with pytest.raises(SourceResolutionError, match="Source CSV is empty"):
        count_csv_rows(source_file)


def test_manifest_json_round_trip() -> None:
    manifest = IngestionManifest(
        source_file_name="flights.csv",
        size_bytes=100,
        sha256="abc123",
        row_count=2,
        ingested_at_utc="2026-10-10T12:00:00Z",
        hdfs_file_uri="hdfs://localhost:9000/flight-delay/bronze/flights.csv",
    )

    assert manifest_from_json(manifest_to_json(manifest)) == manifest


def test_same_source_ignores_ingestion_timestamp() -> None:
    existing = IngestionManifest(
        source_file_name="flights.csv",
        size_bytes=100,
        sha256="abc123",
        row_count=2,
        ingested_at_utc="2026-10-10T12:00:00Z",
        hdfs_file_uri="hdfs://localhost:9000/flight-delay/bronze/flights.csv",
    )
    candidate = replace(existing, ingested_at_utc="2026-10-11T12:00:00Z")

    assert same_source(existing, candidate)
    assert not same_source(existing, replace(candidate, sha256="different"))


def small_source_config(pipeline_config, expected_rows: int = 2):
    quality = replace(pipeline_config.quality, expected_rows=expected_rows)
    return replace(pipeline_config, quality=quality)


def write_month_source(project_root: Path, content: str) -> Path:
    partition = project_root / "data/raw/2025-01"
    partition.mkdir(parents=True)
    source = partition / "flights.csv"
    source.write_text(content, encoding="utf-8")
    return source


def test_ingest_bronze_uploads_source_and_manifest(
    pipeline_config, tmp_path: Path
) -> None:
    config = small_source_config(pipeline_config)
    source = write_month_source(tmp_path, "FlightDate\n2025-01-01\n2025-01-02\n")
    hdfs = MemoryHdfsClient()

    result = ingest_bronze(config, tmp_path, hdfs)

    assert result.status == "ingested"
    assert hdfs.files[result.manifest.hdfs_file_uri] == source.read_bytes()
    stored_manifest = manifest_from_json(hdfs.read_text(manifest_uri(config)))
    assert same_source(stored_manifest, result.manifest)
    assert not any(uri.endswith(".uploading") for uri in hdfs.files)


def test_ingest_bronze_accepts_safe_rerun(pipeline_config, tmp_path: Path) -> None:
    config = small_source_config(pipeline_config)
    write_month_source(tmp_path, "FlightDate\n2025-01-01\n2025-01-02\n")
    hdfs = MemoryHdfsClient()
    ingest_bronze(config, tmp_path, hdfs)

    result = ingest_bronze(config, tmp_path, hdfs)

    assert result.status == "already_ingested"


def test_ingest_bronze_rejects_conflicting_rerun(
    pipeline_config, tmp_path: Path
) -> None:
    config = small_source_config(pipeline_config)
    source = write_month_source(tmp_path, "FlightDate\n2025-01-01\n2025-01-02\n")
    hdfs = MemoryHdfsClient()
    ingest_bronze(config, tmp_path, hdfs)
    source.write_text("FlightDate\n2025-01-03\n2025-01-04\n", encoding="utf-8")

    with pytest.raises(BronzeIngestionError, match="different source manifest"):
        ingest_bronze(config, tmp_path, hdfs)


def test_ingest_bronze_rejects_unexpected_row_count(
    pipeline_config, tmp_path: Path
) -> None:
    config = small_source_config(pipeline_config, expected_rows=3)
    write_month_source(tmp_path, "FlightDate\n2025-01-01\n2025-01-02\n")

    with pytest.raises(BronzeIngestionError, match="expected 3, found 2"):
        ingest_bronze(config, tmp_path, MemoryHdfsClient())
