from dataclasses import replace
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

import pytest

from flight_delay_analysis.config import load_pipeline_config
from flight_delay_analysis.reference import (
    ReferenceFileStats,
    ReferenceIngestionError,
    create_reference_manifest,
    ensure_local_reference,
    ingest_reference,
    reference_bronze_uri,
    reference_local_path,
    reference_manifest_from_json,
    reference_manifest_to_json,
    reference_manifest_uri,
    same_reference,
    validate_reference_file,
)


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


def reference_csv(rows: int = 2) -> bytes:
    values = [
        '1,"Airport, One",City,Country,AAA,AAAA,1.0,2.0,3,4,E,Zone,airport,OurAirports',
        "2,Airport Two,City,Country,BBB,BBBB,5.0,6.0,7,8,U,Zone,airport,OurAirports",
    ]
    return ("\n".join(values[:rows]) + "\n").encode()


def fake_opener(content: bytes):
    def open_response(request, timeout: int):
        assert request.full_url.endswith("/airports.dat")
        assert timeout == 60
        return BytesIO(content)

    return open_response


def test_reference_paths_use_configuration(pipeline_config, tmp_path: Path) -> None:
    assert reference_local_path(pipeline_config, tmp_path) == (
        tmp_path / "data/reference/openflights/airports.dat"
    )
    assert reference_bronze_uri(pipeline_config) == (
        "hdfs://localhost:9000/flight-delay/bronze/reference/openflights/airports"
    )


def test_validate_reference_file_counts_headerless_rows(tmp_path: Path) -> None:
    source = tmp_path / "airports.dat"
    source.write_bytes(reference_csv())

    assert validate_reference_file(source, 14) == ReferenceFileStats(
        row_count=2,
        column_count=14,
    )


def test_validate_reference_file_rejects_wrong_field_count(tmp_path: Path) -> None:
    source = tmp_path / "airports.dat"
    source.write_text("1,only,two\n", encoding="utf-8")

    with pytest.raises(ReferenceIngestionError, match="row 1.*expected 14, found 3"):
        validate_reference_file(source, 14)


def test_ensure_local_reference_downloads_and_preserves_bytes(
    pipeline_config, tmp_path: Path
) -> None:
    content = reference_csv()

    source, status, stats = ensure_local_reference(
        pipeline_config,
        tmp_path,
        fake_opener(content),
    )

    assert status == "downloaded"
    assert source.read_bytes() == content
    assert stats.row_count == 2
    assert not source.with_name(".airports.dat.downloading").exists()


def test_ensure_local_reference_reuses_valid_file_without_network(
    pipeline_config, tmp_path: Path
) -> None:
    source = reference_local_path(pipeline_config, tmp_path)
    source.parent.mkdir(parents=True)
    source.write_bytes(reference_csv())

    def fail_if_called(*args, **kwargs):
        raise AssertionError("network should not be used")

    resolved, status, _ = ensure_local_reference(
        pipeline_config,
        tmp_path,
        fail_if_called,
    )

    assert resolved == source
    assert status == "already_downloaded"


def test_reference_manifest_round_trip(pipeline_config, tmp_path: Path) -> None:
    source = reference_local_path(pipeline_config, tmp_path)
    source.parent.mkdir(parents=True)
    source.write_bytes(reference_csv())
    timestamp = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)
    manifest = create_reference_manifest(
        source,
        ReferenceFileStats(row_count=2, column_count=14),
        pipeline_config,
        timestamp,
    )

    assert manifest.retrieved_at_utc == "2026-10-11T12:00:00Z"
    assert manifest.ingested_at_utc == "2026-10-11T12:00:00Z"
    assert (
        reference_manifest_from_json(reference_manifest_to_json(manifest)) == manifest
    )
    assert same_reference(
        manifest,
        replace(manifest, ingested_at_utc="2026-10-12T12:00:00Z"),
    )


def test_ingest_reference_uploads_unchanged_source_and_manifest(
    pipeline_config, tmp_path: Path
) -> None:
    content = reference_csv()
    hdfs = MemoryHdfsClient()

    result = ingest_reference(
        pipeline_config,
        tmp_path,
        hdfs,
        fake_opener(content),
    )

    assert result.local_status == "downloaded"
    assert result.hdfs_status == "ingested"
    assert hdfs.files[result.manifest.hdfs_file_uri] == content
    stored = reference_manifest_from_json(
        hdfs.read_text(reference_manifest_uri(pipeline_config))
    )
    assert same_reference(stored, result.manifest)
    assert not any(uri.endswith(".uploading") for uri in hdfs.files)


def test_ingest_reference_accepts_safe_rerun(pipeline_config, tmp_path: Path) -> None:
    content = reference_csv()
    hdfs = MemoryHdfsClient()
    ingest_reference(pipeline_config, tmp_path, hdfs, fake_opener(content))

    result = ingest_reference(
        pipeline_config,
        tmp_path,
        hdfs,
        fake_opener(b"different content must not be downloaded"),
    )

    assert result.local_status == "already_downloaded"
    assert result.hdfs_status == "already_ingested"


def test_ingest_reference_rejects_conflicting_local_source(
    pipeline_config, tmp_path: Path
) -> None:
    hdfs = MemoryHdfsClient()
    ingest_reference(
        pipeline_config,
        tmp_path,
        hdfs,
        fake_opener(reference_csv()),
    )
    source = reference_local_path(pipeline_config, tmp_path)
    source.write_bytes(reference_csv(rows=1))

    with pytest.raises(ReferenceIngestionError, match="different source manifest"):
        ingest_reference(pipeline_config, tmp_path, hdfs)
