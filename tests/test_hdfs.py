import subprocess
from pathlib import Path

import pytest

from flight_delay_analysis.hdfs import HdfsClient, HdfsCommandError


class RecordingRunner:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.commands: list[list[str]] = []

    def __call__(self, command, **kwargs):
        self.commands.append(command)
        return subprocess.CompletedProcess(
            command,
            self.returncode,
            stdout=self.stdout,
            stderr=self.stderr,
        )


def test_exists_returns_true_for_existing_path() -> None:
    runner = RecordingRunner(returncode=0)
    client = HdfsClient(runner=runner)

    assert client.exists("hdfs://localhost:9000/flight-delay/bronze")
    assert runner.commands == [
        [
            "hdfs",
            "dfs",
            "-test",
            "-e",
            "hdfs://localhost:9000/flight-delay/bronze",
        ]
    ]


def test_exists_returns_false_for_missing_path() -> None:
    client = HdfsClient(runner=RecordingRunner(returncode=1))

    assert not client.exists("hdfs://localhost:9000/missing")


def test_exists_raises_for_hdfs_failure() -> None:
    client = HdfsClient(
        runner=RecordingRunner(returncode=1, stderr="connection refused")
    )

    with pytest.raises(HdfsCommandError, match="connection refused"):
        client.exists("hdfs://localhost:9000/flight-delay")


def test_make_directory_uses_parent_option() -> None:
    runner = RecordingRunner()
    client = HdfsClient(runner=runner)

    client.make_directory("hdfs://localhost:9000/flight-delay/bronze")

    assert runner.commands[0][2:4] == ["-mkdir", "-p"]


def test_upload_file_preserves_filename_as_one_argument(tmp_path: Path) -> None:
    source = tmp_path / "source file (2025).csv"
    source.write_text("header\nvalue\n", encoding="utf-8")
    runner = RecordingRunner()
    client = HdfsClient(runner=runner)

    client.upload_file(source, "hdfs://localhost:9000/bronze/")

    assert runner.commands[0] == [
        "hdfs",
        "dfs",
        "-put",
        str(source),
        "hdfs://localhost:9000/bronze/",
    ]


def test_upload_file_rejects_missing_local_file(tmp_path: Path) -> None:
    client = HdfsClient(runner=RecordingRunner())

    with pytest.raises(FileNotFoundError, match="Local upload source not found"):
        client.upload_file(tmp_path / "missing.csv", "hdfs://localhost:9000/bronze/")


def test_read_text_returns_command_output() -> None:
    runner = RecordingRunner(stdout='{"row_count": 2}\n')
    client = HdfsClient(runner=runner)

    content = client.read_text("hdfs://localhost:9000/bronze/_manifest.json")

    assert content == '{"row_count": 2}\n'
    assert runner.commands[0][2] == "-cat"


def test_rename_uses_hdfs_move() -> None:
    runner = RecordingRunner()
    client = HdfsClient(runner=runner)

    client.rename("hdfs://localhost/staged", "hdfs://localhost/final")

    assert runner.commands[0] == [
        "hdfs",
        "dfs",
        "-mv",
        "hdfs://localhost/staged",
        "hdfs://localhost/final",
    ]


def test_command_failure_does_not_hide_error() -> None:
    client = HdfsClient(runner=RecordingRunner(returncode=1, stderr="permission denied"))

    with pytest.raises(HdfsCommandError, match="permission denied"):
        client.make_directory("hdfs://localhost:9000/restricted")
