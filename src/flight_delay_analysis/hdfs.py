"""Small, testable interface to the installed HDFS command-line client."""

import subprocess
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path


class HdfsCommandError(RuntimeError):
    """Report an unsuccessful HDFS command."""


CommandResult = subprocess.CompletedProcess[str]
CommandRunner = Callable[..., CommandResult]


class HdfsClient:
    """Run HDFS file operations without invoking a shell."""

    def __init__(
        self,
        executable: str = "hdfs",
        runner: CommandRunner | None = None,
    ) -> None:
        self.executable = executable
        self._runner = runner or subprocess.run

    def _run(self, arguments: Sequence[str], *, check: bool = True) -> CommandResult:
        command = [self.executable, "dfs", *arguments]
        result = self._runner(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
        if check and result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "unknown error"
            raise HdfsCommandError(
                f"HDFS command failed with exit code {result.returncode}: {detail}"
            )
        return result

    def exists(self, uri: str) -> bool:
        """Return whether an HDFS path exists."""
        result = self._run(["-test", "-e", uri], check=False)
        if result.returncode == 0:
            return True
        if result.returncode == 1 and not result.stderr.strip():
            return False
        detail = result.stderr.strip() or result.stdout.strip() or "unknown error"
        raise HdfsCommandError(
            f"HDFS existence check failed with exit code {result.returncode}: {detail}"
        )

    def make_directory(self, uri: str) -> None:
        """Create an HDFS directory and any missing parents."""
        self._run(["-mkdir", "-p", uri])

    def upload_file(self, local_path: Path, hdfs_uri: str) -> None:
        """Upload a local file without silently overwriting an HDFS file."""
        if not local_path.is_file():
            raise FileNotFoundError(f"Local upload source not found: {local_path}")
        self._run(["-put", str(local_path), hdfs_uri])

    def read_text(self, hdfs_uri: str) -> str:
        """Read a small UTF-8 HDFS file, such as a manifest."""
        return self._run(["-cat", hdfs_uri]).stdout

    def rename(self, source_uri: str, destination_uri: str) -> None:
        """Atomically rename an HDFS path without overwriting the destination."""
        self._run(["-mv", source_uri, destination_uri])

    def upload_text(self, content: str, hdfs_uri: str) -> None:
        """Write text through a temporary local file, then upload it to HDFS."""
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            prefix="flight-delay-",
            suffix=".json",
        ) as temporary_file:
            temporary_file.write(content)
            temporary_file.flush()
            self.upload_file(Path(temporary_file.name), hdfs_uri)
