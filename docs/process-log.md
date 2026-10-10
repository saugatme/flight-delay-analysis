# Project Process Log

## 2026-10-08 — Local environment

- Selected WSL 2 for the Linux development environment.
- Stored the project under `~/projects` instead of OneDrive.
- Selected local PySpark. This is not a distributed cluster.
- Installed uv 0.12.23.
- Installed uv-managed Python 3.12.15.
- Confirmed Java 17.0.20.1.
- Confirmed Git 2.43.0.
- Installed and authenticated GitHub CLI.
- Created a private GitHub repository.
- Initialized a packaged uv project with a `src/` layout.
- Added PySpark, pandas, PyArrow, Plotly, and Kaleido.
- Added pytest and Ruff as development tools.

## 2026-10-10 — Spark setup check

- Started PySpark 4.2.0 in local mode with Java 17.
- Created and displayed a one-row Spark DataFrame.
- Confirmed that Python, Java, and PySpark work together in WSL.
- Confirmed that Ruff passes.
- Observed normal WSL and native Hadoop startup warnings.