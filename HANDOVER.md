# Handover

## Current status

The project now follows the approved HDFS, Spark, MongoDB, and Plotly Dash architecture.

Completed environment and data checks:

- WSL 2 with Ubuntu 24.04.4 LTS.
- uv 0.12.23 and Python 3.12.15.
- Java 17.0.20.1 and PySpark 4.2.0.
- BTS January 2025 source with 539,747 rows and 110 columns.
- All 28 required fields are present.
- Existing local Parquet test output with 539,747 typed rows.
- Five tests for the local CSV-to-Parquet step.

Completed HDFS work:

- Apache Hadoop 3.5.0 is installed under `~/opt/hadoop-3.5.0`.
- HDFS uses `hdfs://localhost:9000`.
- The NameNode web interface uses `http://localhost:9870`.
- NameNode, DataNode, and SecondaryNameNode start successfully.
- `hdfs dfsadmin -report` shows one live DataNode.
- HDFS reports zero missing or corrupt blocks.
- Bronze, Silver, and Gold directories exist below `/flight-delay`.

Completed configuration work:

- `config/pipeline.yml` defines the run partition, local source pattern, HDFS layers, quality expectations, delay threshold, and MongoDB endpoint.
- PyYAML is installed through uv.
- The tested loader returns immutable typed settings and rejects invalid values and unsafe paths.

Completed Bronze ingestion work:

- The source resolver requires exactly one configured monthly CSV.
- The ingestion manifest records the source name, byte size, SHA-256 checksum, row count, UTC ingestion time, and HDFS URI.
- The Hadoop CLI adapter uses argument lists and does not invoke a shell.
- The ingestion uses temporary HDFS names and atomic renames.
- A safe rerun is accepted, and a conflicting source is rejected.
- The unchanged January CSV and its manifest are stored in HDFS Bronze.
- Ruff and all 34 automated tests pass.

## Important limits

- HDFS is pseudo-distributed on one computer. It is not a multi-computer cluster.
- The validated Parquet data is not in HDFS Silver yet.
- MongoDB is not installed.
- No Gold result table or dashboard is complete.
- A local untracked prototype named `analyze_carrier_delays.py` reads local CSV and does not fit the target pipeline. It is not part of the committed project. Replace it later instead of extending it.

## Start a work session

```bash
source ~/.bashrc
start-dfs.sh
jps
hdfs dfsadmin -report
cd ~/projects/flight-delay-analysis
git status --short
uv sync --all-groups
```

Do not format the NameNode again.

## Next milestone

Refactor `build_parquet.py` into a configuration-driven Silver pipeline.

The Silver command must:

1. Read the source from the configured HDFS Bronze partition.
2. Confirm 110 source columns and all 28 required fields.
3. Apply the tested types, key rules, and flag rules.
4. Separate accepted and rejected rows without hiding conversion failures.
5. Record quality counts and denominator rules.
6. Write Parquet to HDFS Silver with year and month partitions.
7. Read the output back and verify the written counts.

Do not extend the untracked local-CSV analysis prototype.

## Safety rules

- Do not run `hdfs namenode -format` again.
- Do not change raw source data.
- Do not commit data, credentials, generated output, `.venv`, or Hadoop storage files.
- Keep MongoDB credentials in environment variables.
- Run Ruff and relevant tests after every code change.
- Do not claim that the current one-computer setup is a multi-computer cluster.
