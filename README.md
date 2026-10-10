# Flight Delay Analysis

This project builds a data pipeline for US domestic flight-delay and flight-network analysis. It follows the approved ID2221 proposal.

The final pipeline will use:

- HDFS for raw and cleaned analytical data.
- PySpark and Spark SQL for validation, transformation, joins, and aggregation.
- MongoDB for Gold result tables.
- Plotly Dash for interactive charts and route maps.

Development runs on Windows through WSL 2 and Ubuntu. HDFS currently runs in pseudo-distributed mode on one computer. This is not a multi-computer cluster.

## Current checkpoint

Completed:

- Created the Python project with uv and Python 3.12.
- Confirmed Java 17 and PySpark 4.2.0.
- Downloaded the BTS January 2025 On-Time Performance CSV.
- Confirmed 539,747 records and 110 source columns.
- Confirmed that all 28 required fields are present.
- Created 539,747 validated local Parquet rows with 28 typed columns.
- Added five tests for the existing CSV-to-Parquet step.
- Installed Apache Hadoop 3.5.0 inside WSL.
- Configured and started one NameNode, one DataNode, and one SecondaryNameNode.
- Confirmed one live DataNode and zero missing or corrupt blocks.
- Created the Bronze, Silver, and Gold directory structure in HDFS.
- Added `config/pipeline.yml` and PyYAML.
- Added a typed and validated YAML configuration loader.
- Added configuration-driven Bronze ingestion with no fixed source filename.
- Stored the unchanged January CSV in HDFS Bronze.
- Stored a JSON ingestion manifest with the file identity and ingestion evidence.
- Added safe-rerun and conflicting-source checks.
- Confirmed that all 34 automated tests and Ruff checks pass.

Not complete:

- The validated Parquet data is not in HDFS Silver yet.
- MongoDB is not installed or connected.
- Gold summary tables are not implemented.
- The OpenFlights reference-data join is not implemented.
- Plotly Dash is not implemented.
- Multi-year analysis is not implemented.

See [docs/checkpoint-checklist.md](docs/checkpoint-checklist.md) for the detailed checklist.

## Target data flow

```text
BTS monthly CSV files
        |
        v
HDFS Bronze: unchanged source data, partitioned by year and month
        |
        v
PySpark validation, type conversion, cleaning, and reference-data joins
        |
        v
HDFS Silver: validated Parquet flight records
        |
        v
Spark SQL and DataFrame aggregations
        |
        v
Gold summary tables -> MongoDB -> Plotly Dash
```

The local CSV and local Parquet files are temporary development inputs. The completed pipeline will use HDFS as required by the proposal.

## Data layers

- **Landing:** downloaded files before HDFS ingestion.
- **Bronze:** unchanged source files in HDFS. Partitions identify the source year and month.
- **Silver:** typed and validated Parquet records in HDFS.
- **Gold:** carrier, month, airport, route, connectivity, and cancellation summaries.
- **Serving:** MongoDB collections used by Plotly Dash.

## Current HDFS layout

```text
/flight-delay/bronze/bts/year=2025/month=01
  On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2025_1.csv
  _ingestion_manifest.json
/flight-delay/silver/flights
/flight-delay/gold
```

The Bronze source is unchanged and has these recorded facts:

- rows: 539,747;
- size: 243,177,378 bytes;
- SHA-256: `d7c7d59452cad1215d9605e8ff350a4bad7282750765084fe57928a5ad275453`;
- ingestion time: `2026-10-10T20:21:41.877634Z`.

HDFS uses these local service endpoints:

- `hdfs://localhost:9000`: HDFS client and Spark connection.
- `http://localhost:9870`: NameNode web interface.

Port 9000 is not an HTTP web page. A browser request to that port produces a Hadoop IPC warning.

## Pipeline configuration

Runtime values are stored in `config/pipeline.yml`. Python scripts must read this file. They must not contain fixed input or output paths.

The configuration defines:

- processing year and month;
- local landing directory and file pattern;
- HDFS base URI and layer names;
- expected source row and column counts;
- the 15-minute delay threshold;
- MongoDB host, port, and database name.

Credentials must not be stored in this file. Future MongoDB credentials must come from environment variables.

## Source data

The current development source is the BTS January 2025 On-Time Performance CSV.

Confirmed source result:

- 539,747 records.
- 110 source columns.
- 28 required columns present.
- No required column names missing.

Do not treat cancelled flights as on-time flights. Do not replace a missing arrival delay with zero.

## Required analysis

The proposal requires these results:

- Delay measures by carrier, route, airport, season, and time of day.
- On-time performance and significant-delay frequency.
- Cancellation rates.
- Changes across years.
- Airport connectivity, including departures, arrivals, and unique destinations.
- A composite route and airport score.
- An airport-coordinate join for maps.
- A Plotly Dash dashboard backed by MongoDB.

The January 2025 file is the first development partition. More years will be added after one end-to-end partition works.

## Repository structure

```text
config/                    Shared pipeline configuration.
data/raw/                  Local landing files. Ignored by Git.
data/processed/            Temporary local Parquet files. Ignored by Git.
docs/                      Plans, learning notes, and setup guides.
notebooks/                 Optional exploration notebooks.
outputs/charts/            Generated charts. Ignored by Git.
outputs/tables/            Generated local tables. Ignored by Git.
src/flight_delay_analysis/ Pipeline source code.
tests/                     Automated tests.
```

## WSL development environment

Confirmed versions at this checkpoint:

- Ubuntu 24.04.4 LTS in WSL 2.
- uv 0.12.23.
- Python 3.12.15.
- Java 17.0.20.1.
- PySpark 4.2.0.
- Hadoop 3.5.0.

Keep the repository in the WSL Linux filesystem:

```text
/home/saugat/projects/flight-delay-analysis
```

Do not run this repository from OneDrive. See [docs/windows-wsl-setup.md](docs/windows-wsl-setup.md) for the Windows setup and operating guide.

## Start a work session

Start HDFS if it is not running:

```bash
start-dfs.sh
jps
hdfs dfsadmin -report
```

Open the project and synchronize Python dependencies:

```bash
cd ~/projects/flight-delay-analysis
uv sync --all-groups
```

Run the existing checks:

```bash
uv run ruff check .
uv run pytest
uv run python src/flight_delay_analysis/spark_check.py
uv run python src/flight_delay_analysis/inspect_raw.py
uv run python src/flight_delay_analysis/build_parquet.py
```

Run or safely rerun Bronze ingestion:

```bash
uv run python -m flight_delay_analysis.ingest_bronze \
  --config config/pipeline.yml \
  --project-root .
```

The first successful run reports `ingested`. A later run with the same source reports `already_ingested`. A source conflict stops with an error instead of replacing Bronze data.

## Next milestone

Refactor the Silver pipeline so Spark reads the configured HDFS Bronze CSV instead of the local landing file. The Silver step must:

1. Read the Bronze file from HDFS.
2. Confirm the 110-column source shape and all 28 required fields.
3. Apply explicit data types and documented null rules.
4. separate accepted and rejected records;
5. record input, accepted, rejected, cancelled, and diverted counts;
6. write partitioned Parquet to HDFS Silver;
7. verify counts after the write.

Do not implement Gold analysis before Bronze and Silver work from HDFS.

## Documentation

- [Learning checkpoint](docs/hdfs-pipeline-checkpoint.tex)
- [Checkpoint checklist](docs/checkpoint-checklist.md)
- [Windows and WSL guide](docs/windows-wsl-setup.md)
- [Project plan](docs/project-plan.md)
- [Process log](docs/process-log.md)
- [Session handover](HANDOVER.md)
