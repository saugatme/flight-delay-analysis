# Flight Delay Analysis

This project builds a data pipeline for US domestic flight-delay and flight-network analysis. It follows the approved ID2221 proposal and includes a verified January 2024 versus January 2025 comparison.

The pipeline uses:

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
- Read the manifested Bronze CSV from HDFS with Spark.
- Enforced the 110-column source shape and 28 required fields.
- Split valid typed rows from rejected raw rows with explicit reasons.
- Wrote accepted Parquet, rejected-record Parquet, and JSON quality metrics to HDFS Silver.
- Verified an idempotent Silver rerun, typed Parquet read-back, and healthy HDFS blocks.
- Preserved the unchanged OpenFlights airport source and provenance manifest in HDFS Bronze.
- Parsed all 7,698 reference rows with an explicit 14-field schema.
- Produced 6,071 typed join-ready airports and 1,627 explained rejected rows.
- Wrote checksum-versioned airport reference Parquet and quality metrics to HDFS Silver.
- Left joined airport data to both endpoints without changing the 539,747-flight row count.
- Recorded 539,516 matches and 231 unmatched rows for each endpoint.
- Built checksum-linked carrier, month, season, airport, route, cancellation, connectivity, and score tables in HDFS Gold.
- Loaded every Gold subject table into authenticated MongoDB collections with query indexes.
- Built a MongoDB-backed Plotly Dash application with analytical filters and a route map.
- Confirmed that all 115 automated tests and Ruff checks pass.

Project checklist: 62 of 62 milestones complete.

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
HDFS Gold summary tables -> authenticated MongoDB -> Plotly Dash
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
/flight-delay/silver/flights/year=2025/month=01
/flight-delay/silver/rejected_flights/year=2025/month=01
/flight-delay/silver/quality_metrics/year=2025/month=01
  quality_metrics.json
/flight-delay/bronze/reference/openflights/airports
  airports.dat
  _ingestion_manifest.json
/flight-delay/silver/reference/openflights/airports/source_sha256=<airport-sha256>
/flight-delay/silver/reference/openflights/rejected_airports/source_sha256=<airport-sha256>
/flight-delay/silver/reference/openflights/quality_metrics/source_sha256=<airport-sha256>
/flight-delay/silver/enriched_flights/year=2025/month=01
  flight_source_sha256=<flight-sha256>/airport_source_sha256=<airport-sha256>
/flight-delay/gold/<subject>/year=2025/month=01
  flight_source_sha256=<flight-sha256>/airport_source_sha256=<airport-sha256>
```

The Bronze source is unchanged and has these recorded facts:

- rows: 539,747;
- size: 243,177,378 bytes;
- SHA-256: `d7c7d59452cad1215d9605e8ff350a4bad7282750765084fe57928a5ad275453`;
- ingestion time: `2026-10-10T20:21:41.877634Z`.

The Silver quality record contains:

- input rows: 539,747;
- accepted rows: 539,747;
- rejected rows: 0;
- cancelled rows: 16,312;
- diverted rows: 1,166;
- source SHA-256: the same value as the Bronze manifest;
- processing time: `2026-10-11T14:02:58.140670Z`.

Spark read the stored Parquet back as 539,747 rows and 28 columns, including the `year` and `month` partition columns. `FlightDate` is a date, `ArrDelay` is a double, and `Cancelled` is a Boolean.

The OpenFlights reference result contains 6,071 accepted airports and 1,627 rejected rows. The enriched flight Parquet contains 539,747 rows and 38 stored fields. Origin and destination each matched 539,516 rows. Both endpoints matched on 539,285 rows, and 462 rows had at least one unmatched endpoint.

The January 2025 Gold run contains 14 carrier rows, one month row, one season row, 329 airport rows for each airport subject, 5,649 route rows for each route subject, and four cancellation-code rows. The carrier table reconciles 539,747 scheduled flights, 523,435 operated flights, 522,269 arrival results, 98,130 delayed flights, and 16,312 cancellations.

The January 2024 Gold run contains 15 carrier rows, 334 airport rows for each airport subject, 5,666 route rows for each route subject, and three cancellation-code rows. It reconciles 547,271 scheduled flights, 526,882 operated flights, 525,370 arrival results, 126,410 delayed flights, and 20,389 cancellations. MongoDB contains both years and matches each run's Gold quality counts.

HDFS uses these local service endpoints:

- `hdfs://localhost:9000`: HDFS client and Spark connection.
- `http://localhost:9870`: NameNode web interface.

Port 9000 is not an HTTP web page. A browser request to that port produces a Hadoop IPC warning.

## Pipeline configuration

Runtime values are stored in `config/pipeline.yml`. Python scripts must read this file. They must not contain fixed input or output paths.

The configuration defines:

- processing year and month;
- local landing directory and file pattern;
- HDFS base URI and Bronze, accepted Silver, rejected Silver, quality, and Gold dataset names;
- OpenFlights source, license, local landing path, and reference HDFS datasets;
- enriched-flight and join-quality dataset names;
- expected source row and column counts;
- the 15-minute delay threshold;
- MongoDB host, port, and database name.

Credentials are not stored in this file. MongoDB connection strings come from the ignored `.env` file and process environment.

## Source data

The current development source is the BTS January 2025 On-Time Performance CSV.

Confirmed source result:

- 539,747 records.
- 110 source columns.
- 28 required columns present.
- No required column names missing.

Do not treat cancelled flights as on-time flights. Do not replace a missing arrival delay with zero.

The current reference source is the OpenFlights `airports.dat` file. See [docs/data-sources.md](docs/data-sources.md) for its source, license, attribution, schema, checksum, validation results, and join coverage.

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

January 2024 and January 2025 are the verified like-period partitions. Additional months can use the same command options.

## Repository structure

```text
config/                    Shared pipeline configuration.
data/raw/                  Local landing files. Ignored by Git.
data/processed/            Temporary local Parquet files. Ignored by Git.
docs/                      Plans, operating guide, report, and handover.
notebooks/                 Optional exploration notebooks.
outputs/charts/            Generated charts. Ignored by Git.
outputs/tables/            Generated local tables. Ignored by Git.
scripts/                   Local MongoDB service and user setup.
src/flight_delay_analysis/ Shared pipeline modules.
src/flight_delay_analysis/pipelines/ Executable pipeline and application commands.
src/flight_delay_analysis/tools/ Read-only inspection and validation commands.
src/flight_delay_analysis/gold/ Gold transformations, quality, paths, and storage.
src/flight_delay_analysis/serving/ MongoDB loading and indexes.
src/flight_delay_analysis/dashboard/ Dash application and figures.
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
uv run pytest -q
uv run flight-delay-spark-check
```

Run or safely rerun Bronze ingestion:

```bash
uv run flight-delay-ingest-bronze \
  --config config/pipeline.yml \
  --project-root .
```

The first successful run reports `ingested`. A later run with the same source reports `already_ingested`. A source conflict stops with an error instead of replacing Bronze data.

Run or safely rerun Silver processing:

```bash
uv run flight-delay-build-silver \
  --config config/pipeline.yml
```

The first completed run reports `written`. A later run linked to the same Bronze checksum reports `already_written` without starting a new Spark transformation.

Run or safely rerun the airport reference and endpoint enrichment:

```bash
uv run flight-delay-ingest-reference --config config/pipeline.yml --project-root .
uv run flight-delay-build-airports --config config/pipeline.yml
uv run flight-delay-build-enriched --config config/pipeline.yml
```

Each completed rerun reports an `already_*` status and reuses outputs linked to the same source checksums.

Build or safely rerun all Gold tables:

```bash
uv run flight-delay-build-carrier-gold --config config/pipeline.yml
uv run flight-delay-build-analytics-gold --config config/pipeline.yml
```

Load Gold into MongoDB and start the dashboard:

```bash
set -a
source .env
set +a
./scripts/start_local_mongodb.sh
uv run flight-delay-load-mongodb --config config/pipeline.yml
uv run flight-delay-dashboard --config config/pipeline.yml
```

Open `http://127.0.0.1:8050`. See [docs/operations.md](docs/operations.md) for service startup, period overrides, verification, and shutdown.

## Cross-year result

January 2025 had 7,524 fewer scheduled flights than January 2024. The arrival-delay rate fell from 24.06% to 18.79%, a change of minus 5.27 percentage points. The cancellation rate fell from 3.73% to 3.02%, a change of minus 0.70 points. Average arrival delay fell from 10.35 to 3.76 minutes, a change of minus 6.60 minutes.

These results compare the same month in two years. They do not describe full-year change.

## Documentation

- [Operations guide](docs/operations.md)
- [Historical HDFS learning checkpoint](docs/hdfs-pipeline-checkpoint.tex)
- [Checkpoint checklist](docs/checkpoint-checklist.md)
- [Windows and WSL guide](docs/windows-wsl-setup.md)
- [Project plan](docs/project-plan.md)
- [Process log](docs/process-log.md)

The generated final report is submitted separately from the source repository.
