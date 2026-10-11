# Handover

## Current status

The project now follows the approved HDFS, Spark, MongoDB, and Plotly Dash architecture.

Progress at the end of this session:

- Overall project: 43/62 checklist items complete (69%).
- Bronze ingestion: 8/8 complete.
- Flight Silver processing: 7/7 complete.
- Airport reference and endpoint enrichment: 7/7 complete.
- Ruff passes and all 95 automated tests pass.

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
- Ruff and all 95 automated tests pass.

Completed Silver processing work:

- Spark reads the exact Bronze source URI recorded in the ingestion manifest.
- The source contract requires 110 source columns and all 28 required fields.
- Row classification keeps valid rows as typed data and invalid rows as raw strings with explicit rejection reasons.
- Empty optional delay fields are allowed. Non-empty invalid numeric values are rejected.
- The January run accepted all 539,747 rows and rejected zero rows.
- The quality record reports 16,312 cancelled rows and 1,166 diverted rows.
- Accepted Parquet, rejected-record Parquet, and JSON quality metrics are stored in separate HDFS datasets.
- Spark read the accepted Parquet back as 539,747 rows and 28 columns, including `year` and `month` partition columns.
- The Silver rerun reports `already_written` for the same Bronze checksum.
- `hdfs fsck /flight-delay/silver -files -blocks` reports a healthy filesystem.

Completed airport reference work:

- `config/pipeline.yml` records the OpenFlights source, license, landing path, and HDFS datasets.
- The unchanged 7,698-row `airports.dat` file and provenance manifest are stored in HDFS reference Bronze.
- The source has 14 fields, 1,127,225 bytes, and SHA-256 `9387cdb38df5bd664da823f8ccb69fdd9b33a1888f5b7cca09c34a3cd9ff59f9`.
- Spark parses the headerless file with an explicit 14-field string schema and converts `\\N` to null.
- Validation produced 6,071 typed join-ready airports and 1,627 rejected rows with reasons.
- Rejections contain 1,626 missing IATA codes and one invalid IATA code. Coordinates had no failures.
- Accepted IATA codes are unique.
- Accepted Parquet, rejected Parquet, and quality JSON are versioned by the source checksum in HDFS Silver.
- Airport reference reruns report `already_written` for the same source.

Completed endpoint enrichment work:

- Spark left joins the small airport reference to both `Origin` and `Dest` with broadcast joins.
- The enriched output preserves all 539,747 input flight rows and stores 38 Parquet fields.
- Origin and destination each match 539,516 rows and miss 231 rows.
- Both endpoints match on 539,285 rows; 462 rows have at least one unmatched endpoint.
- The enriched Parquet readback confirms Boolean match flags and double coordinate fields.
- Flight and airport source checksums version the enriched output and join-quality JSON.
- A safe rerun reports `already_written`.

## Important limits

- HDFS is pseudo-distributed on one computer. It is not a multi-computer cluster.
- MongoDB is not installed.
- No Gold result table, MongoDB collection, or dashboard is complete.
- A local untracked prototype named `analyze_carrier_delays.py` reads local CSV and does not fit the target pipeline. It is not part of the committed project. Replace it later instead of extending it.
- The built-in LaTeX compiler returned an unknown status after three attempts. The checkpoint source is updated, but compilation was not confirmed in this session.

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

Build the first Gold result: carrier delay and cancellation summary.

The Gold step must:

1. Read the checksum-linked enriched Silver partition.
2. Define denominators for operated flights, arrival-delay rates, and cancellation rates.
3. Exclude cancelled flights and missing arrival delays from arrival-delay averages.
4. Produce carrier flight counts, delayed-flight counts, average arrival delay, cancellation counts, and rates.
5. Add denominator, range, and row-count tests.
6. Write a checksum-linked Gold result and quality record before MongoDB loading.

Do not extend the untracked local-CSV analysis prototype. Build Gold from HDFS Silver.

## Working method

- Codex writes the project code and explains the design before and after each small section.
- The user runs all WSL, HDFS, Spark, and application commands manually and pastes the output.
- Do not add quiz or comprehension questions.
- Use short ASD-STE100-style instructions.
- Record progress as completed/total when a milestone changes.
- Do not mark an HDFS milestone complete until the user provides real command output.

## First action in the next session

Read `AGENTS.md` and this file completely. Inspect the current branch and recent commits. Do not repeat environment, Bronze, Silver, reference, or enrichment setup.

Start the carrier Gold milestone by defining its output grain and denominators before implementation:

- one row per reporting carrier for the configured month;
- scheduled flights as the cancellation-rate denominator;
- operated flights with a valid arrival result as the arrival-delay denominator;
- delayed flights defined by the configured 15-minute threshold;
- cancelled flights excluded from average arrival delay.

Implement one tested Gold section at a time. Use the checksum-linked enriched Silver path as input. Do not read the local CSV.

## Safety rules

- Do not run `hdfs namenode -format` again.
- Do not change raw source data.
- Do not commit data, credentials, generated output, `.venv`, or Hadoop storage files.
- Keep MongoDB credentials in environment variables.
- Run Ruff and relevant tests after every code change.
- Do not claim that the current one-computer setup is a multi-computer cluster.
