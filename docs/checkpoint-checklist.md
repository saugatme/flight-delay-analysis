# Flight Delay Project Checkpoint

Checkpoint date: 2026-10-10

## Completed

### Project environment

- [x] WSL 2 and Ubuntu 24.04.4 LTS selected.
- [x] Repository stored in the WSL Linux filesystem.
- [x] uv and Python 3.12 configured.
- [x] Java 17 confirmed.
- [x] PySpark 4.2.0 confirmed.
- [x] Ruff and pytest added.

### Source data

- [x] BTS January 2025 archive downloaded.
- [x] Source CSV extracted under `data/raw/2025-01/`.
- [x] 539,747 records confirmed.
- [x] 110 source columns confirmed.
- [x] All 28 required fields confirmed.
- [x] Raw and generated data excluded from Git.

### Initial validation work

- [x] Local CSV-to-Parquet prototype created.
- [x] Date, numeric, and Boolean conversions added.
- [x] Business-key and flag checks added.
- [x] 539,747 typed rows written and read back.
- [x] Five tests added for the local prototype.

### HDFS foundation

- [x] Hadoop 3.5.0 installed natively in WSL.
- [x] Java and Hadoop environment variables configured.
- [x] OpenSSH server installed and started.
- [x] Password-free localhost SSH configured.
- [x] NameNode formatted once.
- [x] NameNode, DataNode, and SecondaryNameNode started.
- [x] One live DataNode confirmed.
- [x] Zero missing and corrupt blocks confirmed.
- [x] Bronze, Silver, and Gold HDFS directories created.

### Shared configuration

- [x] `config/pipeline.yml` created.
- [x] Processing year and month configured.
- [x] Source file pattern configured.
- [x] HDFS base URI and layer names configured.
- [x] Data-quality expectations configured.
- [x] MongoDB endpoint configured.
- [x] PyYAML added and YAML parsing confirmed.

## Next

- [ ] Add a tested configuration loader.
- [ ] Add idempotent Bronze ingestion.
- [ ] Write a Bronze ingestion manifest.
- [ ] Copy the January CSV into HDFS Bronze through the pipeline.
- [ ] Refactor the validation step to read HDFS Bronze.
- [ ] Write validated Parquet into partitioned HDFS Silver.
- [ ] Add rejected-record and quality-metric outputs.

## Remaining project work

### Reference data

- [ ] Add the OpenFlights airport reference data.
- [ ] Validate coordinates and airport codes.
- [ ] Join coordinates to flight records.

### Gold analysis

- [ ] Carrier delay and cancellation summary.
- [ ] Month and seasonal summary.
- [ ] Airport departure and arrival summary.
- [ ] Route summary.
- [ ] Cancellation-code summary.
- [ ] Airport connectivity summary.
- [ ] Composite airport and route score.
- [ ] Rate and denominator tests.

### MongoDB

- [ ] Install MongoDB in WSL.
- [ ] Configure local authentication and secrets.
- [ ] Load Gold results into collections.
- [ ] Add query indexes.
- [ ] Verify record counts after each load.

### Dashboard and scale

- [ ] Build the Plotly Dash application.
- [ ] Add map-based views.
- [ ] Add carrier, airport, route, season, and year filters.
- [ ] Add multiple years of BTS data.
- [ ] Run cross-year comparisons.
- [ ] Complete the report and presentation.

## Current stop point

HDFS and YAML configuration work are complete. The next code task is Bronze ingestion. Do not start Gold analysis yet.
