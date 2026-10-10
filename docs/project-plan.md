# Project plan

## Goal

Build the distributed flight-delay pipeline described in the approved proposal. Use HDFS, PySpark, Spark SQL, MongoDB, OpenFlights reference data, and Plotly Dash.

Development runs in WSL 2 on one computer. HDFS is pseudo-distributed at this checkpoint. State this limit clearly in the report.

## Confirmed input

| Item | Confirmed value |
| --- | --- |
| Source | BTS January 2025 On-Time Performance CSV |
| Flight records | 539,747 |
| Source columns | 110 |
| Required columns | 28 of 28 present |
| Initial partition | Year 2025, month 01 |

January 2025 is the first development partition. The final project must add more years because the proposal includes seasonal and cross-year comparison.

## Target architecture

```text
Local landing CSV
  -> HDFS Bronze CSV by year and month
  -> PySpark validation and cleaning
  -> HDFS Silver Parquet by year and month
  -> Spark SQL Gold summaries
  -> MongoDB serving collections
  -> Plotly Dash dashboard
```

## Stage 1 - Environment and source check

- [x] Create the WSL repository and uv environment.
- [x] Confirm Python, Java, and PySpark.
- [x] Download one BTS monthly file.
- [x] Confirm row count, source column count, and required fields.
- [x] Build and test the first typed local Parquet data set.

## Stage 2 - HDFS foundation

- [x] Install native Linux Hadoop in WSL.
- [x] Configure pseudo-distributed HDFS.
- [x] Configure localhost SSH.
- [x] Format the new NameNode once.
- [x] Start and verify HDFS daemons.
- [x] Create Bronze, Silver, and Gold HDFS directories.
- [x] Add shared YAML configuration.
- [ ] Add a configuration loader with validation.
- [ ] Add idempotent Bronze ingestion and an ingestion manifest.
- [ ] Add Bronze ingestion tests.

## Stage 3 - Silver data

- [ ] Read Bronze CSV from HDFS with Spark.
- [ ] Enforce the 28-field data contract.
- [ ] Apply explicit types and null rules.
- [ ] Record input, accepted, rejected, cancelled, and diverted counts.
- [ ] Write partitioned Silver Parquet to HDFS.
- [ ] Test conversion failures and rejected records.

## Stage 4 - Reference data

- [ ] Download and document OpenFlights airport data.
- [ ] Validate airport codes and coordinates.
- [ ] Store the reference data in HDFS.
- [ ] Join coordinates to flight airports and routes.
- [ ] Record unmatched airport codes.

## Stage 5 - Gold analysis

- [ ] Carrier performance table.
- [ ] Month and seasonal trend table.
- [ ] Origin and destination airport tables.
- [ ] Origin-to-destination route table.
- [ ] Cancellation table and cancellation-code table.
- [ ] Airport connectivity table.
- [ ] Composite airport and route score.
- [ ] Tests for all counts, denominators, and rates.

For delay rates, divide delayed flights by applicable flights. Exclude cancelled and diverted flights when no valid arrival result exists. For cancellation rates, divide cancelled flights by scheduled flights. Store each denominator in the result table.

## Stage 6 - MongoDB serving layer

- [ ] Install and secure MongoDB for local development.
- [ ] Add the MongoDB Spark connector or a controlled bulk-load step.
- [ ] Create one collection for each Gold subject area.
- [ ] Create indexes for carrier, airport, route, year, and month queries.
- [ ] Test record counts between Gold output and MongoDB.
- [ ] Keep credentials outside Git.

## Stage 7 - Dashboard and delivery

- [ ] Build the Plotly Dash dashboard.
- [ ] Add carrier, airport, route, season, and year filters.
- [ ] Add an interactive airport and route map.
- [ ] Add pipeline run instructions and architecture diagrams.
- [ ] Add several years of BTS data.
- [ ] Run the full quality and reproducibility checks.
- [ ] Prepare the report and presentation.

## Current milestone

The HDFS foundation and shared YAML file are complete. The next milestone is configuration-driven Bronze ingestion. No production data is in HDFS yet.
