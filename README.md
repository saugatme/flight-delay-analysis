# US Flight Delay Analysis

This project studies delays, cancellations, and airport connections in U.S. domestic flights. It was built for the ID2221 assignment.

The pipeline uses BTS monthly flight data, PySpark, HDFS, MongoDB, and Plotly Dash. Development runs in WSL 2 with local Spark. HDFS is configured in pseudo-distributed mode on one computer.

## What the project does

- Checks and stores each source file in HDFS Bronze.
- Validates flight rows and writes accepted and rejected records to HDFS Silver.
- Adds airport names and coordinates from the OpenFlights reference.
- Builds carrier, month, season, airport, route, cancellation, connectivity, and score tables in HDFS Gold.
- Loads Gold tables into MongoDB.
- Displays the results in a Plotly Dash dashboard.

## Data flow

```text
BTS monthly CSV
  -> HDFS Bronze
  -> validated and enriched Parquet in HDFS Silver
  -> monthly Gold tables in HDFS
  -> MongoDB
  -> Plotly Dash
```

The pipeline processes one year-month partition at a time. Raw data and generated results are excluded from Git.

## Measures and definitions

The analysis includes carrier and route performance, monthly and seasonal trends, airport and route traffic, cancellations, airport connectivity, and route and airport scores.

The project uses these definitions:

- A delayed arrival is an arrival delay of at least 15 minutes.
- Arrival delay rate = delayed flights / flights with a valid arrival result.
- Cancellation rate = cancelled flights / scheduled flights.
- Average arrival delay uses non-cancelled flights with a valid arrival delay.
- Missing arrival delay is not treated as zero. Cancelled flights are not treated as on time.

## Verified comparison

The verified results compare January 2024 with January 2025. In January 2025, 18.79% of valid arrivals were delayed and 3.02% of scheduled flights were cancelled. In January 2024, the corresponding rates were 24.06% and 3.73%.

These are January-to-January results. They do not describe full-year performance. The dashboard reports how many months are loaded for each year.

## Requirements

The project is designed for:

- WSL 2 with Ubuntu
- Python 3.12 managed by uv
- Java 17
- Apache Hadoop 3.5.0
- PySpark 4.2.0 in local mode
- MongoDB

Keep the repository in the WSL Linux filesystem, for example `~/projects/flight-delay-analysis`. Do not run it from OneDrive.

## Start here

See [docs/operations.md](docs/operations.md) for commands to start the services, download a BTS month, run the pipeline, and open the dashboard.

The OpenFlights airport source and its license are described in [docs/data-sources.md](docs/data-sources.md). Environment setup is described in [docs/windows-wsl-setup.md](docs/windows-wsl-setup.md).

## Repository layout

```text
config/                              Pipeline settings
data/raw/                            Downloaded BTS source files (not in Git)
data/processed/                      Local development data (not in Git)
docs/                                Operations and data-source guides
scripts/                             Local MongoDB helper scripts
src/flight_delay_analysis/
  pipelines/                         Commands for each pipeline stage
  gold/                              Gold calculations and HDFS storage
  serving/                            MongoDB access and loading
  dashboard/                          Dash application
  tools/                               Inspection and Spark checks
  utilities.py                         Shared Spark and command-line helpers
```

## Current data checkpoint

January 2025 is the verified BTS source partition. It contains 539,747 records and 110 source columns. January 2024 is also loaded for the like-month comparison. Add more months using the steps in [the operations guide](docs/operations.md).
