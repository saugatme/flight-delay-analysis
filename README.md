# US Flight Delay Analysis

A batch data pipeline for exploring delays, cancellations and airport connectivity in US domestic flights. It turns monthly BTS flight records into validated datasets and queryable summaries using HDFS, PySpark and MongoDB, with a Plotly Dash dashboard for exploration.

## Architecture

```text
BTS flight records + OpenFlights airports
                  ↓
           HDFS Bronze sources
                  ↓
      PySpark validation and enrichment
                  ↓
          HDFS Silver Parquet
                  ↓
           HDFS Gold summaries
                  ↓
               MongoDB
                  ↓
           Plotly Dash dashboard
```

- **Bronze** preserves source files and ingestion metadata.
- **Silver** contains typed flight records, rejected records with reasons, and airport enrichment.
- **Gold** contains summaries by carrier, period, airport and route, along with cancellation, connectivity and score tables.
- **MongoDB** serves the prepared summaries without running Spark jobs for dashboard requests.

The pipeline processes one month at a time. Source identifiers and quality records link outputs to their inputs and support repeatable runs.

## Dashboard

Explore flight volume, carrier reliability, cancellation reasons, airport performance and busy routes. Filters cover year, reporting period, season, carrier, airport and route.

The coverage notice identifies the loaded months. The whole-year view combines available months and does not imply that a complete year is present. Cross-year comparisons are most meaningful when the same months are loaded.

## Getting started

The development environment uses Ubuntu on WSL 2, Python 3.12, Java 17, Hadoop HDFS and MongoDB. Python dependencies are managed with uv.

```bash
git clone https://github.com/saugatme/flight-delay-analysis.git
cd flight-delay-analysis
uv sync --all-groups
```

1. Configure the local services using the [setup guide](docs/windows-wsl-setup.md).
2. Download a monthly BTS CSV and run the stages in the [operations guide](docs/operations.md).
3. Start the dashboard:

```bash
uv run flight-delay-dashboard --config config/pipeline.yml --host 127.0.0.1 --port 8050
```

Open [localhost:8050](http://127.0.0.1:8050).

## Metric definitions

| Measure | Definition |
| --- | --- |
| Arrival-delay rate | Flights delayed at least 15 minutes / non-cancelled flights with a valid arrival result |
| Cancellation rate | Cancelled flights / scheduled flights |
| Completion rate | Operated flights / scheduled flights |
| Average arrival delay | Mean delay among non-cancelled flights with valid arrival results |

Rates are displayed as percentages. Missing arrival delays are excluded, rather than replaced with zero. Combined-period rates use total counts, not averages of monthly percentages. Negative arrival delays represent early arrivals.

## Project structure

```text
config/                         Shared pipeline configuration
scripts/                        Local MongoDB service helpers
src/flight_delay_analysis/
  pipelines/                    Pipeline commands
  gold/                         Summary calculations and storage
  serving/                      MongoDB loading and verification
  dashboard/                    Dash application
  tools/                        Data inspection and runtime checks
docs/                           Setup, operations and data sources
```

Downloaded data, generated outputs and credentials are excluded from version control.

## Development

```bash
uv run ruff check .
uv run flight-delay-spark-check
```

## Scope

Spark runs in local mode. HDFS runs in pseudo-distributed mode with one DataNode and a replication factor of one. This configuration supports local development; it does not demonstrate multi-machine performance or fault tolerance. Pipeline stages are started manually.

## Data sources

Flight records are provided by the [Bureau of Transportation Statistics](https://www.transtats.bts.gov/). Airport metadata comes from [OpenFlights](https://openflights.org/data.php). See [data sources](docs/data-sources.md) for formats and attribution.

Contains information from the OpenFlights Airports Database, available under the Open Database License (ODbL).
