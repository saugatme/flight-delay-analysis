# Operations

Run commands from the repository root in the configured Ubuntu environment. Initial HDFS and MongoDB configuration is covered in the [setup guide](windows-wsl-setup.md).

## Start services

With Hadoop available on `PATH`:

```bash
start-dfs.sh
./scripts/start_local_mongodb.sh
uv sync --all-groups
set -a
source .env
set +a
```

## Prepare airport reference data

Run these stages before processing flights for the first time. The reference is shared across reporting months.

```bash
uv run flight-delay-ingest-reference --config config/pipeline.yml --project-root .
uv run flight-delay-build-airports --config config/pipeline.yml
```

## Add a reporting month

Download the desired On-Time Performance archive from [BTS](https://transtats.bts.gov/PREZIP/) and extract its CSV into `data/raw/YYYY-MM/`. Each monthly directory must contain exactly one CSV matching the configured pattern. Retain the original source file.

The following example processes February 2025. Use the same year and month for every stage:

```bash
uv run flight-delay-ingest-bronze \
  --config config/pipeline.yml --project-root . --year 2025 --month 2

uv run flight-delay-build-silver \
  --config config/pipeline.yml --year 2025 --month 2

uv run flight-delay-build-enriched \
  --config config/pipeline.yml --year 2025 --month 2

uv run flight-delay-build-carrier-gold \
  --config config/pipeline.yml --year 2025 --month 2

uv run flight-delay-build-analytics-gold \
  --config config/pipeline.yml --year 2025 --month 2

uv run flight-delay-load-mongodb \
  --config config/pipeline.yml --year 2025 --month 2
```

Proceed to the next stage after the preceding command succeeds. Shared settings are in `config/pipeline.yml`; year and month options override the default reporting period. An optional `--expected-rows` value checks a source against an independently known record count.

## Reruns

Bronze recognises an already ingested source and rejects a different file in the same partition. Silver and Gold check source identity and completion records before reusing output. Gold records both flight and airport source identifiers. MongoDB updates matching summaries and removes stale entries within the load's reporting scope.

If output is incomplete or conflicts with its metadata, inspect the reported error and affected partition before attempting a rebuild.

## Open the dashboard

```bash
uv run flight-delay-dashboard \
  --config config/pipeline.yml --host 127.0.0.1 --port 8050
```

Open [localhost:8050](http://127.0.0.1:8050). The coverage notice reports available months. Whole-year measures use all loaded months and combine their counts; they are not simple averages of monthly rates. Compare equivalent coverage when interpreting changes between years.

## Check the environment and storage

```bash
uv run ruff check .
uv run flight-delay-spark-check
hdfs dfsadmin -report
hdfs fsck /flight-delay/gold -files -blocks
```

The MongoDB loader checks stored counts against Gold quality records as part of each load.

## Stop services

Stop the dashboard with `Ctrl+C`, then stop the local services:

```bash
./scripts/stop_local_mongodb.sh
stop-dfs.sh
```
