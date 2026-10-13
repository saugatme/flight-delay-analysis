# Operations guide

This guide runs the pipeline in WSL. Run each command from the project root unless the step says otherwise.

## 1. Start the storage services

Do not format the NameNode again.

```bash
export HADOOP_HOME=/home/saugat/opt/hadoop-3.5.0
export HADOOP_CONF_DIR=/home/saugat/opt/hadoop-3.5.0/etc/hadoop
export PATH=/home/saugat/opt/hadoop-3.5.0/bin:/home/saugat/opt/hadoop-3.5.0/sbin:/home/saugat/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

start-dfs.sh
jps
hdfs dfsadmin -report
```

Expected result: one live DataNode and no missing blocks.

Start MongoDB:

```bash
cd ~/projects/flight-delay-analysis
./scripts/start_local_mongodb.sh
```

The script starts the local authenticated server on `127.0.0.1:27017`.

## 2. Load the project environment

```bash
cd ~/projects/flight-delay-analysis
uv sync --all-groups
set -a
source .env
set +a
```

The ignored `.env` file holds the MongoDB connection strings. Do not print or commit it.

## 3. Run the quality checks

```bash
uv run ruff check .
uv run pytest -q
uv run flight-delay-spark-check
```

The verified checkpoint has 115 passing tests. The Spark check reports `spark-startup` as `true`.

## 4. Run one monthly partition

The default partition comes from `config/pipeline.yml`.

```bash
uv run flight-delay-ingest-bronze \
  --config config/pipeline.yml \
  --project-root .

uv run flight-delay-build-silver \
  --config config/pipeline.yml

uv run flight-delay-build-enriched \
  --config config/pipeline.yml

uv run flight-delay-build-carrier-gold \
  --config config/pipeline.yml

uv run flight-delay-build-analytics-gold \
  --config config/pipeline.yml

uv run flight-delay-load-mongodb \
  --config config/pipeline.yml
```

The steps are safe to rerun with the same source checksums. A completed HDFS step reports an `already_*` status. MongoDB uses deterministic document IDs and replaces the same reporting partition.

## 5. Run another month or year

Place exactly one source CSV in `data/raw/YYYY-MM/`. Then pass the period to every monthly command.

```bash
uv run flight-delay-ingest-bronze \
  --config config/pipeline.yml \
  --project-root . \
  --year 2024 \
  --month 1
```

Use the same `--year 2024 --month 1` options for the Silver, enrichment, Gold, and MongoDB load commands. Add `--expected-rows NUMBER` when an independent source record count is available. If no count is supplied for a non-default period, the Bronze manifest becomes the authoritative monthly count.

The OpenFlights reference is shared by all periods. Build it only when the reference source changes:

```bash
uv run flight-delay-ingest-reference \
  --config config/pipeline.yml \
  --project-root .

uv run flight-delay-build-airports \
  --config config/pipeline.yml
```

## 6. Run the dashboard

```bash
uv run flight-delay-dashboard \
  --config config/pipeline.yml \
  --host 127.0.0.1 \
  --port 8050
```

Open `http://127.0.0.1:8050` in a browser. Stop the dashboard with `Ctrl+C`.

## 7. Verify stored results

```bash
hdfs fsck /flight-delay/gold -files -blocks
```

Expected result: `HEALTHY` with no missing or corrupt blocks.

Verify MongoDB access without printing credentials:

```bash
uv run python -c 'import os; from pymongo import MongoClient; client = MongoClient(os.environ["MONGODB_URI"]); print(client.admin.command("ping")); client.close()'
```

Expected result: `{'ok': 1.0}`.

## 8. Stop local services

```bash
./scripts/stop_local_mongodb.sh
stop-dfs.sh
```

Stop the services only after Spark and Dash have stopped.
