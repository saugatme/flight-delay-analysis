# Run the pipeline

Use this guide from the WSL terminal. Run commands from the project root unless a step says otherwise.

## 1. Start the services

Start HDFS:

```bash
export HADOOP_HOME=/home/saugat/opt/hadoop-3.5.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HOME/.local/bin:$PATH

start-dfs.sh
jps
hdfs dfsadmin -report
```

Confirm that one DataNode is live. Do not format the NameNode again.

Start MongoDB and load the local environment:

```bash
cd ~/projects/flight-delay-analysis
./scripts/start_local_mongodb.sh
uv sync --all-groups

set -a
source .env
set +a
```

The ignored `.env` file contains MongoDB connection strings. Do not print or commit it.

## 2. Add a BTS month

Create one folder per month under `data/raw`. Each folder must contain exactly one CSV.

For example, download February 2025 from the [BTS archive](https://transtats.bts.gov/PREZIP/):

```bash
year=2025
month=2
month_label=$(printf '%02d' "$month")
target_dir="data/raw/$year-$month_label"
archive="$target_dir/bts_ontime_${year}_${month_label}.zip"

mkdir -p "$target_dir"
curl --fail --location --retry 3 \
  "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${year}_${month}.zip" \
  --output "$archive"
unzip -o "$archive" -d "$target_dir"
find "$target_dir" -maxdepth 1 -type f -name '*.csv' -print
```

Check that the folder contains one CSV. Keep the CSV filename from the archive. The pipeline finds it from the configured `*.csv` pattern.

For March or April, change `month=2` to `month=3` or `month=4`. The archive uses an unpadded month number in its URL. The local folder uses a two-digit month, such as `2025-02`.

## 3. Process one month

Run each stage for the same year and month. Example: February 2025.

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

The Bronze manifest records the source row count when you do not provide `--expected-rows`. A completed HDFS stage can be rerun safely with the same source. MongoDB replaces records for the same month.

## 4. Process February through April 2025

After all three CSV files are in their matching folders, this loop processes one month at a time. It stops if a command fails.

```bash
for month in {2..4}; do
  echo "Processing 2025 month $month"

  uv run flight-delay-ingest-bronze \
    --config config/pipeline.yml --project-root . --year 2025 --month "$month" &&
  uv run flight-delay-build-silver \
    --config config/pipeline.yml --year 2025 --month "$month" &&
  uv run flight-delay-build-enriched \
    --config config/pipeline.yml --year 2025 --month "$month" &&
  uv run flight-delay-build-carrier-gold \
    --config config/pipeline.yml --year 2025 --month "$month" &&
  uv run flight-delay-build-analytics-gold \
    --config config/pipeline.yml --year 2025 --month "$month" &&
  uv run flight-delay-load-mongodb \
    --config config/pipeline.yml --year 2025 --month "$month" || break
done
```

For a different period, change the year or month range. Each month needs a CSV in `data/raw/YYYY-MM/`.

## 5. Open the dashboard

```bash
uv run flight-delay-dashboard \
  --config config/pipeline.yml \
  --host 127.0.0.1 \
  --port 8050
```

Open [http://127.0.0.1:8050](http://127.0.0.1:8050). Select year 2025 and `Whole year / all loaded months`. The coverage notice shows which months are included. January through April should show four of twelve months after all four partitions are loaded.

The whole-year rates are calculated from flight counts. They are not simple averages of monthly rates.

## 6. Check results

Run the code and Spark checks:

```bash
uv run ruff check .
uv run flight-delay-spark-check
```

Check HDFS blocks:

```bash
hdfs fsck /flight-delay/gold -files -blocks
```

Stop the dashboard with `Ctrl+C`. Stop the local services when finished:

```bash
./scripts/stop_local_mongodb.sh
stop-dfs.sh
```
