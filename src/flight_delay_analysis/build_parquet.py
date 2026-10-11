"""Validate the January 2025 BTS CSV and write a typed Parquet data set."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from flight_delay_analysis.contracts import (
    REQUIRED_COLUMNS,
)
from flight_delay_analysis.transforms import cast_columns

INPUT_PATH = Path(
    "data/raw/2025-01/"
    "On_Time_Reporting_Carrier_On_Time_Performance_"
    "(1987_present)_2025_1.csv"
)
OUTPUT_PATH = Path("data/processed/2025-01/flights.parquet")
EXPECTED_ROWS = 539_747
EXPECTED_SOURCE_COLUMNS = 110

def validate_source(
    flights: DataFrame,
    *,
    expected_rows: int = EXPECTED_ROWS,
    expected_source_columns: int = EXPECTED_SOURCE_COLUMNS,
) -> None:
    """Raise ValueError when the source does not match the January 2025 input."""
    missing_columns = sorted(set(REQUIRED_COLUMNS) - set(flights.columns))
    if missing_columns:
        message = ", ".join(missing_columns)
        raise ValueError(f"Missing required columns: {message}")

    source_column_count = len(flights.columns)
    if source_column_count != expected_source_columns:
        raise ValueError(
            f"Expected {expected_source_columns} source columns, found "
            f"{source_column_count}"
        )

    row_count = flights.count()
    if row_count != expected_rows:
        raise ValueError(f"Expected {expected_rows:,} rows, found {row_count:,}")


def validate_typed_data(flights: DataFrame) -> None:
    """Reject missing business keys and invalid cancellation or diversion flags."""
    missing_key_count = flights.filter(
        F.col("FlightDate").isNull()
        | F.col("Reporting_Airline").isNull()
        | (F.trim(F.col("Reporting_Airline")) == "")
        | F.col("Origin").isNull()
        | (F.trim(F.col("Origin")) == "")
        | F.col("Dest").isNull()
        | (F.trim(F.col("Dest")) == "")
    ).count()
    if missing_key_count:
        raise ValueError(f"Found {missing_key_count:,} rows with missing business keys")

    invalid_flag_count = flights.filter(
        F.col("Cancelled").isNull() | F.col("Diverted").isNull()
    ).count()
    if invalid_flag_count:
        raise ValueError(f"Found {invalid_flag_count:,} rows with invalid flags")


def create_spark_session() -> SparkSession:
    """Create the local Spark session used by this project."""
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("build-flight-parquet")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main() -> None:
    """Run the validated CSV-to-Parquet data step."""
    spark = create_spark_session()
    try:
        source_flights = spark.read.option("header", True).option(
            "inferSchema", False
        ).csv(str(INPUT_PATH))
        validate_source(source_flights)

        typed_flights = cast_columns(source_flights)
        validate_typed_data(typed_flights)
        typed_flights.write.mode("overwrite").parquet(str(OUTPUT_PATH))

        output_rows = spark.read.parquet(str(OUTPUT_PATH)).count()
        print(f"Wrote {output_rows:,} rows to {OUTPUT_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
