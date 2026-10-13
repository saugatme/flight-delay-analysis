"""Check that the local Spark runtime starts and executes a small job."""

from pyspark.sql import SparkSession


def main() -> None:
    """Start local Spark, show one result row, and stop the session."""
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("flight-delay-analysis")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("ERROR")
        check_df = spark.createDataFrame(
            [("spark-startup", True)],
            ["check_name", "passed"],
        )
        check_df.show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
