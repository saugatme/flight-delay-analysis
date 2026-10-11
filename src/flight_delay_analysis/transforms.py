"""Reusable Spark transformations for detailed flight records."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flight_delay_analysis.contracts import (
    DOUBLE_COLUMNS,
    FLAG_COLUMNS,
    INTEGER_COLUMNS,
    REQUIRED_COLUMNS,
)


def cast_columns(flights: DataFrame) -> DataFrame:
    """Select required fields and apply stable types without ANSI cast failures."""
    typed_flights = flights.select(*REQUIRED_COLUMNS).withColumn(
        "FlightDate", F.expr("try_cast(`FlightDate` AS DATE)")
    )

    for column in INTEGER_COLUMNS:
        typed_flights = typed_flights.withColumn(
            column, F.expr(f"try_cast(`{column}` AS INT)")
        )
    for column in DOUBLE_COLUMNS:
        typed_flights = typed_flights.withColumn(
            column, F.expr(f"try_cast(`{column}` AS DOUBLE)")
        )
    for column in FLAG_COLUMNS:
        numeric_flag = F.expr(f"try_cast(`{column}` AS DOUBLE)")
        typed_flights = typed_flights.withColumn(
            column,
            F.when(numeric_flag.isin(0.0, 1.0), numeric_flag == F.lit(1.0)).otherwise(
                F.lit(None).cast("boolean")
            ),
        )

    return typed_flights
