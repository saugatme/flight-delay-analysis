import pytest
from pyspark.sql import SparkSession

from flight_delay_analysis.build_parquet import (
    REQUIRED_COLUMNS,
    cast_columns,
    validate_source,
    validate_typed_data,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("parquet-tests").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def source_row() -> dict[str, str]:
    return {
        "Year": "2025",
        "Month": "1",
        "DayofMonth": "1",
        "DayOfWeek": "3",
        "FlightDate": "2025-01-01",
        "Reporting_Airline": "AA",
        "DOT_ID_Reporting_Airline": "19805",
        "Flight_Number_Reporting_Airline": "1",
        "OriginAirportID": "12478",
        "Origin": "JFK",
        "OriginCityName": "New York, NY",
        "OriginState": "NY",
        "DestAirportID": "12892",
        "Dest": "LAX",
        "DestCityName": "Los Angeles, CA",
        "DestState": "CA",
        "CRSDepTime": "0900",
        "DepTimeBlk": "0900-0959",
        "DepDelay": "2.0",
        "DepDel15": "0.0",
        "ArrDelay": "5.0",
        "ArrDelayMinutes": "5.0",
        "ArrDel15": "0.0",
        "Cancelled": "0.0",
        "CancellationCode": "",
        "Diverted": "0.0",
        "Flights": "1.0",
        "Distance": "2475.0",
    }


def test_cast_columns_sets_expected_types(spark: SparkSession) -> None:
    flights = spark.createDataFrame([source_row()])

    typed_flights = cast_columns(flights)
    row = typed_flights.first()

    assert typed_flights.schema["FlightDate"].dataType.simpleString() == "date"
    assert typed_flights.schema["ArrDelay"].dataType.simpleString() == "double"
    assert typed_flights.schema["Cancelled"].dataType.simpleString() == "boolean"
    assert row["Cancelled"] is False
    assert row["Diverted"] is False


def test_validate_source_rejects_missing_required_column(spark: SparkSession) -> None:
    source = source_row()
    source.pop("Dest")
    flights = spark.createDataFrame([source])

    with pytest.raises(ValueError, match="Dest"):
        validate_source(
            flights,
            expected_rows=1,
            expected_source_columns=len(flights.columns),
        )


def test_validate_typed_data_rejects_missing_key(spark: SparkSession) -> None:
    source = source_row()
    source["Origin"] = ""
    flights = cast_columns(spark.createDataFrame([source]))

    with pytest.raises(ValueError, match="missing business keys"):
        validate_typed_data(flights)


def test_validate_typed_data_rejects_invalid_flag(spark: SparkSession) -> None:
    source = source_row()
    source["Cancelled"] = "2.0"
    flights = cast_columns(spark.createDataFrame([source]))

    with pytest.raises(ValueError, match="invalid flags"):
        validate_typed_data(flights)


def test_required_columns_are_unique() -> None:
    assert len(REQUIRED_COLUMNS) == 28
    assert len(set(REQUIRED_COLUMNS)) == len(REQUIRED_COLUMNS)
