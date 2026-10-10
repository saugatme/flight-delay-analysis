from pyspark.sql import SparkSession

INPUT_PATH = (
    "data/raw/2025-01/"
    "On_Time_Reporting_Carrier_On_Time_Performance_"
    "(1987_present)_2025_1.csv"
)

REQUIRED_COLUMNS = [
    "Year",
    "Month",
    "DayofMonth",
    "DayOfWeek",
    "FlightDate",
    "Reporting_Airline",
    "DOT_ID_Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "OriginAirportID",
    "Origin",
    "OriginCityName",
    "OriginState",
    "DestAirportID",
    "Dest",
    "DestCityName",
    "DestState",
    "CRSDepTime",
    "DepTimeBlk",
    "DepDelay",
    "DepDel15",
    "ArrDelay",
    "ArrDelayMinutes",
    "ArrDel15",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "Flights",
    "Distance",
]

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("inspect-flight-data")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

flights = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(INPUT_PATH)
)

missing_columns = [
    column for column in REQUIRED_COLUMNS
    if column not in flights.columns
]

print(f"Rows: {flights.count():,}")
print(f"Source columns: {len(flights.columns)}")
print(f"Missing required columns: {missing_columns}")

flights.select(
    "FlightDate",
    "Reporting_Airline",
    "Origin",
    "Dest",
    "ArrDelay",
    "Cancelled",
).show(5, truncate=False)

spark.stop()