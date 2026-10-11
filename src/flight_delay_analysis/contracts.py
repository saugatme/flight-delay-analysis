"""Shared source and Silver column contracts."""

REQUIRED_COLUMNS = (
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
)

INTEGER_COLUMNS = (
    "Year",
    "Month",
    "DayofMonth",
    "DayOfWeek",
    "DOT_ID_Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "OriginAirportID",
    "DestAirportID",
)

DOUBLE_COLUMNS = (
    "DepDelay",
    "DepDel15",
    "ArrDelay",
    "ArrDelayMinutes",
    "ArrDel15",
    "Flights",
    "Distance",
)

FLAG_COLUMNS = ("Cancelled", "Diverted")
KEY_COLUMNS = ("FlightDate", "Reporting_Airline", "Origin", "Dest")
