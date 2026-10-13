"""Airport performance and connectivity Gold aggregations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flight_delay_analysis.gold.common import require_columns, safe_rate


def build_airport_summary(
    flights: DataFrame,
    report_year: int,
    report_month: int,
    delay_threshold_minutes: int,
) -> DataFrame:
    """Build monthly departure and arrival measures for each airport."""
    required = {
        "Origin",
        "Dest",
        "OriginAirportName",
        "DestAirportName",
        "OriginLatitude",
        "OriginLongitude",
        "DestLatitude",
        "DestLongitude",
        "Cancelled",
        "DepDelay",
        "ArrDelay",
    }
    require_columns(flights, required, "Enriched Silver")
    operated = ~F.col("Cancelled")
    has_departure = operated & F.col("DepDelay").isNotNull()
    has_arrival = operated & F.col("ArrDelay").isNotNull()

    departures = flights.groupBy(
        F.col("Origin").alias("airport"),
        F.col("OriginAirportName").alias("departure_airport_name"),
        F.col("OriginLatitude").alias("departure_latitude"),
        F.col("OriginLongitude").alias("departure_longitude"),
    ).agg(
        F.count("*").alias("scheduled_departures"),
        F.sum(operated.cast("long")).alias("operated_departures"),
        F.sum(has_departure.cast("long")).alias("departure_result_flights"),
        F.sum(
            (
                has_departure
                & (F.col("DepDelay") >= delay_threshold_minutes)
            ).cast("long")
        ).alias("delayed_departures"),
        F.sum(F.col("Cancelled").cast("long")).alias("cancelled_departures"),
        F.avg(F.when(has_departure, F.col("DepDelay"))).alias(
            "average_departure_delay_minutes"
        ),
    )
    arrivals = flights.groupBy(
        F.col("Dest").alias("airport"),
        F.col("DestAirportName").alias("arrival_airport_name"),
        F.col("DestLatitude").alias("arrival_latitude"),
        F.col("DestLongitude").alias("arrival_longitude"),
    ).agg(
        F.count("*").alias("scheduled_arrivals"),
        F.sum(operated.cast("long")).alias("operated_arrivals"),
        F.sum(has_arrival.cast("long")).alias("arrival_result_flights"),
        F.sum(
            (
                has_arrival
                & (F.col("ArrDelay") >= delay_threshold_minutes)
            ).cast("long")
        ).alias("delayed_arrivals"),
        F.sum(F.col("Cancelled").cast("long")).alias("cancelled_arrivals"),
        F.avg(F.when(has_arrival, F.col("ArrDelay"))).alias(
            "average_arrival_delay_minutes"
        ),
    )
    joined = departures.join(arrivals, "airport", "full")
    count_columns = [
        "scheduled_departures",
        "operated_departures",
        "departure_result_flights",
        "delayed_departures",
        "cancelled_departures",
        "scheduled_arrivals",
        "operated_arrivals",
        "arrival_result_flights",
        "delayed_arrivals",
        "cancelled_arrivals",
    ]
    for column in count_columns:
        joined = joined.withColumn(column, F.coalesce(F.col(column), F.lit(0)))

    return joined.select(
        F.lit(report_year).cast("int").alias("report_year"),
        F.lit(report_month).cast("int").alias("report_month"),
        "airport",
        F.coalesce("departure_airport_name", "arrival_airport_name").alias(
            "airport_name"
        ),
        F.coalesce("departure_latitude", "arrival_latitude").alias("latitude"),
        F.coalesce("departure_longitude", "arrival_longitude").alias("longitude"),
        *count_columns,
        "average_departure_delay_minutes",
        "average_arrival_delay_minutes",
        safe_rate("delayed_departures", "departure_result_flights").alias(
            "departure_delay_rate"
        ),
        safe_rate("delayed_arrivals", "arrival_result_flights").alias(
            "arrival_delay_rate"
        ),
        safe_rate("cancelled_departures", "scheduled_departures").alias(
            "departure_cancellation_rate"
        ),
        safe_rate("cancelled_arrivals", "scheduled_arrivals").alias(
            "arrival_cancellation_rate"
        ),
    )


def build_airport_connectivity(
    flights: DataFrame,
    report_year: int,
    report_month: int,
) -> DataFrame:
    """Build monthly outbound connectivity measures for each origin airport."""
    required = {
        "Origin",
        "Dest",
        "Reporting_Airline",
        "OriginAirportName",
        "OriginLatitude",
        "OriginLongitude",
        "Cancelled",
    }
    require_columns(flights, required, "Enriched Silver")
    grouped = flights.groupBy(
        "Origin",
        "OriginAirportName",
        "OriginLatitude",
        "OriginLongitude",
    ).agg(
        F.count("*").alias("scheduled_departures"),
        F.sum((~F.col("Cancelled")).cast("long")).alias("operated_departures"),
        F.countDistinct("Dest").alias("unique_destinations"),
        F.countDistinct("Reporting_Airline").alias("unique_carriers"),
    )
    return grouped.select(
        F.lit(report_year).cast("int").alias("report_year"),
        F.lit(report_month).cast("int").alias("report_month"),
        F.col("Origin").alias("airport"),
        F.col("OriginAirportName").alias("airport_name"),
        F.col("OriginLatitude").alias("latitude"),
        F.col("OriginLongitude").alias("longitude"),
        "scheduled_departures",
        "operated_departures",
        "unique_destinations",
        "unique_carriers",
    )
