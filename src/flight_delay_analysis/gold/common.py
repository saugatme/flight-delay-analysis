"""Shared expressions for Gold aggregations."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from flight_delay_analysis.gold.errors import GoldPipelineError


def require_columns(dataframe: DataFrame, required: set[str], label: str) -> None:
    """Require named input columns before a Gold aggregation starts."""
    missing = sorted(required - set(dataframe.columns))
    if missing:
        raise GoldPipelineError(f"{label} is missing columns: {', '.join(missing)}")


def safe_rate(numerator: str | Column, denominator: str | Column) -> Column:
    """Divide only when the denominator is positive."""
    numerator_column = F.col(numerator) if isinstance(numerator, str) else numerator
    denominator_column = (
        F.col(denominator) if isinstance(denominator, str) else denominator
    )
    return F.when(denominator_column > 0, numerator_column / denominator_column)


def season_name(month: int) -> str:
    """Map a calendar month to a meteorological season."""
    if month in {12, 1, 2}:
        return "winter"
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    return "autumn"
