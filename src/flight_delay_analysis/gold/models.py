"""Typed quality records for Gold datasets."""

import json
from dataclasses import asdict, dataclass
from typing import Any

from flight_delay_analysis.gold.errors import GoldPipelineError


@dataclass(frozen=True)
class CarrierGoldMetrics:
    """Record reconciled counts and lineage for one carrier Gold result."""

    flight_source_sha256: str
    airport_source_sha256: str
    input_flight_rows: int
    output_carrier_rows: int
    scheduled_flights: int
    operated_flights: int
    arrival_result_flights: int
    delayed_flights: int
    cancelled_flights: int
    delay_threshold_minutes: int
    arrival_delay_rate_denominator: str
    cancellation_rate_denominator: str
    processed_at_utc: str


@dataclass(frozen=True)
class CarrierGoldResult:
    """Describe whether carrier Gold output was written or reused."""

    status: str
    metrics: CarrierGoldMetrics


@dataclass(frozen=True)
class CarrierGoldReadback:
    """Store verification facts from persisted carrier Gold Parquet."""

    output_carrier_rows: int
    scheduled_flights: int
    cancelled_flights: int


@dataclass(frozen=True)
class AnalyticsGoldMetrics:
    """Record lineage and reconciled counts for all analytics Gold tables."""

    flight_source_sha256: str
    airport_source_sha256: str
    input_flight_rows: int
    cancelled_flights: int
    delay_threshold_minutes: int
    table_rows: dict[str, int]
    route_score_formula: str
    airport_score_formula: str
    processed_at_utc: str


@dataclass(frozen=True)
class AnalyticsGoldResult:
    """Describe whether the analytics Gold bundle was written or reused."""

    status: str
    metrics: AnalyticsGoldMetrics


def carrier_gold_metrics_to_json(metrics: CarrierGoldMetrics) -> str:
    """Serialize carrier Gold quality metrics in a stable format."""
    return json.dumps(asdict(metrics), indent=2, sort_keys=True) + "\n"


def carrier_gold_metrics_from_json(content: str) -> CarrierGoldMetrics:
    """Deserialize stored carrier Gold quality metrics."""
    try:
        values: dict[str, Any] = json.loads(content)
        return CarrierGoldMetrics(**values)
    except (json.JSONDecodeError, TypeError) as error:
        raise GoldPipelineError("Invalid carrier Gold quality metrics") from error


def analytics_gold_metrics_to_json(metrics: AnalyticsGoldMetrics) -> str:
    """Serialize analytics Gold quality metrics."""
    return json.dumps(asdict(metrics), indent=2, sort_keys=True) + "\n"


def analytics_gold_metrics_from_json(content: str) -> AnalyticsGoldMetrics:
    """Deserialize analytics Gold quality metrics."""
    try:
        values: dict[str, Any] = json.loads(content)
        return AnalyticsGoldMetrics(**values)
    except (json.JSONDecodeError, TypeError) as error:
        raise GoldPipelineError("Invalid analytics Gold quality metrics") from error
