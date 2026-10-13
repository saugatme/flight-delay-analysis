"""Public Gold pipeline interfaces."""

from flight_delay_analysis.gold.analytics import (
    build_analytics_tables,
    collect_analytics_gold_metrics,
)
from flight_delay_analysis.gold.analytics_storage import (
    completed_analytics_gold_run,
    validate_analytics_gold_readback,
    write_analytics_gold_outputs,
)
from flight_delay_analysis.gold.carrier import build_carrier_summary
from flight_delay_analysis.gold.errors import GoldPipelineError
from flight_delay_analysis.gold.models import (
    AnalyticsGoldMetrics,
    AnalyticsGoldResult,
    CarrierGoldMetrics,
    CarrierGoldReadback,
    CarrierGoldResult,
    carrier_gold_metrics_from_json,
    carrier_gold_metrics_to_json,
)
from flight_delay_analysis.gold.paths import (
    analytics_quality_metrics_uri,
    analytics_table_uri,
    carrier_quality_metrics_uri,
    carrier_summary_uri,
    gold_run_uri,
)
from flight_delay_analysis.gold.quality import collect_carrier_gold_metrics
from flight_delay_analysis.gold.storage import (
    completed_carrier_gold_run,
    validate_carrier_gold_readback,
    write_carrier_gold_outputs,
)

__all__ = [
    "AnalyticsGoldMetrics",
    "AnalyticsGoldResult",
    "CarrierGoldMetrics",
    "CarrierGoldReadback",
    "CarrierGoldResult",
    "GoldPipelineError",
    "analytics_quality_metrics_uri",
    "analytics_table_uri",
    "build_analytics_tables",
    "build_carrier_summary",
    "carrier_gold_metrics_from_json",
    "carrier_gold_metrics_to_json",
    "carrier_quality_metrics_uri",
    "carrier_summary_uri",
    "collect_analytics_gold_metrics",
    "collect_carrier_gold_metrics",
    "completed_analytics_gold_run",
    "completed_carrier_gold_run",
    "gold_run_uri",
    "validate_analytics_gold_readback",
    "validate_carrier_gold_readback",
    "write_analytics_gold_outputs",
    "write_carrier_gold_outputs",
]
