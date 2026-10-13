"""Controlled Gold-table loading and verification for MongoDB."""

import hashlib
import json
import os
from collections.abc import Mapping
from typing import Any

from pymongo import ASCENDING
from pyspark.sql import DataFrame


class MongoServingError(RuntimeError):
    """Report invalid MongoDB configuration or inconsistent stored results."""


COLLECTION_KEYS: dict[str, tuple[str, ...]] = {
    "carrier_summary": ("report_year", "report_month", "reporting_airline"),
    "month_summary": ("report_year", "report_month"),
    "season_summary": ("report_year", "season"),
    "airport_summary": ("report_year", "report_month", "airport"),
    "route_summary": ("report_year", "report_month", "origin", "destination"),
    "cancellation_summary": (
        "report_year",
        "report_month",
        "cancellation_code",
    ),
    "airport_connectivity": ("report_year", "report_month", "airport"),
    "airport_score": ("report_year", "report_month", "airport"),
    "route_score": ("report_year", "report_month", "origin", "destination"),
}


def mongodb_uri_from_environment(
    environment: Mapping[str, str] | None = None,
) -> str:
    """Read the private MongoDB connection URI from the environment."""
    values = environment if environment is not None else os.environ
    uri = values.get("MONGODB_URI", "").strip()
    if not uri:
        raise MongoServingError("MONGODB_URI is required")
    if not uri.startswith(("mongodb://", "mongodb+srv://")):
        raise MongoServingError("MONGODB_URI must use a MongoDB URI scheme")
    return uri


def _document_id(
    collection_name: str,
    document: dict[str, Any],
    key_fields: tuple[str, ...],
) -> str:
    identity = {
        "collection": collection_name,
        "keys": {field: document.get(field) for field in key_fields},
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def load_dataframe(
    database: Any,
    collection_name: str,
    dataframe: DataFrame,
    lineage: dict[str, str],
) -> int:
    """Upsert one small Gold DataFrame and remove stale rows for the same run."""
    try:
        key_fields = COLLECTION_KEYS[collection_name]
    except KeyError as error:
        raise MongoServingError(f"Unknown Gold collection: {collection_name}") from error

    collection = database[collection_name]
    documents: list[dict[str, Any]] = []
    for row in dataframe.toLocalIterator():
        document = row.asDict(recursive=True)
        document["lineage"] = lineage
        document["_id"] = _document_id(collection_name, document, key_fields)
        documents.append(document)

    ids = [document["_id"] for document in documents]
    for document in documents:
        collection.replace_one({"_id": document["_id"]}, document, upsert=True)
    stale_filter: dict[str, Any]
    if documents:
        stale_filter = {
            field: documents[0][field]
            for field in ("report_year", "report_month")
            if field in documents[0]
        }
    else:
        stale_filter = {"lineage": lineage}
    if ids:
        stale_filter["_id"] = {"$nin": ids}
    collection.delete_many(stale_filter)
    return len(documents)


def create_query_indexes(database: Any) -> None:
    """Create stable indexes for dashboard query patterns."""
    for collection_name, key_fields in COLLECTION_KEYS.items():
        keys = [(field, ASCENDING) for field in key_fields]
        database[collection_name].create_index(
            keys,
            name=f"{collection_name}_query",
            unique=True,
        )
        database[collection_name].create_index(
            [
                ("lineage.flight_source_sha256", ASCENDING),
                ("lineage.airport_source_sha256", ASCENDING),
            ],
            name=f"{collection_name}_lineage",
        )


def verify_collection_counts(
    database: Any,
    expected_rows: Mapping[str, int],
    lineage: dict[str, str],
) -> None:
    """Require MongoDB row counts to match Gold quality metrics."""
    mismatches: dict[str, tuple[int, int]] = {}
    for collection_name, expected in expected_rows.items():
        actual = database[collection_name].count_documents({"lineage": lineage})
        if actual != expected:
            mismatches[collection_name] = (expected, actual)
    if mismatches:
        raise MongoServingError(
            f"MongoDB collection counts do not match Gold: {mismatches}"
        )
