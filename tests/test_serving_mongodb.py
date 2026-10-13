from typing import Any

import pytest
from pyspark.sql import SparkSession

from flight_delay_analysis.serving.mongodb import (
    MongoServingError,
    create_query_indexes,
    load_dataframe,
    mongodb_uri_from_environment,
    verify_collection_counts,
)


class FakeCollection:
    def __init__(self) -> None:
        self.documents: dict[str, dict[str, Any]] = {}
        self.indexes: list[tuple[Any, dict[str, Any]]] = []

    def replace_one(self, query, document, upsert=False) -> None:
        assert upsert is True
        self.documents[query["_id"]] = document

    def delete_many(self, query) -> None:
        keep = set(query.get("_id", {}).get("$nin", []))
        scope = {key: value for key, value in query.items() if key != "_id"}
        stale = [
            key
            for key, document in self.documents.items()
            if all(document.get(field) == value for field, value in scope.items())
            and key not in keep
        ]
        for key in stale:
            del self.documents[key]

    def count_documents(self, query) -> int:
        return sum(
            document["lineage"] == query["lineage"]
            for document in self.documents.values()
        )

    def create_index(self, keys, **options) -> None:
        self.indexes.append((keys, options))


class FakeDatabase:
    def __init__(self) -> None:
        self.collections: dict[str, FakeCollection] = {}

    def __getitem__(self, name: str) -> FakeCollection:
        return self.collections.setdefault(name, FakeCollection())


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("mongodb-serving-tests")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_mongodb_uri_must_come_from_environment() -> None:
    with pytest.raises(MongoServingError, match="MONGODB_URI"):
        mongodb_uri_from_environment({})

    assert mongodb_uri_from_environment(
        {"MONGODB_URI": "mongodb://user:secret@localhost:27017/flight_delay"}
    ).startswith("mongodb://")


def test_load_dataframe_is_idempotent_and_verifies_count(
    spark: SparkSession,
) -> None:
    database = FakeDatabase()
    dataframe = spark.createDataFrame(
        [(2025, 1, "AA", 10), (2025, 1, "BB", 20)],
        ["report_year", "report_month", "reporting_airline", "scheduled_flights"],
    )
    lineage = {
        "flight_source_sha256": "flight-sha",
        "airport_source_sha256": "airport-sha",
    }

    assert load_dataframe(database, "carrier_summary", dataframe, lineage) == 2
    assert load_dataframe(database, "carrier_summary", dataframe, lineage) == 2
    assert len(database["carrier_summary"].documents) == 2
    verify_collection_counts(database, {"carrier_summary": 2}, lineage)


def test_create_query_indexes_covers_all_collections() -> None:
    database = FakeDatabase()

    create_query_indexes(database)

    assert len(database.collections) == 9
    assert all(len(collection.indexes) == 2 for collection in database.collections.values())
