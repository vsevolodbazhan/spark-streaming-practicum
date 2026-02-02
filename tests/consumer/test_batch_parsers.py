import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.consumer.batch_parsers import JsonArrayBatchParser


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    session = SparkSession.builder.appName("test").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def schema():
    """Test schema with two required fields."""
    return StructType(
        [
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
        ]
    )


@pytest.fixture
def parser(schema):
    """Create a JsonArrayBatchParser with the test schema."""
    return JsonArrayBatchParser(parsed_record_schema=schema)


class TestJsonArrayBatchParser:
    def test_parse_valid_records(self, spark, parser):
        """Valid records are parsed correctly."""
        batch = json.dumps([{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}])
        df = spark.createDataFrame([(batch,)], ["value"])

        result = parser.parse(df).collect()

        assert len(result) == 2
        assert result[0]["id"] == "1"
        assert result[0]["name"] == "Alice"
        assert result[0]["_is_corrupted_batch"] is False
        assert result[0]["_has_extra_fields"] is False

    def test_parse_corrupted_batch(self, spark, parser):
        """Corrupted JSON batch is marked as corrupted."""
        batch = "not valid json["
        df = spark.createDataFrame([(batch,)], ["value"])

        result = parser.parse(df).collect()

        assert len(result) == 1
        assert result[0]["_is_corrupted_batch"] is True
        assert result[0]["_raw_record"] == batch

    def test_parse_extra_fields(self, spark, parser):
        """Records with extra fields are flagged."""
        batch = json.dumps([{"id": "1", "name": "Alice", "extra": "field"}])
        df = spark.createDataFrame([(batch,)], ["value"])

        result = parser.parse(df).collect()

        assert len(result) == 1
        assert result[0]["_has_extra_fields"] is True
        assert result[0]["id"] == "1"
        assert result[0]["name"] == "Alice"

    def test_parse_missing_required_field(self, spark, parser):
        """Records missing required fields have null values."""
        batch = json.dumps([{"id": "1"}])
        df = spark.createDataFrame([(batch,)], ["value"])

        result = parser.parse(df).collect()

        assert len(result) == 1
        assert result[0]["id"] == "1"
        assert result[0]["name"] is None
        assert result[0]["_is_corrupted_batch"] is False

    def test_parse_empty_batch(self, spark, parser):
        """Empty JSON array produces one row marked as corrupted."""
        batch = json.dumps([])
        df = spark.createDataFrame([(batch,)], ["value"])

        result = parser.parse(df).collect()

        assert len(result) == 1
        assert result[0]["_is_corrupted_batch"] is True
        assert result[0]["_raw_record"] == batch
