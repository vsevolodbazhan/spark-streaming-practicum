from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    explode_outer,
    from_json,
    map_keys,
    size,
)
from pyspark.sql.types import (
    ArrayType,
    MapType,
    StringType,
    StructType,
)


class BatchParser:
    """
    Base class for parsing raw batch data into structured records.

    Subclasses implement `_parse` to transform raw batches into a DataFrame
    with three columns: raw batch, raw record, and parsed record (as a struct).
    The base class enforces a consistent output schema.

    Parameters
    ----------
    parsed_record_schema
        Schema for the business data within each record.

    Attributes
    ----------
    RAW_BATCH_COLUMN_NAME
        Column containing the original batch string.
    RAW_RECORD_COLUMN_NAME
        Column containing the individual record string.
    PARSED_RECORD_COLUMN_NAME
        Column containing the parsed record struct.
    IS_CORRUPTED_BATCH_COLUMN_NAME
        Column that marks if the batch was corrupted.
    HAS_EXTRA_FIELDS_COLUMN_NAME
        Column that marks if the record had extra fields not specified
        in the schema.
    """

    RAW_BATCH_COLUMN_NAME = "_raw_batch"
    RAW_RECORD_COLUMN_NAME = "_raw_record"
    IS_CORRUPTED_BATCH_COLUMN_NAME = "_is_corrupted_batch"
    HAS_EXTRA_FIELDS_COLUMN_NAME = "_has_extra_fields"
    PARSED_RECORD_COLUMN_NAME = "_parsed_record"

    def __init__(self, parsed_record_schema: StructType) -> None:
        self.parsed_record_schema = parsed_record_schema

    def parse(self, batch: DataFrame) -> DataFrame:
        expected_field_count = len(self.parsed_record_schema.fields)
        return (
            self._parse(batch)
            # Mark batches where JSON parsing failed
            # (exploded array of raw records in empty).
            .withColumn(
                self.IS_CORRUPTED_BATCH_COLUMN_NAME,
                col(self.RAW_RECORD_COLUMN_NAME).isNull(),
            )
            # Mark records with extra fields not in the schema.
            .withColumn(
                self.HAS_EXTRA_FIELDS_COLUMN_NAME,
                size(
                    map_keys(
                        from_json(
                            col(self.RAW_RECORD_COLUMN_NAME),
                            MapType(StringType(), StringType()),  # type: ignore
                        )
                    )
                )
                > expected_field_count,
            )
            # For corrupted batches, use the original batch as the raw record.
            .withColumn(
                self.RAW_RECORD_COLUMN_NAME,
                coalesce(
                    col(self.RAW_RECORD_COLUMN_NAME),
                    col(self.RAW_BATCH_COLUMN_NAME),
                ),
            )
            .drop(self.RAW_BATCH_COLUMN_NAME)
            .select(
                [
                    col(column_name)
                    # Select parsing metadata columns.
                    for column_name in [
                        self.RAW_RECORD_COLUMN_NAME,
                        self.IS_CORRUPTED_BATCH_COLUMN_NAME,
                        self.HAS_EXTRA_FIELDS_COLUMN_NAME,
                    ]
                    # Select parsed columns specified in the schema.
                    + [
                        f"{self.PARSED_RECORD_COLUMN_NAME}.{field.name}"
                        for field in self.parsed_record_schema
                    ]
                ]
            )
        )

    def _parse(self, batch: DataFrame) -> DataFrame:
        raise NotImplementedError()


class JsonArrayBatchParser(BatchParser):
    """
    Parse batches containing JSON arrays of records.

    Each batch is expected to be a JSON array of objects. The array is exploded
    into individual records, which are then parsed against the provided schema.

    Parameters
    ----------
    parsed_record_schema
        Schema for the business data within each record.
    """

    def _parse(self, batch: DataFrame) -> DataFrame:
        return (
            batch.select(
                # Keep raw batch value.
                col("value").alias(self.RAW_BATCH_COLUMN_NAME),
                # Parse batch value into an array to strings (raw records).
                explode_outer(
                    from_json(
                        col("value"),
                        ArrayType(StringType()),
                    ),
                ).alias(self.RAW_RECORD_COLUMN_NAME),
            )
            # Parse raw records against the schema.
            .withColumn(
                self.PARSED_RECORD_COLUMN_NAME,
                from_json(
                    col(self.RAW_RECORD_COLUMN_NAME),
                    self.parsed_record_schema,
                ),
            )
        )
