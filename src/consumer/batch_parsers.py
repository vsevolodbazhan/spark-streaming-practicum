from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer, from_json
from pyspark.sql.types import ArrayType, StringType, StructField, StructType


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
    """

    RAW_BATCH_COLUMN_NAME = "_raw_batch"
    RAW_RECORD_COLUMN_NAME = "_raw_record"
    PARSED_RECORD_COLUMN_NAME = "_parsed_record"

    def __init__(self, parsed_record_schema: StructType) -> None:
        self._parsed_record_schema = parsed_record_schema

    @property
    def _parsed_dataframe_schema(self) -> StructType:
        """Return the full schema of the parsed dataframe including metadata columns."""
        return StructType(
            [
                StructField(self.RAW_BATCH_COLUMN_NAME, StringType(), nullable=True),
                StructField(self.RAW_RECORD_COLUMN_NAME, StringType(), nullable=True),
                StructField(
                    self.PARSED_RECORD_COLUMN_NAME,
                    self._parsed_record_schema,
                    nullable=True,
                ),
            ]
        )

    def parse(self, batch: DataFrame) -> DataFrame:
        return self._parse(batch).select(
            # Enforce output schema.
            [
                col(field.name).cast(field.dataType)
                for field in self._parsed_dataframe_schema.fields
            ]
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
                    self._parsed_record_schema,
                ),
            )
        )
