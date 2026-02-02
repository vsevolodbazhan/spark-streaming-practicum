from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when

from .batch_parsers import BatchParser
from .data_sinks import DataSink
from .data_sources import DataSource
from .utilities import Path, convert_path_to_string


class StreamProcessor:
    """
    Process streaming data from a source, parse batches, and route to sinks.

    Valid records are written to the main sink, while corrupted batches and
    records with invalid schema are routed to a dead letter sink.

    Parameters
    ----------
    session
        Spark session for stream processing.
    source
        Data source to read streaming batches from.
    sink
        Destination for valid records.
    dead_letters_sink
        Destination for corrupted or invalid records.
    parser
        Parser for transforming raw batches into structured records.
    checkpoint_location
        Path for storing stream checkpoints.
    trigger_interval
        Processing trigger interval (e.g., "20 seconds").

    Attributes
    ----------
    DEAD_LETTER_REASON_COLUMN_NAME
        Column name for the dead letter reason in invalid records.
    """

    DEAD_LETTER_REASON_COLUMN_NAME = "_dead_letter_reason"

    def __init__(
        self,
        session: SparkSession,
        *,
        source: DataSource,
        sink: DataSink,
        dead_letters_sink: DataSink,
        parser: BatchParser,
        checkpoint_location: Path,
        # TODO: Accept timedelta.
        trigger_interval: str,
    ) -> None:
        self._session = session
        self._source = source
        self._sink = sink
        self._dead_letters_sink = dead_letters_sink
        self._parser = parser
        self._checkpoint_location = convert_path_to_string(checkpoint_location)
        self._trigger_interval = trigger_interval

    def _route_to_sinks(self, batch: DataFrame, batch_id: int) -> None:
        # Build validation condition: all required columns must be present.
        has_all_required = lit(True)
        for required_column in [
            field.name
            for field in self._parser.parsed_record_schema.fields
            if not field.nullable
        ]:
            has_all_required = has_all_required & col(required_column).isNotNull()

        # Determine dead letter reason.
        batch = batch.withColumn(
            self.DEAD_LETTER_REASON_COLUMN_NAME,
            when(
                col(self._parser.IS_CORRUPTED_BATCH_COLUMN_NAME),
                lit("corrupted_batch"),
            )
            .when(
                ~has_all_required,
                lit("invalid_schema"),
            )
            .otherwise(lit(None)),
        ).drop(self._parser.IS_CORRUPTED_BATCH_COLUMN_NAME)

        # If there is no reason to dead letter, drop service columns
        # and route to valid records.
        valid_batch = batch.filter(
            col(self.DEAD_LETTER_REASON_COLUMN_NAME).isNull()
        ).drop(
            self._parser.RAW_RECORD_COLUMN_NAME,
            self.DEAD_LETTER_REASON_COLUMN_NAME,
        )
        # If there is a reason to dead letter, route to invalid records.
        invalid_batch = batch.filter(
            col(self.DEAD_LETTER_REASON_COLUMN_NAME).isNotNull()
        ).select(
            # Reorder columns to put dead letter reason first.
            self.DEAD_LETTER_REASON_COLUMN_NAME,
            *[
                column
                for column in batch.columns
                if column != self.DEAD_LETTER_REASON_COLUMN_NAME
            ],
        )

        # Write to sinks.
        for batch, sink in [
            (valid_batch, self._sink),
            (invalid_batch, self._dead_letters_sink),
        ]:
            sink.write(batch)

    def start(self) -> None:
        (
            self._parser.parse(self._source.load(self._session))
            .writeStream.option(
                "checkpointLocation",
                self._checkpoint_location,
            )
            .trigger(processingTime=self._trigger_interval)
            .foreachBatch(self._route_to_sinks)
            .start()
            .awaitTermination()
        )
