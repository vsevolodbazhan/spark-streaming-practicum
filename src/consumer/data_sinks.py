from enum import StrEnum
from pathlib import Path

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from s3path import S3Path

from .utilities import convert_path_to_string


class DataSinkMode(StrEnum):
    """Write mode for data sinks."""

    APPEND = "append"


class DataSinkFormat(StrEnum):
    """Output format for data sinks."""

    PARQUET = "parquet"


class DataSink:
    """
    Base class for writing DataFrames to a destination.

    Subclasses implement `write` to handle the actual write operation
    for their specific storage backend.
    """

    def write(self, batch: DataFrame) -> None:
        raise NotImplementedError()


class LocalDataSink(DataSink):
    """
    Write DataFrames to local filesystem.

    Parameters
    ----------
    path
        Local filesystem path for writing data.
    format
        Output format (e.g., Parquet).
    mode
        Write mode (e.g., append).
    """

    def __init__(
        self,
        path: Path,
        *,
        format: DataSinkFormat = DataSinkFormat.PARQUET,
        mode: DataSinkMode = DataSinkMode.APPEND,
    ) -> None:
        self._path = path
        self._format = format
        self._mode = mode

    def write(self, batch: DataFrame) -> None:
        batch.write.save(
            path=convert_path_to_string(self._path),
            mode=str(self._mode),
            format=str(self._format),
        )


class S3DataSink(LocalDataSink):
    """
    Write DataFrames to S3.

    Parameters
    ----------
    path
        S3 path for writing data.
    format
        Output format (e.g., Parquet).
    mode
        Write mode (e.g., append).
    """

    def __init__(self, path: S3Path, **kwargs) -> None:
        super().__init__(path=path, **kwargs)


class IcebergDataSink(DataSink):
    """
    Write DataFrames to an Iceberg table.

    Creates the table on first write using the DataFrame's schema.
    Supports schema evolution via mergeSchema option.

    Parameters
    ----------
    table_name
        Fully qualified Iceberg table name (e.g., "catalog.namespace.table").
    """

    def __init__(self, table_name: str) -> None:
        print(table_name)
        self._table_name = table_name
        self._table_exists: bool | None = None

    def _ensure_namespace_exists(self, session: SparkSession) -> None:
        """Create the namespace if it doesn't exist."""
        parts = self._table_name.split(".")
        if len(parts) >= 3:
            catalog = parts[0]
            namespace = parts[1]
            # Use backticks to avoid issues with reserved words.
            session.sql(f"create namespace if not exists `{catalog}`.`{namespace}`")

    def _does_table_exist(self, session: SparkSession) -> bool:
        """Check if the table already exists."""
        try:
            session.table(self._table_name)
            return True
        except AnalysisException:
            return False

    def write(self, batch: DataFrame) -> None:
        if self._table_exists is None:
            self._table_exists = self._does_table_exist(batch.sparkSession)

        if not self._table_exists:
            self._ensure_namespace_exists(batch.sparkSession)
            batch.writeTo(self._table_name).using("iceberg").create()
            self._table_exists = True
        else:
            # mergeSchema option enables schema evolution.
            batch.writeTo(self._table_name).option("mergeSchema", "true").append()
