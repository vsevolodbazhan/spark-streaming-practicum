from pathlib import Path
from typing import Self

from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, SparkSession
from s3path import S3Path

from .utilities import convert_path_to_string


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

    def __init__(self, path: Path) -> None:
        self._path = path
        self._format = "parquet"
        self._mode = "append"

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
        self._table_name = table_name
        self._table_exists: bool | None = None
        self._partitioned_by: Column | None

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

    def with_partitioned_by(self, _partitioned_by: Column | None) -> Self:
        """
        Set partioning predicate.
        """
        self._partitioned_by = _partitioned_by
        return self

    def write(self, batch: DataFrame) -> None:
        if self._table_exists is None:
            self._table_exists = self._does_table_exist(batch.sparkSession)

        if not self._table_exists:
            self._ensure_namespace_exists(batch.sparkSession)
            writer = batch.writeTo(self._table_name).using("iceberg")
            if self._partitioned_by is not None:
                writer.partitionedBy(self._partitioned_by)
            writer.create()
            self._table_exists = True
        else:
            # mergeSchema option enables schema evolution.
            batch.writeTo(self._table_name).option("mergeSchema", "true").append()
