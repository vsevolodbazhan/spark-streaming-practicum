from enum import StrEnum
from pathlib import Path

from pyspark.sql import DataFrame
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


class LocalFileDataSink(DataSink):
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


class S3DataSink(LocalFileDataSink):
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
