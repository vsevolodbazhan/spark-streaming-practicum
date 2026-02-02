from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from s3path import S3Path

from .utilities import convert_path_to_string

# Always read source data as-is.
DATA_SOURCE_FORMAT = "text"


class DataSource:
    """Base class for loading streaming data."""

    def load(self, session: SparkSession) -> DataFrame:
        raise NotImplementedError()


class LocalFileDataSource(DataSource):
    """
    Load streaming data from local filesystem.

    Parameters
    ----------
    path
        Local filesystem path to read from.
    """

    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self, session: SparkSession) -> DataFrame:
        return session.readStream.load(
            path=convert_path_to_string(self._path),
            format=DATA_SOURCE_FORMAT,
        )


class S3DataSource(LocalFileDataSource):
    """
    Load streaming data from S3.

    Parameters
    ----------
    path
        S3 path to read from.
    """

    def __init__(self, path: S3Path) -> None:
        self._path = path
