import json
import sys
from enum import StrEnum
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

import boto3
import structlog
from rich import print

logger = structlog.getLogger()


class DataSinkType(StrEnum):
    STDOUT = "stdout"
    LOCAL_FILE = "local_file"
    S3 = "s3"


class DataSinkFormat(StrEnum):
    JSON = "json"


class DataSink:
    """
    Base class for data sinks.
    """

    def __init__(self, *, format: DataSinkFormat = DataSinkFormat.JSON) -> None:
        self._format = format

    def _serialize(self, batch: Iterable[dict]) -> bytes:
        match self._format:
            case DataSinkFormat.JSON:
                batch = list(batch)
                serialized_batch = json.dumps(batch).encode("utf-8")
            case _:
                raise NotImplementedError("Unsupported data format.")

        logger.info(
            "Serialized batch",
            batch_size=sys.getsizeof(serialized_batch),
            units="bytes",
        )
        return serialized_batch

    def _write(self, serialized_batch: bytes) -> Any:
        """
        This method is designed to be implemented in subclasses.
        """
        raise NotImplementedError()

    def sink(self, batch: Iterable[dict]) -> Any:
        serialized_batch = self._serialize(batch)
        self._write(serialized_batch)


class StdoutDataSink(DataSink):
    """
    Prints data to standard output.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        if self._format != DataSinkFormat.JSON:
            raise ValueError("Formats other than JSON are not supported by this sink.")

    def _write(self, serialized_batch: bytes) -> Any:
        for datum in json.loads(serialized_batch):
            print(datum)


class _FileDataSink(DataSink):
    def _generate_file_name(self) -> str:
        return f"{uuid4()}.{self._format}"


class LocalFileDataSink(_FileDataSink):
    """
    Write data as local files.
    """

    def __init__(self, *, output: Path, **kwargs) -> None:
        super().__init__(**kwargs)

        if output.exists() and not output.is_dir():
            raise ValueError("Output must be a directory.")

        output.mkdir(parents=True, exist_ok=True)
        self._output = output

    def _write(self, serialized_batch: bytes) -> Any:
        file = self._output / self._generate_file_name()
        file.write_bytes(serialized_batch)
        logger.info("Written batch to file", file=file)


class S3DataSink(_FileDataSink):
    """
    Write data to S3 bucket.
    """

    def __init__(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region: str,
        bucket: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._bucket = bucket
        self._client = boto3.client(
            service_name="s3",
            region_name=region,
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        logger.info(
            "Set up sink",
            endpoint_url=endpoint_url,
            region=region,
            bucket=bucket,
        )

    def _write(self, serialized_batch: bytes) -> Any:
        key = self._generate_file_name()
        self._client.upload_fileobj(
            Fileobj=BytesIO(serialized_batch),
            Bucket=self._bucket,
            Key=key,
        )
        logger.info("Uploaded batch", bucket=self._bucket, key=key)
