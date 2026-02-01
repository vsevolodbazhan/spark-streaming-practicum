import argparse
import os
from pathlib import Path
from time import sleep

import structlog
from dotenv import load_dotenv

from .data_sinks import (
    DataSinkType,
    LocalFileDataSink,
    S3DataSink,
    StdoutDataSink,
)
from .event_factory import EventFactory

logger = structlog.getLogger()


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(prog="producer")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="The number of events in a single batch.",
    )
    parser.add_argument(
        "--sleep-between-batches-seconds",
        type=int,
        default=3,
        help="Backoff time between batch generations.",
    )
    parser.add_argument(
        "--data-sink",
        choices=list(DataSinkType),
        default=DataSinkType.STDOUT,
        help="Data sink type.",
    )
    parser.add_argument(
        "--local-file-output",
        type=str,
        default=os.environ["PRODUCER_LOCAL_TARGET_PATH"],
        help="Output directory for local file sink.",
    )
    args = parser.parse_args()

    match data_sink_type := args.data_sink:
        case DataSinkType.STDOUT:
            data_sink = StdoutDataSink()
        case DataSinkType.LOCAL_FILE:
            data_sink = LocalFileDataSink(output=Path(args.local_file_output))
        case DataSinkType.S3:
            data_sink = S3DataSink(
                endpoint_url=os.environ["AWS_ENDPOINT_URL"],
                region=os.environ["AWS_REGION"],
                access_key=os.environ["AWS_ACCESS_KEY_ID"],
                secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                bucket=os.environ["AWS_BUCKET"],
                prefix="events",
            )
        case _:
            raise NotImplementedError(f"Unsupported data sink type: {data_sink_type}")

    event_factory = EventFactory()
    while True:
        events = event_factory.create_random_events(n=args.batch_size)
        data_sink.sink(events)
        sleep_duration = args.sleep_between_batches_seconds
        logger.info("Sleeping", duration=sleep_duration, units="seconds")
        sleep(sleep_duration)
