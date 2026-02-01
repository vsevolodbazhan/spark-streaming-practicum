import os
from pathlib import Path
from textwrap import dedent

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

PRODUCER_TARGET = Path(os.environ["PRODUCER_LOCAL_TARGET_PATH"])
CONSUMER_TARGET = Path(os.environ["CONSUMER_LOCAL_TARGET_PATH"])

(
    SparkSession.builder.appName("consumer")
    .getOrCreate()
    .readStream.load(
        path=PRODUCER_TARGET.as_posix(),
        format="json",
        schema=dedent(
            """
            user_id string,
            event_id string,
            event_timestamp timestamp,
            event_type string,
            properties map<string, string>
            """
        ),
    )
    .writeStream.option(
        "checkpointLocation",
        (CONSUMER_TARGET / "checkpoints").as_posix(),
    )
    .start(
        path=(CONSUMER_TARGET / "output").as_posix(),
        format="parquet",
        outputMode="append",
    )
    .awaitTermination()
)
