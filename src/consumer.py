import os
from pathlib import Path
from textwrap import dedent

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

CONSUMER_TARGET_DIR = Path(os.environ["CONSUMER_LOCAL_TARGET_DIR"])

spark = SparkSession.builder.appName("consumer").getOrCreate()
input = spark.readStream.load(
    path="target/producer/output",
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
query = (
    input.writeStream.option(
        "checkpointLocation",
        (CONSUMER_TARGET_DIR / "checkpoints").as_posix(),
    )
    .start(
        path=(CONSUMER_TARGET_DIR / "output").as_posix(),
        format="parquet",
        outputMode="append",
    )
    .awaitTermination()
)
