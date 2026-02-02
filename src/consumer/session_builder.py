from pathlib import Path
from typing import Self

from pyspark.sql import SparkSession

from .utilities import convert_path_to_string


class SessionBuilder:
    """
    High-level builder for configuring and creating Spark sessions.

    Provides methods to configure optional features like Spark UI and S3 access.
    Use method chaining to add configurations, then call `build()` to create
    the session.

    Attributes
    ----------
    SRARK_APP_NAME
        Default application name for the Spark session.

    Examples
    --------
    >>> session = (
    ...     SessionBuilder()
    ...     .with_spark_ui(port=4040)
    ...     .with_s3(access_key_id, secret_key, endpoint, region)
    ...     .build()
    ... )
    """

    SRARK_APP_NAME = "consumer"

    def __init__(self) -> None:
        self._builder = SparkSession.builder.appName(self.SRARK_APP_NAME)

    def with_spark_ui(self, port: int) -> Self:
        """
        Enable Spark UI on the specified port.

        Parameters
        ----------
        port
            Port number for the Spark UI web interface.
        """
        self._builder = (
            self._builder.config("spark.ui.port", str(port))
            .config("spark.ui.host", "0.0.0.0")
            .config("spark.ui.enabled", "true")
        )
        return self

    def with_s3(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_endpoint_url: str,
        aws_region: str,
    ) -> Self:
        """
        Configure S3 access using the S3A filesystem.

        Parameters
        ----------
        aws_access_key_id
            AWS access key ID.
        aws_secret_access_key
            AWS secret access key.
        aws_endpoint_url
            S3 endpoint URL (e.g., MinIO endpoint).
        aws_region
            AWS region name.
        """
        self._builder = (
            self._builder.config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
            .config("spark.hadoop.fs.s3a.endpoint", aws_endpoint_url)
            .config("spark.hadoop.fs.s3a.region", aws_region)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
        )
        return self

    def with_iceberg(
        self,
        catalog_name: str,
        warehouse_path: Path,
    ) -> Self:
        """
        Configure Iceberg with a Hadoop catalog stored on S3.

        Parameters
        ----------
        catalog_name
            Name for the Iceberg catalog (used in SQL as `catalog_name.db.table`).
        warehouse_path
            S3 path for the Iceberg warehouse.
        """
        self._builder = (
            self._builder.config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                f"spark.sql.catalog.{catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(
                f"spark.sql.catalog.{catalog_name}.type",
                "hadoop",
            )
            .config(
                f"spark.sql.catalog.{catalog_name}.warehouse",
                convert_path_to_string(warehouse_path),
            )
        )
        return self

    def build(self) -> SparkSession:
        """Create and return the configured Spark session."""
        return self._builder.getOrCreate()
