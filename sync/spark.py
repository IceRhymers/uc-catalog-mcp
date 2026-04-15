"""Spark session factory with environment auto-detection.

Creates a Databricks Connect (serverless) session when running locally,
and uses the active cluster session when running on Databricks.
"""
import os

from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    """Return the active SparkSession, using Databricks Connect when running locally.

    Detects environment via DATABRICKS_RUNTIME_VERSION:
    - Set: running on a Databricks cluster — get the active session.
    - Unset: running locally — use DatabricksSession (databricks-connect)
      targeting serverless compute (Environment Version 5).

    Returns:
        Active or newly created SparkSession.
    """
    if os.environ.get("DATABRICKS_RUNTIME_VERSION") is None:
        try:
            from databricks.connect import DatabricksSession
            return DatabricksSession.builder.serverless().getOrCreate()
        except ImportError:
            return SparkSession.builder.getOrCreate()
    return SparkSession.builder.getOrCreate()
