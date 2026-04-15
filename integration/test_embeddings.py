"""Integration tests for sync/embeddings.py — requires Databricks credentials.

Run with: make test-integration
These tests are NOT run in CI (integration/ is outside tests/).
"""
import pytest

from sync.spark import create_spark_session
from sync.embeddings import embed_dataframe


@pytest.fixture(scope="module")
def spark():
    return create_spark_session()


def test_embed_dataframe_returns_1024_dim(spark):
    """ai_query returns 1024-dim vectors from databricks-bge-large-en."""
    df = spark.createDataFrame(
        [("main.schema.users: User table. Columns: id (BIGINT): ",)],
        ["content"],
    )
    result = embed_dataframe(df).collect()
    assert len(result) == 1
    assert len(result[0]["embedding"]) == 1024
    assert all(isinstance(v, float) for v in result[0]["embedding"])


def test_embed_dataframe_multiple_rows(spark):
    """Multiple rows each produce a 1024-dim embedding."""
    rows = [(f"table_{i}: comment. Columns: id (BIGINT): ",) for i in range(3)]
    df = spark.createDataFrame(rows, ["content"])
    result = embed_dataframe(df).collect()
    assert len(result) == 3
    assert all(len(r["embedding"]) == 1024 for r in result)
