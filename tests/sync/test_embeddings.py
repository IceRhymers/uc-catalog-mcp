"""Unit tests for sync/embeddings.py and sync/spark.py."""
import os
from unittest.mock import MagicMock, patch


def test_embed_dataframe_uses_ai_query():
    """selectExpr is called with ai_query('databricks-bge-large-en', content)."""
    from sync.embeddings import embed_dataframe

    mock_df = MagicMock()
    embed_dataframe(mock_df)

    call_args = mock_df.selectExpr.call_args[0]
    assert any("ai_query('databricks-bge-large-en', content" in arg for arg in call_args)


def test_embed_dataframe_passes_text_col():
    """Custom text_col appears in the ai_query call."""
    from sync.embeddings import embed_dataframe

    mock_df = MagicMock()
    embed_dataframe(mock_df, text_col="my_text")

    call_args = mock_df.selectExpr.call_args[0]
    assert any("my_text" in arg for arg in call_args)


def test_embed_dataframe_returns_dataframe():
    """embed_dataframe returns the result of selectExpr."""
    from sync.embeddings import embed_dataframe

    mock_df = MagicMock()
    result = embed_dataframe(mock_df)
    assert result is mock_df.selectExpr.return_value


def test_content_string_importable_from_embeddings():
    """build_content_string can be imported from sync.embeddings and matches sync.hash."""
    from sync.embeddings import build_content_string as emb_bcs
    from sync.hash import build_content_string as hash_bcs
    from sync.types import ColumnInfo

    cols = [ColumnInfo("id", "BIGINT"), ColumnInfo("name", "STRING", "user")]
    assert emb_bcs("main.s.t", "comment", cols) == hash_bcs("main.s.t", "comment", cols)


def test_build_content_string_format():
    """Content string format: '{full_name}: {comment}. Columns: {col} ({type}): {comment}, ...'"""
    from sync.hash import build_content_string
    from sync.types import ColumnInfo

    cols = [ColumnInfo("b", "STRING", "last"), ColumnInfo("a", "INT", None)]
    result = build_content_string("cat.sch.tbl", "A table", cols)
    # columns sorted by name: a first, then b
    assert result == "cat.sch.tbl: A table. Columns: a (INT): , b (STRING): last"


def test_create_spark_session_uses_connect_when_no_runtime():
    """When DATABRICKS_RUNTIME_VERSION unset, uses DatabricksSession.builder.serverless()."""
    from sync.spark import create_spark_session

    mock_session = MagicMock()
    mock_databricks_session = MagicMock()
    mock_databricks_session.builder.serverless.return_value.getOrCreate.return_value = mock_session

    env = {k: v for k, v in os.environ.items() if k != "DATABRICKS_RUNTIME_VERSION"}
    with patch.dict(os.environ, env, clear=True):
        with patch.dict("sys.modules", {"databricks.connect": MagicMock(DatabricksSession=mock_databricks_session)}):
            result = create_spark_session()

    mock_databricks_session.builder.serverless.assert_called_once()
    assert result is mock_session


def test_create_spark_session_uses_active_when_on_cluster():
    """When DATABRICKS_RUNTIME_VERSION is set, uses SparkSession.builder.getOrCreate()."""
    from sync import spark as spark_module

    mock_session = MagicMock()
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "15.4"}):
        with patch.object(spark_module.SparkSession, "builder") as mock_builder:
            mock_builder.getOrCreate.return_value = mock_session
            result = spark_module.create_spark_session()

    mock_builder.getOrCreate.assert_called_once()
    assert result is mock_session
