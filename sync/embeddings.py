"""Embedding generation via Databricks ai_query.

Uses the built-in Spark SQL function ai_query to call the Databricks
Foundation Models API (BGE-large, 1024-dim) in a distributed manner.
No manual batching, auth, or retry logic needed — Spark handles it.
"""
from pyspark.sql import DataFrame

from sync.hash import build_content_string  # noqa: F401 — re-exported

EMBEDDING_ENDPOINT = "databricks-bge-large-en"


def embed_dataframe(df: DataFrame, text_col: str = "content") -> DataFrame:
    """Add an 'embedding' column to df by calling ai_query on text_col.

    Uses the Databricks BGE-large endpoint (1024-dimensional vectors).
    Spark handles parallelism, retries, and auth automatically.

    Args:
        df: Input DataFrame containing a text column.
        text_col: Name of the column containing text to embed. Default: 'content'.

    Returns:
        DataFrame with all original columns plus an 'embedding' column.
    """
    return df.selectExpr(
        "*",
        f"ai_query('{EMBEDDING_ENDPOINT}', {text_col}) AS embedding",
    )
