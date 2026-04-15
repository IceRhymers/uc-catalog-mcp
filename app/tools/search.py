"""search_tables MCP tool — semantic table search via pgvector ANN."""

from __future__ import annotations

from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.embed import embed_text


def search_tables(
    query: str,
    db: Session,
    limit: int = 10,
    embed_fn: Callable[[str], list[float]] = embed_text,
) -> list[dict]:
    """Find tables semantically related to query using cosine similarity.

    Embeds the query string, then runs a pgvector ANN search over catalog_metadata.
    Returns results ranked by descending similarity (most relevant first).

    Use this as the primary discovery tool when an agent needs to find tables
    relevant to a topic or question.

    Args:
        query: Natural language search query.
        db: SQLAlchemy Session (injected by /mcp handler).
        limit: Maximum number of results to return (default: 10).
        embed_fn: Embedding function (injectable for tests).

    Returns:
        List of dicts with keys: full_name, comment, similarity (0–1 float).
    """
    vec = embed_fn(query)
    rows = db.execute(
        text("""
            SELECT full_name, comment, 1 - (embedding <=> :vec::vector) AS similarity
            FROM catalog_metadata
            ORDER BY embedding <=> :vec::vector
            LIMIT :limit
        """),
        {"vec": str(vec), "limit": limit},
    ).fetchall()
    return [
        {"full_name": r.full_name, "comment": r.comment, "similarity": float(r.similarity)}
        for r in rows
    ]
