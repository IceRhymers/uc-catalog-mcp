"""SHA-256 content hash for hash-based incremental ETL.

Computes a deterministic hash of a table's metadata — used to detect changes
without calling the embedding API on every sync cycle.
"""
import hashlib

from sync.types import ColumnInfo


def build_content_string(
    full_name: str,
    comment: str | None,
    columns: list[ColumnInfo],
) -> str:
    """Build a canonical string representation of table metadata.

    Columns are sorted by name so column order does not affect the hash.
    This format is shared with sync/embeddings.py — the same string is used
    as both the hash input and the embedding input.

    Args:
        full_name: Fully-qualified table name (catalog.schema.table).
        comment: Table description, or None.
        columns: List of columns (order-independent).

    Returns:
        Canonical content string.
    """
    sorted_cols = sorted(columns, key=lambda c: c.name)
    col_parts = ", ".join(
        f"{c.name} ({c.type}): {c.comment or ''}" for c in sorted_cols
    )
    return f"{full_name}: {comment or ''}. Columns: {col_parts}"


def compute_content_hash(
    full_name: str,
    comment: str | None,
    columns: list[ColumnInfo],
) -> str:
    """Compute a SHA-256 hash of table metadata content.

    Returns a 64-character lowercase hex string. Identical logical content
    always produces the same hash regardless of column order.

    Args:
        full_name: Fully-qualified table name (catalog.schema.table).
        comment: Table description, or None.
        columns: List of columns (order-independent).

    Returns:
        64-character SHA-256 hex digest.
    """
    content = build_content_string(full_name, comment, columns)
    return hashlib.sha256(content.encode()).hexdigest()
