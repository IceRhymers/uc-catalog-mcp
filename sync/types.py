"""Canonical column type for the uc-catalog-mcp sync pipeline.

ColumnInfo is the single source of truth for column metadata shared between
the sync pipeline (Spark cluster) and the app (Databricks App venv).
Both environments import from this module — sync/ never imports from app/.
"""

from typing import NamedTuple


class ColumnInfo(NamedTuple):
    """A single Unity Catalog column.

    Args:
        name: Column name.
        type: Data type string (e.g. 'BIGINT', 'STRING').
        comment: Optional column description.
    """

    name: str
    type: str
    comment: str | None = None
