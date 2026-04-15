"""describe_table MCP tool — full column detail for a table."""

from __future__ import annotations

from sqlalchemy.orm import Session

from app.db.models import CatalogMetadataOrm


def describe_table(full_name: str, db: Session) -> dict:
    """Return full schema detail for a Unity Catalog table.

    Returns table comment and a typed column list (name, type, comment).
    Use after search_tables to get column-level detail for a specific table.

    Args:
        full_name: Fully-qualified table name (catalog.schema.table).
        db: SQLAlchemy Session (injected by /mcp handler).

    Returns:
        Dict with full_name, comment, columns (list of {name, type, comment}).
        Returns {"error": "Table not found: ..."} if full_name is not indexed.
    """
    row = db.get(CatalogMetadataOrm, full_name)
    if row is None:
        return {"error": f"Table not found: {full_name}"}
    return {
        "full_name": row.full_name,
        "comment": row.comment,
        "columns": [
            {"name": c["name"], "type": c["type"], "comment": c.get("comment")}
            for c in (row.columns or [])
        ],
    }
