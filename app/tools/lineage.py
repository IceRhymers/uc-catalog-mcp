"""Lineage MCP tools — pure Databricks API passthrough, no Lakebase involvement."""

from __future__ import annotations

from databricks.sdk import WorkspaceClient


def get_table_lineage(full_name: str, ws: WorkspaceClient | None = None) -> dict:
    """Return upstream and downstream table lineage from Databricks Unity Catalog.

    Data is always fresh — forwarded directly to the lineage API, not cached in Lakebase.

    Args:
        full_name: Three-part table name (catalog.schema.table).
        ws: Optional WorkspaceClient for testing. Created automatically if not provided.

    Returns:
        Dict with upstream_tables and downstream_tables lists, or {"error": ...} on failure.
    """
    client = ws or WorkspaceClient()
    try:
        return client.api_client.do(
            "GET",
            "/api/2.0/lineage-tracking/table-lineage",
            query={"table_name": full_name, "include_entity_lineage": "true"},
        )
    except Exception as e:
        return {"error": str(e)}


def get_column_lineage(full_name: str, column: str, ws: WorkspaceClient | None = None) -> dict:
    """Return upstream and downstream column lineage from Databricks Unity Catalog.

    Data is always fresh — forwarded directly to the lineage API, not cached in Lakebase.

    Args:
        full_name: Three-part table name (catalog.schema.table).
        column: Column name to trace.
        ws: Optional WorkspaceClient for testing. Created automatically if not provided.

    Returns:
        Dict with column lineage info, or {"error": ...} on failure.
    """
    client = ws or WorkspaceClient()
    try:
        return client.api_client.do(
            "GET",
            "/api/2.0/lineage-tracking/column-lineage",
            query={"table_name": full_name, "column_name": column},
        )
    except Exception as e:
        return {"error": str(e)}
