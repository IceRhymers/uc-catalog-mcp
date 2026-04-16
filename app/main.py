"""uc-catalog-mcp FastMCP application."""

from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager

from databricks.sdk import WorkspaceClient
from mcp.server.fastmcp import Context, FastMCP
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.db.client import create_lakebase_engine
from app.tools.describe import describe_table as _describe_table
from app.tools.lineage import get_column_lineage as _get_column_lineage
from app.tools.lineage import get_table_lineage as _get_table_lineage
from app.tools.list import list_catalogs as _list_catalogs
from app.tools.list import list_schemas as _list_schemas
from app.tools.search import search_tables as _search_tables

logger = logging.getLogger(__name__)

LAKEBASE_INSTANCE = os.environ.get("LAKEBASE_INSTANCE", "uc-catalog-mcp-db")
OBO_HEADER = "x-forwarded-access-token"


def _extract_obo_client(ctx: Context) -> WorkspaceClient | str:
    """Extract OBO token from X-Forwarded-Access-Token and return a WorkspaceClient.

    Returns a WorkspaceClient acting as the calling user on success, or a JSON
    error string on failure. Never falls back to the app service principal —
    a missing token is an explicit error to prevent silent privilege escalation.
    """
    token = ctx.request_context.request.headers.get(OBO_HEADER)
    if not token:
        return json.dumps({"error": "OBO token required — connect via Databricks OAuth"})
    return WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=token)


@asynccontextmanager
async def lifespan(server: FastMCP):
    logger.info("Connecting to Lakebase instance %s", LAKEBASE_INSTANCE)
    ws = WorkspaceClient()
    engine = create_lakebase_engine(LAKEBASE_INSTANCE, ws)
    session_factory = sessionmaker(bind=engine)
    yield {"session_factory": session_factory}
    engine.dispose()


mcp = FastMCP("uc-catalog-mcp", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool()
async def search_tables(query: str, ctx: Context, limit: int = 10) -> str:
    """Find tables semantically related to a query using cosine similarity."""
    session_factory = ctx.request_context.lifespan_context["session_factory"]
    session = session_factory()
    try:
        result = _search_tables(db=session, query=query, limit=limit)
        return json.dumps(result)
    finally:
        session.close()


@mcp.tool()
async def describe_table(full_name: str, ctx: Context) -> str:
    """Return full schema detail (columns + types + comments) for a table."""
    session_factory = ctx.request_context.lifespan_context["session_factory"]
    session = session_factory()
    try:
        result = _describe_table(db=session, full_name=full_name)
        return json.dumps(result)
    finally:
        session.close()


@mcp.tool()
async def list_catalogs(ctx: Context) -> str:
    """Return sorted list of all indexed Unity Catalog catalog names."""
    session_factory = ctx.request_context.lifespan_context["session_factory"]
    session = session_factory()
    try:
        result = _list_catalogs(db=session)
        return json.dumps(result)
    finally:
        session.close()


@mcp.tool()
async def list_schemas(catalog: str, ctx: Context) -> str:
    """Return sorted list of schema names within a catalog."""
    session_factory = ctx.request_context.lifespan_context["session_factory"]
    session = session_factory()
    try:
        result = _list_schemas(db=session, catalog=catalog)
        return json.dumps(result)
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


@mcp.tool()
async def get_table_lineage(full_name: str, ctx: Context) -> str:
    """Return upstream and downstream table lineage for a Unity Catalog table.

    Enforces the calling user's UC permissions via OBO token — lineage is only
    returned for tables the user has access to in Unity Catalog.
    """
    ws_or_error = _extract_obo_client(ctx)
    if isinstance(ws_or_error, str):
        return ws_or_error
    result = _get_table_lineage(full_name, ws=ws_or_error)
    return json.dumps(result)


@mcp.tool()
async def get_column_lineage(full_name: str, column: str, ctx: Context) -> str:
    """Return upstream and downstream column lineage for a specific column.

    Enforces the calling user's UC permissions via OBO token — lineage is only
    returned for tables the user has access to in Unity Catalog.
    """
    ws_or_error = _extract_obo_client(ctx)
    if isinstance(ws_or_error, str):
        return ws_or_error
    result = _get_column_lineage(full_name, column=column, ws=ws_or_error)
    return json.dumps(result)


@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


# ---------------------------------------------------------------------------
# ASGI app export
# ---------------------------------------------------------------------------

app = mcp.streamable_http_app()
