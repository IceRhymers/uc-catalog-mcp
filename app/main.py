"""uc-catalog-mcp FastAPI application."""

from __future__ import annotations

import json

from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session

from app.db.client import get_db
from app.tools.describe import describe_table
from app.tools.list import list_catalogs, list_schemas
from app.tools.search import search_tables

app = FastAPI(title="uc-catalog-mcp")

# ---------------------------------------------------------------------------
# Tool registry — used by tools/list and tools/call
# ---------------------------------------------------------------------------

TOOL_REGISTRY = {
    "search_tables": {
        "description": "Find tables semantically related to a query using cosine similarity.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Natural language search query"},
                "limit": {"type": "integer", "default": 10, "description": "Max results"},
            },
            "required": ["query"],
        },
    },
    "describe_table": {
        "description": "Return full schema detail (columns + types + comments) for a table.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "full_name": {"type": "string", "description": "catalog.schema.table"},
            },
            "required": ["full_name"],
        },
    },
    "list_catalogs": {
        "description": "Return sorted list of all indexed Unity Catalog catalog names.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    "list_schemas": {
        "description": "Return sorted list of schema names within a catalog.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "catalog": {"type": "string", "description": "Catalog name"},
            },
            "required": ["catalog"],
        },
    },
}


def _call_tool(name: str, arguments: dict, db: Session) -> dict:
    """Dispatch a tool call by name. Returns tool result dict."""
    if name == "search_tables":
        return search_tables(db=db, **arguments)
    if name == "describe_table":
        return describe_table(db=db, **arguments)
    if name == "list_catalogs":
        return list_catalogs(db=db)
    if name == "list_schemas":
        return list_schemas(db=db, **arguments)
    raise KeyError(f"Unknown tool: {name}")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/mcp")
async def mcp_handler(body: dict, db: Session = Depends(get_db)):
    """JSON-RPC 2.0 dispatcher for MCP tools/list and tools/call."""
    rpc_id = body.get("id")
    method = body.get("method", "")

    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": rpc_id,
            "result": {"tools": [{"name": k, **v} for k, v in TOOL_REGISTRY.items()]},
        }

    if method == "tools/call":
        params = body.get("params", {})
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})
        try:
            result = _call_tool(tool_name, arguments, db)
            is_error = isinstance(result, dict) and "error" in result
            text_content = (
                result.get("error", json.dumps(result)) if is_error else json.dumps(result)
            )
            return {
                "jsonrpc": "2.0",
                "id": rpc_id,
                "result": {
                    "content": [{"type": "text", "text": text_content}],
                    "isError": is_error,
                },
            }
        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id": rpc_id,
                "result": {
                    "content": [{"type": "text", "text": str(e)}],
                    "isError": True,
                },
            }

    return {
        "jsonrpc": "2.0",
        "id": rpc_id,
        "error": {"code": -32601, "message": "Method not found"},
    }
