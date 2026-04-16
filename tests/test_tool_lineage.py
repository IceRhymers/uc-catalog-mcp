"""Unit tests for app/tools/lineage.py and OBO auth wrappers in app/main.py."""

import asyncio
import json
import os
from unittest.mock import MagicMock, patch


def _make_ws(table_response=None, column_response=None, side_effect=None):
    ws = MagicMock()
    if side_effect is not None:
        ws.api_client.do.side_effect = side_effect
    else:
        ws.api_client.do.return_value = table_response or column_response or {}
    return ws


def test_table_lineage_calls_correct_endpoint():
    from app.tools.lineage import get_table_lineage

    mock_ws = MagicMock()
    mock_ws.api_client.do.return_value = {
        "upstream_tables": [{"name": "catalog.schema.upstream"}],
        "downstream_tables": [],
    }
    result = get_table_lineage("catalog.schema.table", ws=mock_ws)
    mock_ws.api_client.do.assert_called_once_with(
        "GET",
        "/api/2.0/lineage-tracking/table-lineage",
        query={"table_name": "catalog.schema.table", "include_entity_lineage": "true"},
    )
    assert "upstream_tables" in result


def test_table_lineage_returns_upstream_downstream():
    from app.tools.lineage import get_table_lineage

    mock_ws = MagicMock()
    mock_ws.api_client.do.return_value = {
        "upstream_tables": [{"name": "catalog.schema.src"}],
        "downstream_tables": [{"name": "catalog.schema.sink"}],
    }
    result = get_table_lineage("catalog.schema.table", ws=mock_ws)
    assert "upstream_tables" in result
    assert "downstream_tables" in result
    assert result["upstream_tables"][0]["name"] == "catalog.schema.src"
    assert result["downstream_tables"][0]["name"] == "catalog.schema.sink"


def test_column_lineage_calls_correct_endpoint():
    from app.tools.lineage import get_column_lineage

    mock_ws = MagicMock()
    mock_ws.api_client.do.return_value = {"upstream_cols": [], "downstream_cols": []}
    result = get_column_lineage("catalog.schema.table", "id", ws=mock_ws)
    mock_ws.api_client.do.assert_called_once_with(
        "GET",
        "/api/2.0/lineage-tracking/column-lineage",
        query={"table_name": "catalog.schema.table", "column_name": "id"},
    )
    assert result is not None


def test_column_lineage_returns_lineage_info():
    from app.tools.lineage import get_column_lineage

    mock_ws = MagicMock()
    mock_ws.api_client.do.return_value = {
        "upstream_cols": [{"name": "id", "table_name": "catalog.schema.src"}],
        "downstream_cols": [],
    }
    result = get_column_lineage("catalog.schema.table", "id", ws=mock_ws)
    assert "upstream_cols" in result
    assert result["upstream_cols"][0]["name"] == "id"


def test_table_lineage_api_error_returns_structured_error():
    from app.tools.lineage import get_table_lineage

    mock_ws = MagicMock()
    mock_ws.api_client.do.side_effect = Exception("PERMISSION_DENIED: not authorized")
    result = get_table_lineage("catalog.schema.table", ws=mock_ws)
    assert "error" in result
    assert "PERMISSION_DENIED" in result["error"]


def test_column_lineage_api_error_returns_structured_error():
    from app.tools.lineage import get_column_lineage

    mock_ws = MagicMock()
    mock_ws.api_client.do.side_effect = Exception("NOT_FOUND: table does not exist")
    result = get_column_lineage("catalog.schema.missing", "id", ws=mock_ws)
    assert "error" in result
    assert "NOT_FOUND" in result["error"]


# ---------------------------------------------------------------------------
# OBO auth tests — app/main.py wrappers
# ---------------------------------------------------------------------------


def _make_ctx(headers: dict | None = None) -> MagicMock:
    """Build a mock FastMCP Context with Starlette request headers."""
    ctx = MagicMock()
    ctx.request_context.request.headers = headers or {}
    return ctx


@patch.dict(os.environ, {"DATABRICKS_HOST": "https://test.cloud.databricks.com"})
@patch("app.main.WorkspaceClient")
def test_table_lineage_uses_obo_token(mock_ws_cls):
    from app.main import get_table_lineage

    ctx = _make_ctx({"x-forwarded-access-token": "user-oauth-token-123"})
    mock_ws_cls.return_value.api_client.do.return_value = {"upstreams": []}

    result = json.loads(asyncio.run(get_table_lineage("catalog.schema.table", ctx)))

    mock_ws_cls.assert_called_once_with(
        host="https://test.cloud.databricks.com",
        token="user-oauth-token-123",
    )
    assert "error" not in result


@patch.dict(os.environ, {"DATABRICKS_HOST": "https://test.cloud.databricks.com"})
@patch("app.main.WorkspaceClient")
def test_column_lineage_uses_obo_token(mock_ws_cls):
    from app.main import get_column_lineage

    ctx = _make_ctx({"x-forwarded-access-token": "user-oauth-token-456"})
    mock_ws_cls.return_value.api_client.do.return_value = {"upstream_cols": []}

    result = json.loads(asyncio.run(get_column_lineage("catalog.schema.table", "col1", ctx)))

    mock_ws_cls.assert_called_once_with(
        host="https://test.cloud.databricks.com",
        token="user-oauth-token-456",
    )
    assert "error" not in result


def test_table_lineage_missing_obo_header_returns_error():
    from app.main import get_table_lineage

    ctx = _make_ctx({})
    result = json.loads(asyncio.run(get_table_lineage("catalog.schema.table", ctx)))

    assert "error" in result
    assert "OBO token required" in result["error"]


def test_column_lineage_missing_obo_header_returns_error():
    from app.main import get_column_lineage

    ctx = _make_ctx({})
    result = json.loads(asyncio.run(get_column_lineage("catalog.schema.table", "col1", ctx)))

    assert "error" in result
    assert "OBO token required" in result["error"]


@patch("app.main.WorkspaceClient")
def test_missing_header_does_not_instantiate_workspace_client(mock_ws_cls):
    from app.main import get_table_lineage

    ctx = _make_ctx({})
    asyncio.run(get_table_lineage("catalog.schema.table", ctx))

    mock_ws_cls.assert_not_called()
