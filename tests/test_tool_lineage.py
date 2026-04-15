"""Unit tests for app/tools/lineage.py."""

from unittest.mock import MagicMock


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
        "POST",
        "/api/2.0/lineage-tracking/table-lineage",
        body={"table_name": "catalog.schema.table"},
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
        "POST",
        "/api/2.0/lineage-tracking/column-lineage",
        body={"table_name": "catalog.schema.table", "column_name": "id"},
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
