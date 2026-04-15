"""Unit tests for app/tools/describe.py."""

from unittest.mock import MagicMock


def _make_orm_row(full_name="main.db.tbl", comment="My table", columns=None):
    row = MagicMock()
    row.full_name = full_name
    row.comment = comment
    row.columns = columns or [{"name": "id", "type": "BIGINT", "comment": None}]
    return row


def test_describe_returns_full_schema():
    from app.tools.describe import describe_table

    db = MagicMock()
    db.get.return_value = _make_orm_row()

    result = describe_table("main.db.tbl", db=db)

    assert result["full_name"] == "main.db.tbl"
    assert result["comment"] == "My table"
    assert isinstance(result["columns"], list)


def test_describe_columns_deserialized():
    from app.tools.describe import describe_table

    db = MagicMock()
    db.get.return_value = _make_orm_row(
        columns=[{"name": "id", "type": "BIGINT", "comment": "primary key"}]
    )

    result = describe_table("main.db.tbl", db=db)

    assert len(result["columns"]) == 1
    col = result["columns"][0]
    assert col["name"] == "id"
    assert col["type"] == "BIGINT"
    assert col["comment"] == "primary key"


def test_describe_not_found_returns_error():
    from app.tools.describe import describe_table

    db = MagicMock()
    db.get.return_value = None

    result = describe_table("main.db.missing", db=db)

    assert "error" in result
    assert "main.db.missing" in result["error"]
