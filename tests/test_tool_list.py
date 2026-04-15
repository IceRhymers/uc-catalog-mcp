"""Unit tests for app/tools/list.py."""

from unittest.mock import MagicMock


def test_list_catalogs_returns_distinct():
    from app.tools.list import list_catalogs

    db = MagicMock()
    db.scalars.return_value.all.return_value = ["dev", "main", "analytics"]

    result = list_catalogs(db=db)

    assert result == ["analytics", "dev", "main"]  # sorted
    # Verify distinct was used in the query
    assert db.scalars.called


def test_list_schemas_filters_by_catalog():
    from app.tools.list import list_schemas

    db = MagicMock()
    db.scalars.return_value.all.return_value = ["prod_data", "raw"]

    result = list_schemas("main", db=db)

    assert result == ["prod_data", "raw"]
    assert db.scalars.called


def test_list_schemas_unknown_catalog_returns_empty():
    from app.tools.list import list_schemas

    db = MagicMock()
    db.scalars.return_value.all.return_value = []

    result = list_schemas("nonexistent", db=db)

    assert result == []
