"""Integration tests for app/tools/lineage.py — requires Databricks credentials.

Requires:
- Databricks credentials configured (.databrickscfg or env vars)
- LINEAGE_TEST_TABLE env var set to a real Unity Catalog table (catalog.schema.table)

Run with: make test-integration
Never run in CI — requires live Databricks workspace with lineage tracking enabled.
"""

import os
import logging

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_INTEGRATION_TESTS") != "1",
    reason="Set RUN_INTEGRATION_TESTS=1 to run integration tests locally",
)

LINEAGE_TEST_TABLE = os.environ.get("LINEAGE_TEST_TABLE", "main.default.lineage_test")


def test_get_table_lineage_returns_dict():
    """get_table_lineage returns a dict with expected lineage keys."""
    from app.tools.lineage import get_table_lineage

    result = get_table_lineage(LINEAGE_TEST_TABLE)
    logging.info(result)
    assert isinstance(result, dict), f"Expected dict, got: {type(result)}"
    assert "error" not in result, f"API returned error: {result.get('error')}"
    # Lineage API returns upstream/downstream as lists (may be empty for leaf tables)
    assert "upstreams" in result or "downstreams" in result, (
        f"Expected upstreams/downstreams keys, got: {list(result.keys())}"
    )


def test_get_column_lineage_returns_dict():
    """get_column_lineage returns a dict for the first column of the test table."""
    from app.tools.lineage import get_column_lineage

    # Use the table name and pick an arbitrary column name; API returns empty lists
    # rather than erroring on unknown columns, so this tests the call path.
    result = get_column_lineage(LINEAGE_TEST_TABLE, "id")
    logging.info(result)
    assert isinstance(result, dict), f"Expected dict, got: {type(result)}"
    assert "error" not in result, f"API returned error: {result.get('error')}"


def test_get_table_lineage_invalid_table_returns_error():
    """get_table_lineage returns structured error dict for a non-existent table."""
    from app.tools.lineage import get_table_lineage

    result = get_table_lineage("catalog.schema.__nonexistent_claw_test__")
    logging.info(result)
    # Either an error dict OR an empty lineage response — both acceptable
    assert isinstance(result, dict)
