"""Unit tests for sync/config.py — allowlist parsing and SQL filter generation."""

import pytest


def test_parse_allowlist_catalog_only():
    from sync.config import parse_allowlist, AllowlistEntry

    entries = parse_allowlist(["main"])
    assert entries == [AllowlistEntry(catalog="main", schema_pattern=None)]


def test_parse_allowlist_catalog_and_schema():
    from sync.config import parse_allowlist, AllowlistEntry

    entries = parse_allowlist(["analytics.prod_*"])
    assert entries == [AllowlistEntry(catalog="analytics", schema_pattern="prod_*")]


def test_parse_allowlist_multiple():
    from sync.config import parse_allowlist, AllowlistEntry

    entries = parse_allowlist(["main", "analytics.prod_*", "dev.test_schema"])
    assert entries == [
        AllowlistEntry(catalog="main", schema_pattern=None),
        AllowlistEntry(catalog="analytics", schema_pattern="prod_*"),
        AllowlistEntry(catalog="dev", schema_pattern="test_schema"),
    ]


def test_parse_allowlist_empty():
    from sync.config import parse_allowlist

    assert parse_allowlist([]) == []


def test_allowlist_generates_sql_filter_catalog_only():
    from sync.config import parse_allowlist, build_system_table_filter

    entries = parse_allowlist(["main"])
    sql, params = build_system_table_filter(entries)

    assert "table_catalog" in sql
    assert "main" in params


def test_allowlist_generates_sql_filter_catalog_and_schema():
    from sync.config import parse_allowlist, build_system_table_filter

    entries = parse_allowlist(["analytics.prod_*"])
    sql, params = build_system_table_filter(entries)

    # Should generate a LIKE clause for schema pattern
    assert "table_schema" in sql or "schema" in sql.lower()
    assert any("prod_" in str(p) for p in params)


def test_allowlist_generates_sql_filter_parameterized():
    """Filter must use parameters (no string interpolation) to prevent SQL injection."""
    from sync.config import parse_allowlist, build_system_table_filter

    entries = parse_allowlist(["main", "analytics.prod_*"])
    sql, params = build_system_table_filter(entries)

    # SQL should not literally contain catalog/schema names — they go in params
    assert isinstance(params, list)
    assert len(params) > 0
    # The SQL should contain placeholders
    assert "%" in sql or "?" in sql


def test_allowlist_entry_schema_pattern_none_by_default():
    from sync.config import AllowlistEntry

    e = AllowlistEntry(catalog="main")
    assert e.schema_pattern is None


def test_build_system_table_filter_empty_raises():
    """Empty allowlist should raise — we must not scan all catalogs."""
    from sync.config import build_system_table_filter

    with pytest.raises((ValueError, AssertionError)):
        build_system_table_filter([])
