"""Tests for sync/hash.py — SHA-256 content hash for incremental ETL."""
from sync.types import ColumnInfo
from sync.hash import build_content_string, compute_content_hash


def test_hash_deterministic():
    cols = [ColumnInfo("id", "BIGINT"), ColumnInfo("name", "STRING", "user name")]
    h1 = compute_content_hash("main.schema.users", "Users table", cols)
    h2 = compute_content_hash("main.schema.users", "Users table", cols)
    assert h1 == h2


def test_hash_changes_on_comment_change():
    cols = [ColumnInfo("id", "BIGINT")]
    h1 = compute_content_hash("main.s.t", "comment A", cols)
    h2 = compute_content_hash("main.s.t", "comment B", cols)
    assert h1 != h2


def test_hash_changes_on_column_add():
    cols1 = [ColumnInfo("id", "BIGINT")]
    cols2 = [ColumnInfo("id", "BIGINT"), ColumnInfo("name", "STRING")]
    h1 = compute_content_hash("main.s.t", None, cols1)
    h2 = compute_content_hash("main.s.t", None, cols2)
    assert h1 != h2


def test_hash_changes_on_column_type_change():
    h1 = compute_content_hash("main.s.t", None, [ColumnInfo("id", "BIGINT")])
    h2 = compute_content_hash("main.s.t", None, [ColumnInfo("id", "STRING")])
    assert h1 != h2


def test_hash_changes_on_column_comment_change():
    h1 = compute_content_hash("main.s.t", None, [ColumnInfo("id", "BIGINT", "old")])
    h2 = compute_content_hash("main.s.t", None, [ColumnInfo("id", "BIGINT", "new")])
    assert h1 != h2


def test_hash_stable_on_column_order():
    cols_ab = [ColumnInfo("a", "INT"), ColumnInfo("b", "INT")]
    cols_ba = [ColumnInfo("b", "INT"), ColumnInfo("a", "INT")]
    assert compute_content_hash("main.s.t", None, cols_ab) == compute_content_hash("main.s.t", None, cols_ba)


def test_hash_handles_none_comment():
    cols = [ColumnInfo("id", "BIGINT", None)]
    result = compute_content_hash("main.s.t", None, cols)
    assert isinstance(result, str)


def test_hash_returns_hex_string():
    cols = [ColumnInfo("id", "BIGINT")]
    result = compute_content_hash("main.s.t", "test", cols)
    assert len(result) == 64
    assert all(c in "0123456789abcdef" for c in result)
