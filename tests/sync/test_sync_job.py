"""Unit tests for sync/job.py — ETL orchestrator.

All mocks: Spark DataFrames (MagicMock), psycopg, WorkspaceClient, embed_dataframe.
No real Spark or DB needed.
"""

import json
import logging
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_table_row(catalog, schema, table, comment=None):
    row = MagicMock()
    row.table_catalog = catalog
    row.table_schema = schema
    row.table_name = table
    row.comment = comment
    return row


def _make_col_row(catalog, schema, table, col_name, col_type, col_comment=None):
    row = MagicMock()
    row.table_catalog = catalog
    row.table_schema = schema
    row.table_name = table
    row.column_name = col_name
    row.data_type = col_type
    row.comment = col_comment
    return row


def _make_spark_with_tables(table_rows, col_rows):
    """Return a mocked SparkSession that returns table_rows / col_rows from spark.sql()."""
    spark = MagicMock()
    tables_df = MagicMock()
    tables_df.collect.return_value = table_rows
    cols_df = MagicMock()
    cols_df.collect.return_value = col_rows

    call_count = [0]

    def sql_side_effect(query, *args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return tables_df
        return cols_df

    spark.sql.side_effect = sql_side_effect
    return spark


# ---------------------------------------------------------------------------
# test_skips_unchanged_tables
# ---------------------------------------------------------------------------


def test_skips_unchanged_tables(monkeypatch):
    """Tables with matching content_hash must NOT reach embed_dataframe."""
    from sync.hash import compute_content_hash
    from sync.types import ColumnInfo

    col = ColumnInfo(name="id", type="BIGINT")
    existing_hash = compute_content_hash("main.db.tbl", None, [col])

    table_row = _make_table_row("main", "db", "tbl")
    col_row = _make_col_row("main", "db", "tbl", "id", "BIGINT")
    spark = _make_spark_with_tables([table_row], [col_row])

    mock_ws = MagicMock()
    mock_ws.database.get_database_instance.return_value.read_write_dns = "host"
    mock_ws.current_user.me.return_value.user_name = "user"
    mock_ws.database.generate_database_credential.return_value.token = "tok"

    existing_hashes_df = MagicMock()
    row = MagicMock()
    row.__getitem__ = lambda s, k: {"full_name": "main.db.tbl", "content_hash": existing_hash}[k]
    existing_hashes_df.select.return_value.collect.return_value = [row]
    spark.read.jdbc.return_value = existing_hashes_df

    embed_mock = MagicMock(side_effect=lambda df, col: df)

    monkeypatch.setenv("CATALOG_ALLOWLIST", json.dumps(["main"]))
    monkeypatch.setenv("LAKEBASE_INSTANCE_NAME", "inst")

    with (
        patch("sync.job.create_spark_session", return_value=spark),
        patch("sync.job.WorkspaceClient", return_value=mock_ws),
        patch("sync.job.embed_dataframe", embed_mock),
        patch("sync.job.get_lakebase_jdbc_url", return_value=("jdbc:...", {})),
        patch("sync.job.fetch_existing_hashes", return_value={"main.db.tbl": existing_hash}),
    ):
        from sync import job
        import importlib

        importlib.reload(job)
        with (
            patch("sync.job.create_spark_session", return_value=spark),
            patch("sync.job.WorkspaceClient", return_value=mock_ws),
            patch("sync.job.embed_dataframe", embed_mock),
            patch("sync.job.get_lakebase_jdbc_url", return_value=("jdbc:...", {})),
            patch("sync.job.fetch_existing_hashes", return_value={"main.db.tbl": existing_hash}),
        ):
            stats = job.run_sync()

    assert stats["skipped"] == 1
    assert stats["embedded"] == 0
    embed_mock.assert_not_called()


# ---------------------------------------------------------------------------
# test_embeds_new_tables
# ---------------------------------------------------------------------------


def test_embeds_new_tables(monkeypatch):
    """Tables absent from Lakebase must be embedded and inserted."""
    table_row = _make_table_row("main", "db", "new_tbl")
    col_row = _make_col_row("main", "db", "new_tbl", "id", "BIGINT")
    spark = _make_spark_with_tables([table_row], [col_row])
    spark.createDataFrame.return_value = MagicMock()

    mock_ws = MagicMock()
    mock_ws.database.get_database_instance.return_value.read_write_dns = "host"
    mock_ws.current_user.me.return_value.user_name = "user"
    mock_ws.database.generate_database_credential.return_value.token = "tok"

    embedded_df = MagicMock()
    embedded_df.foreachPartition = MagicMock()
    embed_mock = MagicMock(return_value=embedded_df)

    monkeypatch.setenv("CATALOG_ALLOWLIST", json.dumps(["main"]))
    monkeypatch.setenv("LAKEBASE_INSTANCE_NAME", "inst")

    with (
        patch("sync.job.create_spark_session", return_value=spark),
        patch("sync.job.WorkspaceClient", return_value=mock_ws),
        patch("sync.job.embed_dataframe", embed_mock),
        patch("sync.job.get_lakebase_jdbc_url", return_value=("jdbc:...", {})),
        patch("sync.job.fetch_existing_hashes", return_value={}),
    ):
        from sync import job
        import importlib

        importlib.reload(job)
        with (
            patch("sync.job.create_spark_session", return_value=spark),
            patch("sync.job.WorkspaceClient", return_value=mock_ws),
            patch("sync.job.embed_dataframe", embed_mock),
            patch("sync.job.get_lakebase_jdbc_url", return_value=("jdbc:...", {})),
            patch("sync.job.fetch_existing_hashes", return_value={}),
        ):
            stats = job.run_sync()

    assert stats["embedded"] >= 1
    embed_mock.assert_called_once()


# ---------------------------------------------------------------------------
# test_embeds_changed_tables
# ---------------------------------------------------------------------------


def test_embeds_changed_tables(monkeypatch):
    """Tables with hash mismatch must be embedded and upserted."""
    table_row = _make_table_row("main", "db", "tbl")
    col_row = _make_col_row("main", "db", "tbl", "id", "BIGINT")
    spark = _make_spark_with_tables([table_row], [col_row])
    spark.createDataFrame.return_value = MagicMock()

    mock_ws = MagicMock()
    mock_ws.database.get_database_instance.return_value.read_write_dns = "host"
    mock_ws.current_user.me.return_value.user_name = "user"
    mock_ws.database.generate_database_credential.return_value.token = "tok"

    embedded_df = MagicMock()
    embedded_df.foreachPartition = MagicMock()
    embed_mock = MagicMock(return_value=embedded_df)

    monkeypatch.setenv("CATALOG_ALLOWLIST", json.dumps(["main"]))
    monkeypatch.setenv("LAKEBASE_INSTANCE_NAME", "inst")

    with (
        patch("sync.job.create_spark_session", return_value=spark),
        patch("sync.job.WorkspaceClient", return_value=mock_ws),
        patch("sync.job.embed_dataframe", embed_mock),
        patch("sync.job.get_lakebase_jdbc_url", return_value=("jdbc:...", {})),
        patch("sync.job.fetch_existing_hashes", return_value={"main.db.tbl": "old_stale_hash"}),
    ):
        from sync import job
        import importlib

        importlib.reload(job)
        with (
            patch("sync.job.create_spark_session", return_value=spark),
            patch("sync.job.WorkspaceClient", return_value=mock_ws),
            patch("sync.job.embed_dataframe", embed_mock),
            patch("sync.job.get_lakebase_jdbc_url", return_value=("jdbc:...", {})),
            patch("sync.job.fetch_existing_hashes", return_value={"main.db.tbl": "old_stale_hash"}),
        ):
            stats = job.run_sync()

    assert stats["embedded"] >= 1
    embed_mock.assert_called_once()


# ---------------------------------------------------------------------------
# test_deletes_removed_tables
# ---------------------------------------------------------------------------


def test_deletes_removed_tables(monkeypatch):
    """Tables in Lakebase but absent from UC system tables must be deleted."""
    # No tables in UC
    spark = _make_spark_with_tables([], [])

    mock_ws = MagicMock()
    mock_ws.database.get_database_instance.return_value.read_write_dns = "host"
    mock_ws.current_user.me.return_value.user_name = "user"
    mock_ws.database.generate_database_credential.return_value.token = "tok"

    embed_mock = MagicMock()

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_cur.__enter__ = MagicMock(return_value=mock_cur)
    mock_cur.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cur

    monkeypatch.setenv("CATALOG_ALLOWLIST", json.dumps(["main"]))
    monkeypatch.setenv("LAKEBASE_INSTANCE_NAME", "inst")

    with (
        patch("sync.job.create_spark_session", return_value=spark),
        patch("sync.job.WorkspaceClient", return_value=mock_ws),
        patch("sync.db.WorkspaceClient", return_value=mock_ws),
        patch("sync.job.embed_dataframe", embed_mock),
        patch("sync.job.get_lakebase_jdbc_url", return_value=("jdbc:...", {})),
        patch("sync.job.fetch_existing_hashes", return_value={"main.db.removed": "hash123"}),
        patch("sync.db.psycopg.connect", return_value=mock_conn),
    ):
        from sync import job
        import importlib

        importlib.reload(job)
        with (
            patch("sync.job.create_spark_session", return_value=spark),
            patch("sync.job.WorkspaceClient", return_value=mock_ws),
            patch("sync.db.WorkspaceClient", return_value=mock_ws),
            patch("sync.job.embed_dataframe", embed_mock),
            patch("sync.job.get_lakebase_jdbc_url", return_value=("jdbc:...", {})),
            patch("sync.job.fetch_existing_hashes", return_value={"main.db.removed": "hash123"}),
            patch("sync.db.psycopg.connect", return_value=mock_conn),
        ):
            stats = job.run_sync()

    assert stats["deleted"] == 1


# ---------------------------------------------------------------------------
# test_upsert_uses_on_conflict
# ---------------------------------------------------------------------------


def test_upsert_uses_on_conflict(monkeypatch):
    """upsert_partition SQL must contain ON CONFLICT (full_name) DO UPDATE SET."""
    from sync.db import upsert_partition

    rows = [
        {"full_name": "a.b.c", "content": "x", "embedding": [0.1], "content_hash": "abc"},
    ]
    mock_ws = MagicMock()
    mock_ws.database.get_database_instance.return_value.read_write_dns = "host"
    mock_ws.current_user.me.return_value.user_name = "user"
    mock_ws.database.generate_database_credential.return_value.token = "tok"

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_cur.__enter__ = MagicMock(return_value=mock_cur)
    mock_cur.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cur

    with (
        patch("sync.db.WorkspaceClient", return_value=mock_ws),
        patch("sync.db.psycopg.connect", return_value=mock_conn),
    ):
        upsert_partition(iter(rows), "inst")

    sql = mock_cur.execute.call_args[0][0]
    assert "ON CONFLICT (full_name) DO UPDATE SET" in sql


# ---------------------------------------------------------------------------
# test_full_sync_logs_stats
# ---------------------------------------------------------------------------


def test_full_sync_logs_stats(monkeypatch, caplog):
    """run_sync() must log: Sync complete: scanned=N skipped=N embedded=N deleted=N"""
    spark = _make_spark_with_tables([], [])

    mock_ws = MagicMock()
    mock_ws.database.get_database_instance.return_value.read_write_dns = "host"
    mock_ws.current_user.me.return_value.user_name = "user"
    mock_ws.database.generate_database_credential.return_value.token = "tok"

    monkeypatch.setenv("CATALOG_ALLOWLIST", json.dumps(["main"]))
    monkeypatch.setenv("LAKEBASE_INSTANCE_NAME", "inst")

    with (
        patch("sync.job.create_spark_session", return_value=spark),
        patch("sync.job.WorkspaceClient", return_value=mock_ws),
        patch("sync.job.embed_dataframe", MagicMock()),
        patch("sync.job.get_lakebase_jdbc_url", return_value=("jdbc:...", {})),
        patch("sync.job.fetch_existing_hashes", return_value={}),
    ):
        from sync import job
        import importlib

        importlib.reload(job)
        with (
            patch("sync.job.create_spark_session", return_value=spark),
            patch("sync.job.WorkspaceClient", return_value=mock_ws),
            patch("sync.job.embed_dataframe", MagicMock()),
            patch("sync.job.get_lakebase_jdbc_url", return_value=("jdbc:...", {})),
            patch("sync.job.fetch_existing_hashes", return_value={}),
        ):
            with caplog.at_level(logging.INFO):
                job.run_sync()

    log_text = caplog.text
    assert "Sync complete" in log_text
    assert "scanned=" in log_text
    assert "skipped=" in log_text
    assert "embedded=" in log_text
    assert "deleted=" in log_text
