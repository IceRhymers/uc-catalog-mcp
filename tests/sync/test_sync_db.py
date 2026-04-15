"""Unit tests for sync/db.py — Lakebase helpers with Databricks SDK credentials."""

from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# test_get_lakebase_jdbc_url
# ---------------------------------------------------------------------------


def _make_ws(host="lakebase.host.example", username="user@example.com", token="tok123"):
    ws = MagicMock()
    ws.database.get_database_instance.return_value.read_write_dns = host
    ws.current_user.me.return_value.user_name = username
    ws.database.generate_database_credential.return_value.token = token
    return ws


def test_get_lakebase_jdbc_url_returns_url_and_props():
    from sync.db import get_lakebase_jdbc_url

    ws = _make_ws(
        host="myhost.lakebase.azure.databricks.com", username="alice@acme.com", token="tok-abc"
    )
    url, props = get_lakebase_jdbc_url("my_instance", ws)

    assert "myhost.lakebase.azure.databricks.com" in url
    assert url.startswith("jdbc:postgresql://")
    assert "sslmode=require" in url
    assert props["user"] == "alice@acme.com"
    assert props["password"] == "tok-abc"
    assert props["driver"] == "org.postgresql.Driver"


def test_get_lakebase_jdbc_url_uses_sdk_for_credentials():
    from sync.db import get_lakebase_jdbc_url

    ws = _make_ws()
    get_lakebase_jdbc_url("my_instance", ws)

    ws.database.get_database_instance.assert_called_once_with("my_instance")
    ws.database.generate_database_credential.assert_called_once_with(instance_names=["my_instance"])


def test_get_lakebase_jdbc_url_no_env_vars_needed(monkeypatch):
    """No LAKEBASE_* env vars should be required — SDK provides everything."""
    from sync.db import get_lakebase_jdbc_url

    for var in [
        "LAKEBASE_HOST",
        "LAKEBASE_PORT",
        "LAKEBASE_DB",
        "LAKEBASE_USER",
        "LAKEBASE_PASSWORD",
    ]:
        monkeypatch.delenv(var, raising=False)

    ws = _make_ws()
    url, props = get_lakebase_jdbc_url("inst", ws)
    assert url  # just must not raise


# ---------------------------------------------------------------------------
# test_fetch_existing_hashes
# ---------------------------------------------------------------------------


def test_fetch_existing_hashes_returns_dict():
    from sync.db import fetch_existing_hashes

    row1 = MagicMock()
    row1.__getitem__ = lambda self, k: {"full_name": "main.db.tbl1", "content_hash": "aaa"}[k]
    row2 = MagicMock()
    row2.__getitem__ = lambda self, k: {"full_name": "main.db.tbl2", "content_hash": "bbb"}[k]

    mock_df = MagicMock()
    mock_df.select.return_value.collect.return_value = [row1, row2]

    spark = MagicMock()
    spark.read.jdbc.return_value = mock_df

    result = fetch_existing_hashes(spark, "jdbc:postgresql://host/db", {"user": "u"})

    assert result == {"main.db.tbl1": "aaa", "main.db.tbl2": "bbb"}
    spark.read.jdbc.assert_called_once_with(
        url="jdbc:postgresql://host/db",
        table="catalog_metadata",
        properties={"user": "u"},
    )


def test_fetch_existing_hashes_empty_table():
    from sync.db import fetch_existing_hashes

    mock_df = MagicMock()
    mock_df.select.return_value.collect.return_value = []

    spark = MagicMock()
    spark.read.jdbc.return_value = mock_df

    result = fetch_existing_hashes(spark, "jdbc:postgresql://host/db", {})
    assert result == {}


# ---------------------------------------------------------------------------
# test_upsert_partition
# ---------------------------------------------------------------------------


def test_upsert_partition_calls_on_conflict():
    """SQL must contain ON CONFLICT (full_name) DO UPDATE SET."""
    from sync.db import upsert_partition

    rows = [
        {
            "full_name": "main.db.tbl",
            "content": "text",
            "embedding": [0.1, 0.2],
            "content_hash": "abc",
        },
    ]

    mock_ws = _make_ws(host="host.example", username="user@x.com", token="tok")
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
        upsert_partition(iter(rows), "my_instance")

    assert mock_cur.execute.called
    sql_called = mock_cur.execute.call_args[0][0]
    assert "ON CONFLICT (full_name) DO UPDATE SET" in sql_called


def test_upsert_partition_commits():
    from sync.db import upsert_partition

    rows = [
        {"full_name": "a.b.c", "content": "x", "embedding": [], "content_hash": "h"},
    ]
    mock_ws = _make_ws()
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

    mock_conn.commit.assert_called_once()


def test_upsert_partition_refreshes_credentials_per_call():
    """Each partition call must fetch fresh credentials via generate_database_credential."""
    from sync.db import upsert_partition

    mock_ws = _make_ws()
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
        upsert_partition(iter([]), "inst")

    mock_ws.database.generate_database_credential.assert_called_once_with(instance_names=["inst"])
