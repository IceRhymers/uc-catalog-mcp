"""Unit tests for app/tools/search.py."""

from unittest.mock import MagicMock


def _make_row(full_name="main.db.tbl", comment="A table", similarity=0.9):
    row = MagicMock()
    row.full_name = full_name
    row.comment = comment
    row.similarity = similarity
    return row


def test_search_calls_pgvector_ann():
    from app.tools.search import search_tables

    db = MagicMock()
    db.execute.return_value.fetchall.return_value = []
    embed_fn = MagicMock(return_value=[0.1] * 1024)

    search_tables("sales data", db=db, embed_fn=embed_fn)

    assert db.execute.called
    sql_arg = str(db.execute.call_args[0][0])
    assert "embedding <=> CAST(:vec AS vector)" in sql_arg
    assert "LIMIT :limit" in sql_arg


def test_search_embeds_query_first():
    from app.tools.search import search_tables

    db = MagicMock()
    db.execute.return_value.fetchall.return_value = []
    embed_fn = MagicMock(return_value=[0.1] * 1024)
    call_order = []
    embed_fn.side_effect = lambda t: (call_order.append("embed"), [0.1] * 1024)[1]
    db.execute.side_effect = lambda *a, **kw: (call_order.append("db"), db.execute.return_value)[1]

    search_tables("query", db=db, embed_fn=embed_fn)

    assert call_order[0] == "embed", "embed_fn must be called before db.execute"


def test_search_returns_ranked_results():
    from app.tools.search import search_tables

    db = MagicMock()
    db.execute.return_value.fetchall.return_value = [
        _make_row("a.b.t1", "Table one", 0.95),
        _make_row("a.b.t2", "Table two", 0.80),
    ]
    embed_fn = MagicMock(return_value=[0.1] * 1024)

    results = search_tables("query", db=db, embed_fn=embed_fn)

    assert len(results) == 2
    assert results[0]["full_name"] == "a.b.t1"
    assert results[0]["similarity"] == 0.95
    assert "comment" in results[0]


def test_search_default_limit_is_10():
    from app.tools.search import search_tables

    db = MagicMock()
    db.execute.return_value.fetchall.return_value = []
    embed_fn = MagicMock(return_value=[0.1] * 1024)

    search_tables("query", db=db, embed_fn=embed_fn)

    params = db.execute.call_args[0][1]
    assert params["limit"] == 10
