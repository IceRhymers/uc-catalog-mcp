"""Tests for app/db/client.py — written RED-first before implementation."""

import os
from unittest.mock import MagicMock, patch

import pytest


def test_get_db_yields_session():
    """get_db() yields a SQLAlchemy Session from app.state.session_factory."""
    from app.db.client import get_db

    mock_session = MagicMock()
    mock_factory = MagicMock(return_value=mock_session)
    mock_request = MagicMock()
    mock_request.app.state.session_factory = mock_factory

    gen = get_db(mock_request)
    session = next(gen)
    assert session is mock_session


def test_get_db_closes_on_teardown():
    """Session is closed after the generator is exhausted."""
    from app.db.client import get_db

    mock_session = MagicMock()
    mock_request = MagicMock()
    mock_request.app.state.session_factory = MagicMock(return_value=mock_session)

    gen = get_db(mock_request)
    next(gen)
    try:
        next(gen)
    except StopIteration:
        pass

    mock_session.close.assert_called_once()


def test_create_engine_uses_env_vars():
    """create_lakebase_engine() reads LAKEBASE_HOST from env; raises if missing."""
    from app.db.client import create_lakebase_engine

    mock_ws = MagicMock()
    mock_ws.current_user.me.return_value = MagicMock(user_name="test@example.com")

    with pytest.raises(KeyError):
        # LAKEBASE_HOST not set — should raise
        env = {k: v for k, v in os.environ.items() if k != "LAKEBASE_HOST"}
        with patch.dict(os.environ, env, clear=True):
            create_lakebase_engine("test-instance", mock_ws)


def test_create_engine_returns_engine_with_host():
    """create_lakebase_engine() returns an engine when LAKEBASE_HOST is set."""
    from sqlalchemy import Engine

    from app.db.client import create_lakebase_engine

    mock_ws = MagicMock()
    mock_ws.current_user.me.return_value = MagicMock(user_name="test@example.com")

    with patch.dict(os.environ, {"LAKEBASE_HOST": "test-host.db.databricks.com"}):
        engine = create_lakebase_engine("test-instance", mock_ws)
    assert isinstance(engine, Engine)
    engine.dispose()


def test_no_fstring_sql_in_module():
    """No f-string SQL patterns in app/db/client.py."""
    from pathlib import Path

    source = Path("app/db/client.py").read_text()
    # f-strings used for the connection URL are OK; check for f-string SQL queries
    sql_keywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP"]
    for line in source.splitlines():
        if line.strip().startswith("#"):
            continue
        if line.startswith('    url = f"postgresql'):
            continue  # connection URL f-string is expected
        if any(kw in line.upper() for kw in sql_keywords):
            assert 'f"' not in line and "f'" not in line, f"f-string SQL found: {line.strip()}"
