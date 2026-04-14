"""Lakebase database client for uc-catalog-mcp.

Provides create_lakebase_engine() for building a SQLAlchemy engine with
automatic OAuth token refresh, and get_db() as a FastAPI dependency that
yields a SQLAlchemy Session.
"""

from __future__ import annotations

import os
from collections.abc import Generator

from fastapi import Request
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session


def create_lakebase_engine(instance_name: str, ws_client) -> Engine:
    """Build a SQLAlchemy engine for Lakebase with automatic token refresh.

    Uses the Databricks SDK to generate a fresh database credential on every
    new connection via the do_connect event, so tokens never go stale.
    """
    host = os.environ["LAKEBASE_HOST"]
    database = os.environ.get("LAKEBASE_DATABASE", "databricks_postgres")
    me = ws_client.current_user.me()
    username = me.user_name

    url = f"postgresql+psycopg://{username}:@{host}:5432/{database}"
    engine = create_engine(url, connect_args={"sslmode": "require"})

    @event.listens_for(engine, "do_connect")
    def _refresh_token(dialect, conn_rec, cargs, cparams):
        cred = ws_client.database.generate_database_credential(instance_names=[instance_name])
        cparams["password"] = cred.token

    return engine


def get_db(request: Request) -> Generator[Session, None, None]:
    """FastAPI dependency: yield a SQLAlchemy session, closing on teardown.

    Requires app.state.session_factory to be set during startup.
    """
    session = request.app.state.session_factory()
    try:
        yield session
    finally:
        session.close()
