"""Lakebase database client for uc-catalog-mcp.

Provides create_lakebase_engine() for building a SQLAlchemy engine with
automatic OAuth token refresh.
"""

from __future__ import annotations

from sqlalchemy import Engine, create_engine, event


def create_lakebase_engine(instance_name: str, ws_client) -> Engine:
    """Build a SQLAlchemy engine for Lakebase with automatic token refresh.

    Resolves the instance host via the Databricks SDK
    (ws.database.get_database_instance), so no environment variables are needed
    for the host. Token is refreshed on every new connection via the do_connect
    event — credentials never go stale.
    """
    instance = ws_client.database.get_database_instance(instance_name)
    host = instance.read_write_dns
    me = ws_client.current_user.me()
    username = me.user_name

    url = f"postgresql+psycopg://{username}:@{host}:5432/databricks_postgres"
    engine = create_engine(url, connect_args={"sslmode": "require"})

    @event.listens_for(engine, "do_connect")
    def _refresh_token(dialect, conn_rec, cargs, cparams):
        cred = ws_client.database.generate_database_credential(instance_names=[instance_name])
        cparams["password"] = cred.token

    return engine
