"""Lakebase helpers for the sync pipeline.

Provides credential resolution via Databricks SDK, a JDBC URL builder for
spark.read.jdbc(), and a psycopg upsert function for foreachPartition writes.

No imports from app/ — boundary rule enforced.
"""

from __future__ import annotations

import psycopg
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession


def get_lakebase_jdbc_url(instance_name: str, ws: WorkspaceClient) -> tuple[str, dict]:
    """Return (jdbc_url, connection_properties) for spark.read.jdbc().

    Resolves host via ws.database.get_database_instance() and generates a
    short-lived OAuth token via ws.database.generate_database_credential().
    No static env vars needed for host or password.

    Args:
        instance_name: Lakebase instance name (e.g. "my_lakebase").
        ws: Authenticated Databricks WorkspaceClient.

    Returns:
        Tuple of (jdbc_url, props_dict) ready for spark.read.jdbc().
    """
    instance = ws.database.get_database_instance(instance_name)
    host = instance.read_write_dns
    me = ws.current_user.me()
    username = me.user_name
    cred = ws.database.generate_database_credential(instance_names=[instance_name])
    url = f"jdbc:postgresql://{host}:5432/databricks_postgres?sslmode=require"
    props = {"user": username, "password": cred.token, "driver": "org.postgresql.Driver"}
    return url, props


def fetch_existing_hashes(spark: SparkSession, jdbc_url: str, props: dict) -> dict[str, str]:
    """Fetch {full_name: content_hash} from Lakebase in one JDBC read.

    Args:
        spark: Active SparkSession.
        jdbc_url: JDBC connection URL (from get_lakebase_jdbc_url).
        props: Connection properties dict (from get_lakebase_jdbc_url).

    Returns:
        Dict mapping full_name → content_hash for all rows in catalog_metadata.
    """
    df = spark.read.jdbc(url=jdbc_url, table="catalog_metadata", properties=props)
    return {
        row["full_name"]: row["content_hash"]
        for row in df.select("full_name", "content_hash").collect()
    }


def upsert_partition(rows, instance_name: str) -> None:
    """psycopg upsert for use with DataFrame.foreachPartition().

    Credentials are refreshed per partition call — tokens never go stale
    across a long job run. Each partition opens one connection and commits
    all rows atomically.

    Args:
        rows: Iterator of Row objects with keys: full_name, content, embedding, content_hash.
        instance_name: Lakebase instance name for credential resolution.
    """
    ws = WorkspaceClient()
    instance = ws.database.get_database_instance(instance_name)
    host = instance.read_write_dns
    me = ws.current_user.me()
    username = me.user_name
    cred = ws.database.generate_database_credential(instance_names=[instance_name])

    with psycopg.connect(
        host=host,
        port=5432,
        dbname="databricks_postgres",
        user=username,
        password=cred.token,
        sslmode="require",
    ) as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO catalog_metadata
                        (full_name, catalog, schema_name, table_name,
                         table_type, comment, columns, content_hash, embedding)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (full_name) DO UPDATE SET
                        catalog = EXCLUDED.catalog,
                        schema_name = EXCLUDED.schema_name,
                        table_name = EXCLUDED.table_name,
                        table_type = EXCLUDED.table_type,
                        comment = EXCLUDED.comment,
                        columns = EXCLUDED.columns,
                        content_hash = EXCLUDED.content_hash,
                        embedding = EXCLUDED.embedding,
                        synced_at = now()
                    """,
                    (
                        row["full_name"],
                        row["catalog"],
                        row["schema_name"],
                        row["table_name"],
                        row["table_type"],
                        row["comment"],
                        row["columns"],
                        row["content_hash"],
                        row["embedding"],
                    ),
                )
        conn.commit()


def delete_tables(instance_name: str, full_names: list[str]) -> None:
    """Delete rows from catalog_metadata for removed tables.

    Args:
        instance_name: Lakebase instance name for credential resolution.
        full_names: List of full_name values to delete.
    """
    if not full_names:
        return

    ws = WorkspaceClient()
    instance = ws.database.get_database_instance(instance_name)
    host = instance.read_write_dns
    me = ws.current_user.me()
    username = me.user_name
    cred = ws.database.generate_database_credential(instance_names=[instance_name])

    with psycopg.connect(
        host=host,
        port=5432,
        dbname="databricks_postgres",
        user=username,
        password=cred.token,
        sslmode="require",
    ) as conn:
        with conn.cursor() as cur:
            for full_name in full_names:
                cur.execute("DELETE FROM catalog_metadata WHERE full_name = %s", (full_name,))
        conn.commit()
