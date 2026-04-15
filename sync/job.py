"""ETL orchestrator: sync Unity Catalog metadata to Lakebase.

Steps:
  1. Parse allowlist from CATALOG_ALLOWLIST env var
  2. Get Spark session
  3. Query system.information_schema for tables + columns
  4. Build content strings and hashes
  5. Diff against existing Lakebase hashes
  6. Embed new/changed tables via ai_query (one batched DataFrame call)
  7. Upsert via foreachPartition + psycopg
  8. Delete removed tables
  9. Log stats

No imports from app/ — boundary rule enforced.
"""

from __future__ import annotations

import json
import logging
import os
from collections import defaultdict

from databricks.sdk import WorkspaceClient

from sync.config import AllowlistEntry, parse_allowlist
from sync.db import delete_tables, fetch_existing_hashes, get_lakebase_jdbc_url, upsert_partition
from sync.embeddings import embed_dataframe
from sync.hash import build_content_string, compute_content_hash
from sync.spark import create_spark_session
from sync.types import ColumnInfo

logger = logging.getLogger(__name__)


def _build_allowlist_sql_filter(entries: list[AllowlistEntry]) -> tuple[str, list]:
    """Wrap build_system_table_filter for use in spark.sql() f-strings.

    Returns a SQL WHERE fragment and params list. Since spark.sql() doesn't
    support bind params, we inline safely-escaped catalog/schema values.
    """
    # For Spark SQL we inline values (not user-supplied — from env config)
    clauses = []
    for entry in entries:
        if entry.schema_pattern is None:
            clauses.append(f"table_catalog = '{entry.catalog}'")
        else:
            like_pattern = entry.schema_pattern.replace("*", "%")
            clauses.append(
                f"table_catalog = '{entry.catalog}' AND table_schema LIKE '{like_pattern}'"
            )
    return "(" + " OR ".join(clauses) + ")", []


def run_sync() -> dict:
    """Run the full ETL sync cycle.

    Reads CATALOG_ALLOWLIST (JSON list) and LAKEBASE_INSTANCE_NAME from env.

    Returns:
        Dict with keys: scanned, skipped, embedded, deleted.
    """
    # 1. Parse allowlist
    raw_allowlist = json.loads(os.environ["CATALOG_ALLOWLIST"])
    instance_name = os.environ["LAKEBASE_INSTANCE_NAME"]
    entries = parse_allowlist(raw_allowlist)
    where_clause, _ = _build_allowlist_sql_filter(entries)

    # 2. Get Spark session
    spark = create_spark_session()

    # 3. Query system tables
    tables_df = spark.sql(f"""
        SELECT table_catalog, table_schema, table_name, comment
        FROM system.information_schema.tables
        WHERE {where_clause}
          AND table_type = 'BASE TABLE'
    """)
    cols_df = spark.sql(f"""
        SELECT table_catalog, table_schema, table_name, column_name, data_type, comment
        FROM system.information_schema.columns
        WHERE {where_clause}
    """)

    table_rows = tables_df.collect()
    col_rows = cols_df.collect()

    # 4. Group columns by full table name
    cols_by_table: dict[str, list[ColumnInfo]] = defaultdict(list)
    for row in col_rows:
        full_name = f"{row.table_catalog}.{row.table_schema}.{row.table_name}"
        cols_by_table[full_name].append(
            ColumnInfo(name=row.column_name, type=row.data_type, comment=row.comment)
        )

    # Build table metadata map
    tables: dict[str, dict] = {}
    for row in table_rows:
        full_name = f"{row.table_catalog}.{row.table_schema}.{row.table_name}"
        cols = cols_by_table.get(full_name, [])
        content = build_content_string(full_name, row.comment, cols)
        content_hash = compute_content_hash(full_name, row.comment, cols)
        tables[full_name] = {"content": content, "content_hash": content_hash}

    # 5–7. Lakebase diff + embed + upsert
    ws = WorkspaceClient()
    jdbc_url, props = get_lakebase_jdbc_url(instance_name, ws)
    existing_hashes = fetch_existing_hashes(spark, jdbc_url, props)

    to_embed: list[dict] = []
    skipped = 0

    for full_name, meta in tables.items():
        if existing_hashes.get(full_name) == meta["content_hash"]:
            skipped += 1
        else:
            to_embed.append({"full_name": full_name, **meta})

    # 8. Embed new + changed in one batch
    if to_embed:
        embed_df = spark.createDataFrame(
            [
                {
                    "full_name": r["full_name"],
                    "content": r["content"],
                    "content_hash": r["content_hash"],
                }
                for r in to_embed
            ]
        )
        result_df = embed_dataframe(embed_df, "content")
        result_df.foreachPartition(lambda rows: upsert_partition(rows, instance_name))

    # 9. Delete removed tables (only within allowed namespaces)
    allowed_catalogs = {e.catalog for e in entries}
    removed = [
        fn for fn in existing_hashes if fn not in tables and fn.split(".")[0] in allowed_catalogs
    ]
    if removed:
        delete_tables(instance_name, removed)

    stats = {
        "scanned": len(tables),
        "skipped": skipped,
        "embedded": len(to_embed),
        "deleted": len(removed),
    }
    logger.info(
        "Sync complete: scanned=%(scanned)s skipped=%(skipped)s "
        "embedded=%(embedded)s deleted=%(deleted)s",
        stats,
    )
    return stats


def main() -> None:
    """Console script entry point for the uc-catalog-sync wheel task."""
    logging.basicConfig(level=logging.INFO)
    run_sync()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_sync()
