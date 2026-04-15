import dataclasses
from pathlib import Path


def test_column_metadata_fields():
    from app.db.models import ColumnMetadata

    assert set(ColumnMetadata._fields) == {"name", "type", "comment"}


def test_catalog_metadata_fields():
    from app.db.models import CatalogMetadata

    expected = {
        "full_name",
        "catalog",
        "schema_name",
        "table_name",
        "table_type",
        "comment",
        "columns",
        "content_hash",
        "embedding",
        "synced_at",
    }
    assert {f.name for f in dataclasses.fields(CatalogMetadata)} == expected


def test_migration_sql_creates_extension():
    sql = Path("migrations/001_initial.sql").read_text()
    assert "CREATE EXTENSION IF NOT EXISTS vector" in sql


def test_migration_sql_creates_table():
    sql = Path("migrations/001_initial.sql").read_text()
    assert "CREATE TABLE IF NOT EXISTS catalog_metadata" in sql
    assert "full_name TEXT PRIMARY KEY" in sql
    assert "embedding vector(1024)" in sql
    assert "columns JSONB" in sql
    assert "content_hash TEXT" in sql
    assert "synced_at TIMESTAMPTZ" in sql


def test_migration_sql_creates_hnsw_index():
    sql = Path("migrations/001_initial.sql").read_text()
    assert "USING hnsw" in sql
    assert "vector_cosine_ops" in sql
