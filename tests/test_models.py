import datetime
from pathlib import Path


def test_column_metadata_fields():
    from app.db.models import ColumnMetadata

    obj = ColumnMetadata(name="id", type="BIGINT", comment="primary key")
    assert obj.name == "id"
    assert obj.type == "BIGINT"
    assert obj.comment == "primary key"


def test_catalog_metadata_fields():
    from app.db.models import CatalogMetadata, ColumnMetadata

    col = ColumnMetadata(name="id", type="BIGINT", comment=None)
    obj = CatalogMetadata(
        full_name="main.default.users",
        catalog="main",
        schema_name="default",
        table_name="users",
        table_type="MANAGED",
        comment="User table",
        columns=[col],
        content_hash="abc123",
        embedding=[0.1] * 1024,
        synced_at=datetime.datetime.now(tz=datetime.timezone.utc),
    )
    assert obj.full_name == "main.default.users"
    assert obj.catalog == "main"
    assert obj.schema_name == "default"
    assert obj.table_name == "users"
    assert obj.table_type == "MANAGED"
    assert obj.comment == "User table"
    assert len(obj.columns) == 1
    assert obj.content_hash == "abc123"
    assert len(obj.embedding) == 1024
    assert obj.synced_at is not None


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
