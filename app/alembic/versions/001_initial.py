"""Initial catalog_metadata table with pgvector HNSW index.

Revision ID: 001
Revises:
Create Date: 2026-04-14
"""

from typing import Sequence, Union

from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute("""
        CREATE TABLE IF NOT EXISTS catalog_metadata (
            full_name TEXT PRIMARY KEY,
            catalog TEXT NOT NULL,
            schema_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            table_type TEXT,
            comment TEXT,
            columns JSONB,
            content_hash TEXT,
            embedding vector(1024),
            synced_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_catalog_metadata_embedding
            ON catalog_metadata USING hnsw (embedding vector_cosine_ops)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_catalog_metadata_catalog
            ON catalog_metadata (catalog)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_catalog_metadata_catalog_schema
            ON catalog_metadata (catalog, schema_name)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS catalog_metadata")
    op.execute("DROP EXTENSION IF EXISTS vector")
