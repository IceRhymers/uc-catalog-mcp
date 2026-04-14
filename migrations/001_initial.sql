CREATE EXTENSION IF NOT EXISTS vector;

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
);

CREATE INDEX IF NOT EXISTS idx_catalog_metadata_embedding
  ON catalog_metadata USING hnsw (embedding vector_cosine_ops);

CREATE INDEX IF NOT EXISTS idx_catalog_metadata_catalog
  ON catalog_metadata (catalog);

CREATE INDEX IF NOT EXISTS idx_catalog_metadata_catalog_schema
  ON catalog_metadata (catalog, schema_name);
