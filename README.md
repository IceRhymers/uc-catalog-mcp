# uc-catalog-mcp

Semantic catalog discovery MCP server for Databricks Unity Catalog.

Deployed as a Databricks App, backed by Lakebase (pgvector) indexed from UC system tables. Fronted by [uc-mcp-proxy](https://github.com/IceRhymers/uc-mcp-proxy) for MCP client connectivity.

**No SQL execution.** Agents use [spark-connect-mcp](https://github.com/IceRhymers/spark-connect-mcp) for that.

## Architecture

```
UC system tables                     Lakebase (pgvector)
  system.information_schema.tables  → catalog_metadata
  system.information_schema.columns    (full_name PK, comment, columns JSONB,
                                         content_hash TEXT, embedding vector(1024),
                                         synced_at TIMESTAMPTZ)

Sync Job (Databricks Job, every 6h)
  1. Read system tables for allowed catalogs/schemas
  2. Compute content_hash = SHA-256(table_comment + all column names/types/comments)
  3. Compare vs stored hashes in Lakebase
  4. Call Databricks FM API (BGE-large) ONLY for new/changed tables
  5. Upsert changed rows, delete removed tables

FastAPI App (Databricks App)
  /mcp  ← uc-mcp-proxy routes here
  Tools: search (pgvector ANN), describe (Lakebase SELECT), list (Lakebase SELECT)
         lineage (direct Databricks API passthrough)
```

## MCP Tools

| Tool | Source | Description |
|------|--------|-------------|
| `search_tables(query)` | Lakebase pgvector | Semantic search over table+column descriptions |
| `describe_table(full_name)` | Lakebase | Full schema: columns, types, comments |
| `list_catalogs()` | Lakebase | All indexed catalogs |
| `list_schemas(catalog)` | Lakebase | Schemas within a catalog |
| `get_table_lineage(full_name)` | Databricks API | Upstream/downstream tables |
| `get_column_lineage(full_name, column)` | Databricks API | Column-level provenance |

## Requirements

- Databricks workspace with `system.information_schema.*` enabled
- Lakebase (provisioned via `make deploy`)
- Databricks App service principal with UC metastore access
- [uc-mcp-proxy](https://github.com/IceRhymers/uc-mcp-proxy) for MCP client routing

## Deploy

```bash
# Configure allowlist in databricks.yml, then:
make deploy
```

Single target provisions the App, Lakebase, runs migrations, and triggers the initial sync job.

## Configuration

Operator specifies which catalogs (or catalog+schema combinations) to index in `databricks.yml`:

```yaml
variables:
  catalog_allowlist:
    default: |
      - catalog: main
      - catalog: analytics
        schema_pattern: "prod_*"
```

Only namespaces in the allowlist are indexed. No "index everything" default.

## Embedding Strategy

- Model: Databricks Foundation Models API (BGE-large, 1024 dimensions)
- Content: `{full_name}: {table_comment}. Columns: {col} ({type}): {col_comment}, ...`
- Granularity: one vector per table (column context included, not per-column)
- Index: HNSW in pgvector

Hash-based incremental ETL — stable workspaces skip 90%+ of embedding API calls.
