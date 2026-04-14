# uc-catalog-mcp — CLAUDE.md

## What this project is

Semantic catalog discovery MCP server for Databricks Unity Catalog. FastAPI app
deployed as a Databricks App, backed by Lakebase (pgvector) indexed from UC
system tables. Fronted by uc-mcp-proxy.

No SQL execution tools. Read-only metadata + semantic search + lineage passthrough.

## Key architecture decisions

- **No SQL tools** — agents use spark-connect-mcp for execution
- **One vector per table** — full_name + table_comment + all column names/types/comments
  concatenated into a single BGE-large (1024-dim) embedding
- **Hash-based incremental ETL** — SHA-256 of table+column content; embedding API
  only called on hash mismatch. Stable workspaces skip 90%+ of embedding calls.
- **Lineage passthrough** — not indexed; forwarded directly to Databricks lineage APIs
- **Allowlist config** — operator specifies catalog or catalog+schema patterns;
  no "index everything" default

## Project layout

```
app/
  main.py           # FastAPI app, /mcp endpoint
  tools/            # One file per MCP tool
    search.py       # search_tables — pgvector ANN
    describe.py     # describe_table — Lakebase SELECT
    list.py         # list_catalogs, list_schemas
    lineage.py      # get_table_lineage, get_column_lineage — API passthrough
  db/
    client.py       # Lakebase connection (psycopg2 + token refresh)
    models.py       # catalog_metadata schema
sync/
  job.py            # Databricks Job: system tables → Lakebase ETL
  embeddings.py     # Databricks FM API calls (BGE-large)
  hash.py           # content_hash computation
migrations/
  001_initial.sql   # catalog_metadata table + pgvector HNSW index
scripts/
  deploy.sh         # End-to-end deploy: App + Lakebase + migrations + sync job
Makefile            # make deploy, make migrate, make sync
databricks.yml      # Databricks Asset Bundle config
```

## Doc update rules

When adding or changing features, update docs in the same PR — no separate doc PRs:

1. **README tool list** must stay in sync with `app/tools/` — if a tool is added,
   renamed, or removed, update the MCP Tools table in README.md
2. **Every MCP tool function** must have an agent-friendly docstring explaining
   what it returns and when to use it (agents read these)
3. **CHANGELOG.md** — add an entry for every user-facing change (new tool,
   changed behavior, config option added/removed)
4. **Architecture diagram** in README.md — update if the data flow changes
   (new data source, new index, new API integration)

## Lakebase patterns

Follow databricks-claw patterns for Lakebase:
- Connection via psycopg2, credentials from UC OAuth token (refresh every 60min)
- Use `%s` placeholders (not f-strings) in all SQL queries
- Vector similarity search: `ORDER BY embedding <=> %s::vector LIMIT %s`
- Content hash: `hashlib.sha256(content.encode()).hexdigest()`

## Sync job patterns

- Read `system.information_schema.tables` and `.columns` for allowed namespaces
- Build content string per table, compute SHA-256
- Fetch existing hashes from Lakebase in one query
- Call embedding API only for tables where hash changed or is new
- Upsert via `INSERT ... ON CONFLICT (full_name) DO UPDATE SET ...`
- Delete rows for tables no longer in system tables (for allowed namespaces)

## MCP tool patterns

Each tool in `app/tools/` follows this pattern:
```python
async def tool_name(param: str) -> ToolResult:
    """Agent-friendly docstring: what this returns and when to use it."""
    async with get_db() as conn:
        ...
```

Tools are registered in `app/main.py` via FastAPI + MCP protocol handler.

## Deployment

`make deploy` runs `scripts/deploy.sh` which:
1. `databricks bundle deploy` — deploys the App and Job
2. Provisions Lakebase instance (idempotent)
3. Runs migrations (`migrations/*.sql` in order)
4. Triggers initial sync job run

## GitHub workflow

- Open an issue before any PR
- PR title: `feat:`, `fix:`, `chore:` prefix
- Squash merge
