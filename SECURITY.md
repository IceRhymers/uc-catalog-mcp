# Security Model

## Metadata Access

The MCP tools (`search_tables`, `describe_table`, `list_catalogs`, `list_schemas`) query a
pre-built Lakebase (PostgreSQL + pgvector) cache of Unity Catalog table metadata. All MCP
users see the same cached metadata — there is **no per-user UC permission filtering** at
query time.

### What is exposed to any MCP user

| Field | Source |
|---|---|
| Table full name (`catalog.schema.table`) | Unity Catalog |
| Table comment | Unity Catalog |
| Column names | Unity Catalog |
| Column types | Unity Catalog |
| Column comments | Unity Catalog |

### What is NOT exposed

- Actual data rows
- Row counts or sample values
- Access policies or credentials

## Comparison to UC BROWSE Privilege

Unity Catalog's `BROWSE` privilege — granted by default to `account users` in most Databricks
deployments — already exposes table names, column names, column types, and table comments. The
MCP tools expose the same set **plus column comments**. That is the only delta beyond what
BROWSE already permits.

This is an intentional design tradeoff. The core value of the Lakebase cache is sub-100ms
semantic search via a pre-built pgvector index. Per-user UC permission checks at query time
would add 500ms+ latency per search, require over-fetching then filtering, and fundamentally
undermine the semantic search use case for agentic analytics workflows.

## Operator-Controlled Scope

Only catalogs explicitly listed in the `CATALOG_ALLOWLIST` environment variable are indexed.
The operator decides which catalogs are visible to MCP users. The app's
`account users: CAN_USE` permission in `resources/app.yml` grants access to the same user
set that already receives BROWSE by default — no additional privilege is granted.

## Future: `ENFORCE_UC_PERMISSIONS` Opt-In

A future `ENFORCE_UC_PERMISSIONS=true` environment variable (opt-in, default off) could add
per-user UC permission batch-checks at query time for strict-governance deployments — accepting
the latency penalty in exchange for exact permission parity with UC ACLs. This is out of scope
for v1; track separately when a concrete deployment requires it.
