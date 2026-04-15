"""Allowlist configuration for the sync ETL pipeline.

Parses the CATALOG_ALLOWLIST env var and generates parameterized SQL fragments
for filtering system.information_schema queries.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class AllowlistEntry:
    """A single allowlist entry: a catalog with optional schema pattern.

    Args:
        catalog: Unity Catalog catalog name.
        schema_pattern: Optional schema glob pattern (e.g. "prod_*"). None means all schemas.
    """

    catalog: str
    schema_pattern: str | None = field(default=None)


def parse_allowlist(raw: list[str]) -> list[AllowlistEntry]:
    """Parse a list of allowlist strings into AllowlistEntry objects.

    Each string is either:
    - "catalog" → AllowlistEntry(catalog="catalog", schema_pattern=None)
    - "catalog.schema_pattern" → AllowlistEntry(catalog="catalog", schema_pattern="schema_pattern")

    Args:
        raw: List of allowlist strings (e.g. ["main", "analytics.prod_*"]).

    Returns:
        List of AllowlistEntry objects.
    """
    entries = []
    for item in raw:
        if "." in item:
            catalog, schema_pattern = item.split(".", 1)
            entries.append(AllowlistEntry(catalog=catalog, schema_pattern=schema_pattern))
        else:
            entries.append(AllowlistEntry(catalog=item, schema_pattern=None))
    return entries


def build_system_table_filter(entries: list[AllowlistEntry]) -> tuple[str, list]:
    """Build a parameterized WHERE clause fragment for system table queries.

    Each AllowlistEntry contributes one OR-clause. Schema patterns use LIKE
    with '*' replaced by '%' for SQL LIKE syntax.

    Args:
        entries: Non-empty list of AllowlistEntry objects.

    Returns:
        Tuple of (sql_fragment, params_list) — safe for use with psycopg / JDBC.

    Raises:
        ValueError: If entries is empty (would scan all catalogs — not allowed).
    """
    if not entries:
        raise ValueError("Allowlist must not be empty — refusing to scan all catalogs")

    clauses = []
    params: list = []

    for entry in entries:
        if entry.schema_pattern is None:
            clauses.append("(table_catalog = %s)")
            params.append(entry.catalog)
        else:
            # Convert glob '*' to SQL LIKE '%'
            like_pattern = entry.schema_pattern.replace("*", "%")
            clauses.append("(table_catalog = %s AND table_schema LIKE %s)")
            params.append(entry.catalog)
            params.append(like_pattern)

    sql = "(" + " OR ".join(clauses) + ")"
    return sql, params
