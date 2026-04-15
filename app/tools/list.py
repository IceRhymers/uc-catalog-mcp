"""list_catalogs and list_schemas MCP tools — namespace browsing."""

from __future__ import annotations

from sqlalchemy import distinct, select
from sqlalchemy.orm import Session

from app.db.models import CatalogMetadataOrm


def list_catalogs(db: Session) -> list[str]:
    """Return sorted list of all indexed Unity Catalog catalog names.

    Use this to discover which catalogs have been synced to the semantic index.

    Args:
        db: SQLAlchemy Session (injected by /mcp handler).

    Returns:
        Sorted list of catalog name strings.
    """
    return sorted(db.scalars(select(distinct(CatalogMetadataOrm.catalog))).all())


def list_schemas(catalog: str, db: Session) -> list[str]:
    """Return sorted list of schema names within a catalog that are indexed.

    Use this to browse the structure of a specific catalog. Returns [] if the
    catalog is unknown or has no indexed tables — never raises.

    Args:
        catalog: Unity Catalog catalog name.
        db: SQLAlchemy Session (injected by /mcp handler).

    Returns:
        Sorted list of schema name strings. Empty list if catalog not found.
    """
    return sorted(
        db.scalars(
            select(distinct(CatalogMetadataOrm.schema_name)).where(
                CatalogMetadataOrm.catalog == catalog
            )
        ).all()
    )
