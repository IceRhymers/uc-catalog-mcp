import dataclasses
import datetime


@dataclasses.dataclass
class ColumnMetadata:
    """Represents a single column in a Unity Catalog table."""

    name: str
    type: str
    comment: str | None = None


@dataclasses.dataclass
class CatalogMetadata:
    """Full metadata record for a Unity Catalog table, as stored in Lakebase."""

    full_name: str  # catalog.schema.table
    catalog: str
    schema_name: str
    table_name: str
    table_type: str | None
    comment: str | None
    columns: list[ColumnMetadata]
    content_hash: str | None
    embedding: list[float] | None
    synced_at: datetime.datetime | None
