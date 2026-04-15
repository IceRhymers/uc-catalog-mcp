import dataclasses
import datetime
from typing import NamedTuple

from pgvector.sqlalchemy import Vector
from sqlalchemy import Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class ColumnInfo(NamedTuple):
    """A single Unity Catalog column."""

    name: str
    type: str
    comment: str | None = None


class Base(DeclarativeBase):
    pass


class CatalogMetadataOrm(Base):
    """SQLAlchemy ORM model for the catalog_metadata table in Lakebase."""

    __tablename__ = "catalog_metadata"

    full_name: Mapped[str] = mapped_column(Text, primary_key=True)
    catalog: Mapped[str] = mapped_column(Text, nullable=False)
    schema_name: Mapped[str] = mapped_column(Text, nullable=False)
    table_name: Mapped[str] = mapped_column(Text, nullable=False)
    table_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    columns: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    content_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    embedding: Mapped[list | None] = mapped_column(Vector(1024), nullable=True)
    synced_at: Mapped[datetime.datetime | None] = mapped_column(
        nullable=True, server_default=func.now()
    )


@dataclasses.dataclass
class CatalogMetadata:
    """Python DTO for a catalog_metadata row — use CatalogMetadataOrm for DB operations."""

    full_name: str  # catalog.schema.table
    catalog: str
    schema_name: str
    table_name: str
    table_type: str | None
    comment: str | None
    columns: list[ColumnInfo]
    content_hash: str | None
    embedding: list[float] | None
    synced_at: datetime.datetime | None
