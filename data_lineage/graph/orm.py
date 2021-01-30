from contextlib import closing
from typing import Any, Dict, List

from dbcat.catalog.orm import Base, Catalog, CatColumn, CatDatabase, CatSchema, CatTable
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship


class ColumnEdge(Base):
    __tablename__ = "column_edges"

    id = Column(Integer, primary_key=True)
    payload = Column(JSONB)

    source_id = Column(Integer, ForeignKey("columns.id"))
    target_id = Column(Integer, ForeignKey("columns.id"))
    source = relationship("CatColumn", foreign_keys=source_id, lazy="joined")
    target = relationship("CatColumn", foreign_keys=target_id, lazy="joined")

    def __init__(self, source: CatColumn, target: CatColumn, payload: Dict[Any, Any]):
        self.source = source
        self.target = target
        self.payload = payload

    def __repr__(self):
        return "<Edge: {} -{}-> {}>".format(self.source, self.target, self.payload)


class LineageCatalog(Catalog):
    def save_lineage(self, edges: List[ColumnEdge]):
        with closing(self.session) as session:
            for e in edges:
                self._get_one_or_create(
                    session,
                    ColumnEdge,
                    source=e.source,
                    target=e.target,
                    create_method_kwargs={"payload": e.payload},
                )
            session.commit()

    def get_column_edge(
        self, source_name: CatColumn, target_name: CatColumn, payload: Dict[Any, Any]
    ):
        with closing(self.session) as session:
            source = (
                session.query(CatColumn)
                .join(CatColumn.table)
                .join(CatTable.schema)
                .join(CatSchema.database)
                .filter(CatDatabase.name == source_name.table.schema.database.name)
                .filter(CatSchema.name == source_name.table.schema.name)
                .filter(CatTable.name == source_name.table.name)
                .filter(CatColumn.name == source_name.name)
                .one()
            )

            target = (
                session.query(CatColumn)
                .join(CatColumn.table)
                .join(CatTable.schema)
                .join(CatSchema.database)
                .filter(CatDatabase.name == target_name.table.schema.database.name)
                .filter(CatSchema.name == target_name.table.schema.name)
                .filter(CatTable.name == target_name.table.name)
                .filter(CatColumn.name == target_name.name)
                .one()
            )

            column_edge = self._get_one_or_create(
                session,
                ColumnEdge,
                source=source,
                target=target,
                create_method_kwargs={"payload": payload},
            )

            session.commit()
            return column_edge
