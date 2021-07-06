import json
import logging
from abc import ABC, abstractmethod
from json import JSONEncoder
from typing import List, Mapping, Set, Type

from dbcat.catalog import Catalog, CatColumn, CatSource, CatTable
from pglast import Node
from pglast.ast import RangeSubselect, RangeVar

from data_lineage import ColumnNotFound, SemanticError, TableNotFound
from data_lineage.parser.visitor import (
    ColumnRefVisitor,
    ExprVisitor,
    RangeSubselectVisitor,
    RangeVarVisitor,
)


class ColumnContext:
    def __init__(self, alias: str, columns: Set[CatColumn]):
        self._alias = alias.lower()
        self._columns = columns

    @property
    def alias(self):
        return self._alias

    @property
    def columns(self) -> Set[CatColumn]:
        return self._columns


class AliasContext:
    def __init__(self, catalog: Catalog, alias: str, tables: Set[CatTable]):
        self._catalog = catalog
        self._alias = alias.lower()
        self._tables = tables

    @property
    def alias(self):
        return self._alias

    @property
    def tables(self):
        return self._tables

    def get_columns(self, column_names: List[str] = None) -> List[ColumnContext]:
        columns: List[CatColumn] = []
        for table in self._tables:
            logging.debug("Searching in {}".format(table.fqdn))
            columns = columns + self._catalog.get_columns_for_table(table, column_names)

        return [
            ColumnContext(alias=column.name, columns={column}) for column in columns
        ]


class WithContext(AliasContext):
    def __init__(
        self,
        catalog: Catalog,
        alias: str,
        tables: Set[CatTable],
        columns: List[ColumnContext],
    ):
        super(WithContext, self).__init__(catalog, alias, tables)
        self._columns = columns

    def get_columns(self, column_names: List[str] = None) -> List[ColumnContext]:
        if column_names is not None:
            filtered = []
            for column in self._columns:
                logging.debug(
                    "Comparing with alias: {} - contains columns: {}".format(
                        column.alias,
                        json.dumps(list(column.columns), cls=CatColumnEncoder),
                    )
                )
                if column.alias in column_names:
                    filtered.append(column)

            return filtered
        else:
            return self._columns


class CatTableEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, CatTable):
            return {
                "name": obj.name,
                "schema": obj.schema.name,
                "source": obj.schema.source.name,
            }

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class CatColumnEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, CatColumn):
            return {
                "name": obj.name,
                "table": obj.table.name,
                "schema": obj.table.schema.name,
                "source": obj.table.schema.source.name,
            }

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class Binder(ABC):
    @property
    @abstractmethod
    def _visited_tables(self) -> List[Node]:
        pass

    @property
    @abstractmethod
    def _visited_columns(self) -> List[ExprVisitor]:
        pass

    @property
    def tables(self) -> Set[CatTable]:
        return self._tables

    @property
    def columns(self) -> List[ColumnContext]:
        return self._columns

    def __init__(
        self,
        catalog: Catalog,
        source: CatSource,
        alias_generator,
        expr_visitor_clazz: Type[ExprVisitor],
        alias_map: Mapping[str, AliasContext] = None,
    ):
        self._catalog = catalog
        self._source = source
        self._tables: Set[CatTable] = set()
        self._columns: List[ColumnContext] = []
        self._alias_map: Mapping[str, AliasContext] = alias_map or {}
        self._alias_generator = alias_generator
        self._expr_visitor_clazz = expr_visitor_clazz

    def bind(self):
        bound_tables = self._bind_tables()

        self._tables = set(bound_tables)
        self._columns = self._bind_columns()

    def _bind_tables(self):
        bound_tables = []
        for table in self._visited_tables:
            if isinstance(table, RangeVar):
                visitor = RangeVarVisitor()
                visitor(table)

                logging.debug("Searching for: {}".format(visitor.search_string))

                if not visitor.is_qualified and visitor.name in self._alias_map:
                    bound_tables = bound_tables + list(
                        self._alias_map[visitor.name].tables
                    )
                    logging.debug("Added tables for alias {}".format(visitor.name))
                else:
                    try:
                        candidate_table = self._catalog.search_table(
                            source_like=self._source.name, **visitor.search_string
                        )
                    except RuntimeError as err:
                        logging.debug(str(err))
                        raise TableNotFound(
                            '"{schema_like}"."{table_like}" is not found'.format(
                                **visitor.search_string
                            )
                        )
                    logging.debug("Bound source table: {}".format(candidate_table))

                    self._alias_map[visitor.alias] = AliasContext(
                        catalog=self._catalog,
                        alias=visitor.alias,
                        tables={candidate_table},
                    )
                    bound_tables.append(candidate_table)
            elif isinstance(table, RangeSubselect):
                visitor = RangeSubselectVisitor(self._expr_visitor_clazz)
                visitor(table)
                binder = SelectBinder(
                    self._catalog,
                    self._source,
                    visitor.sources,
                    visitor.columns,
                    self._alias_generator,
                    self._expr_visitor_clazz,
                )
                binder.bind()
                self._alias_map[visitor.alias] = WithContext(
                    catalog=self._catalog,
                    alias=visitor.alias,
                    tables=binder.tables,
                    columns=binder.columns,
                )
                bound_tables = bound_tables + list(binder.tables)
            else:
                raise SemanticError("Unknown parser state. Please contact Support")
        return bound_tables

    def _bind_columns(self) -> List[ColumnContext]:
        bound_cols: List[ColumnContext] = []
        for expr_visitor in self._visited_columns:
            target_cols: Set[ColumnContext] = set()
            is_a_star = False
            for column in expr_visitor.columns:
                column_ref_visitor = ColumnRefVisitor()
                column_ref_visitor(column)
                is_a_star = column_ref_visitor.is_a_star
                alias_list = list(self._alias_map.values())
                if column_ref_visitor.is_qualified:
                    if column_ref_visitor.table_name not in self._alias_map:
                        raise TableNotFound(
                            "{} not found for column ({}).".format(
                                column_ref_visitor.name[0], column_ref_visitor.name
                            )
                        )
                    assert column_ref_visitor.table_name is not None
                    alias_list = [self._alias_map[column_ref_visitor.table_name]]
                target_cols.update(
                    Binder._search_column_in_tables(column_ref_visitor, alias_list)
                )

            if is_a_star:
                for col in target_cols:
                    bound_cols.append(
                        ColumnContext(alias=col.alias, columns=col.columns)
                    )
            else:
                if expr_visitor.alias is not None:
                    alias = expr_visitor.alias
                elif len(target_cols) == 1:
                    alias = list(target_cols)[0].alias
                else:
                    alias = next(self._alias_generator)
                cols: Set[CatColumn] = set()
                for tgt in target_cols:
                    for c in tgt.columns:
                        cols.add(c)
                bound_cols.append(ColumnContext(alias=alias, columns=cols))

        if len(bound_cols) == 0:
            raise ColumnNotFound("No source columns found.")
        return bound_cols

    @staticmethod
    def _search_column_in_tables(
        column_ref_visitor, alias_list: List[AliasContext]
    ) -> List[ColumnContext]:
        found_cols: List[ColumnContext] = []
        if column_ref_visitor.is_a_star:
            for alias_context in alias_list:
                found_cols = alias_context.get_columns()
                logging.debug(
                    "Bound all source columns in {}".format(alias_context.tables)
                )
        else:
            candidate_columns: List[ColumnContext] = []
            global_table_list: List[CatTable] = []
            logging.debug("Searching for {}".format(column_ref_visitor.column_name))
            for alias_context in alias_list:
                logging.debug("Searching in {}".format(alias_context.alias))
                candidate_columns = candidate_columns + alias_context.get_columns(
                    [column_ref_visitor.column_name]
                )
                global_table_list = global_table_list + list(alias_context.tables)

            if len(candidate_columns) == 0:
                raise ColumnNotFound(
                    '"{}" not found in the following tables: {}'.format(
                        column_ref_visitor.column_name,
                        json.dumps(global_table_list, cls=CatTableEncoder),
                    )
                )
            elif len(candidate_columns) > 1:
                column_list = []
                for candidate in candidate_columns:
                    for col in candidate.columns:
                        column_list.append(col)
                raise ColumnNotFound(
                    "{} Ambiguous column name. Multiple matches found: {}".format(
                        column_ref_visitor.name,
                        json.dumps(column_list, cls=CatColumnEncoder),
                    )
                )
            logging.debug("Bound source column: {}".format(candidate_columns[0]))
            found_cols.append(candidate_columns[0])
        return found_cols


class SelectBinder(Binder):
    def __init__(
        self,
        catalog: Catalog,
        source: CatSource,
        tables: List[Node],
        columns: List[ExprVisitor],
        alias_generator,
        expr_visitor_clazz: Type[ExprVisitor],
        alias_map: Mapping[str, AliasContext] = None,
    ):
        super(SelectBinder, self).__init__(
            catalog, source, alias_generator, expr_visitor_clazz, alias_map
        )
        self._table_nodes: List[Node] = tables
        self._column_nodes: List[ExprVisitor] = columns

    @property
    def _visited_tables(self) -> List[Node]:
        return self._table_nodes

    @property
    def _visited_columns(self) -> List[ExprVisitor]:
        return self._column_nodes
