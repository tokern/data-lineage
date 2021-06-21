import logging
from abc import ABC, abstractmethod
from typing import List, Mapping, Set

from dbcat.catalog import Catalog, CatColumn, CatTable

from data_lineage.parser.node import AcceptingNode
from data_lineage.parser.table_visitor import (
    ColumnRefVisitor,
    RangeVarVisitor,
    TableVisitor,
)
from data_lineage.parser.visitor import Visitor


class AliasContext:
    def __init__(self, catalog: Catalog, alias: str, tables: List[CatTable]):
        self._catalog = catalog
        self._alias = alias
        self._tables = tables

    @property
    def alias(self):
        return self._alias

    @property
    def tables(self):
        return self._tables

    def get_columns(self, column_names: List[str] = []) -> List[CatColumn]:
        columns: List[CatColumn] = []
        for table in self._tables:
            columns = columns + self._catalog.get_columns_for_table(table, column_names)

        return columns


class WithContext(AliasContext):
    def __init__(
        self,
        catalog: Catalog,
        alias: str,
        tables: List[CatTable],
        columns: List[CatColumn],
    ):
        super(WithContext, self).__init__(catalog, alias, tables)
        self._columns = columns

    def get_columns(self, column_names: List[str] = []) -> List[CatColumn]:
        if len(column_names) > 0:
            filtered = []
            for column in self._columns:
                if column.name in column_names:
                    filtered.append(column)

            return filtered
        else:
            return self._columns


class Binder(Visitor, ABC):
    @property
    @abstractmethod
    def _visited_tables(self) -> List[AcceptingNode]:
        pass

    @property
    @abstractmethod
    def _visited_columns(self) -> List[AcceptingNode]:
        pass

    @property
    def tables(self) -> Set[CatTable]:
        return self._tables

    @property
    def columns(self) -> List[CatColumn]:
        return self._columns

    def __init__(self, catalog: Catalog, alias_map: Mapping[str, AliasContext] = {}):
        self._catalog = catalog
        self._tables: Set[CatTable] = set()
        self._columns: List[CatColumn] = []
        self._alias_map: Mapping[str, AliasContext] = alias_map

    def bind(self):
        bound_tables = self._bind_tables()

        self._tables = set(bound_tables)
        self._columns = self._bind_columns()

    def _bind_tables(self):
        bound_tables = []
        for table in self._visited_tables:
            visitor = RangeVarVisitor()
            visitor.visit(table)

            logging.debug("Searching for: {}".format(visitor.search_string))

            if not visitor.is_qualified and visitor.name in self._alias_map:
                bound_tables = bound_tables + list(self._alias_map[visitor.name].tables)
                logging.debug("Added tables for alias {}".format(visitor.name))
            else:
                candidate_table = self._catalog.search_table(**visitor.search_string)
                logging.debug("Bound source table: {}".format(candidate_table))

                self._alias_map[visitor.alias] = AliasContext(
                    catalog=self._catalog, alias=visitor.alias, tables=[candidate_table]
                )
                bound_tables.append(candidate_table)
        return bound_tables

    def _bind_columns(self) -> List[CatColumn]:
        bound_cols: List[CatColumn] = []
        for column in self._visited_columns:
            column_ref_visitor = ColumnRefVisitor()
            column_ref_visitor.visit(column)
            alias_list = list(self._alias_map.values())
            if column_ref_visitor.is_qualified:
                if column_ref_visitor.table_name not in self._alias_map:
                    raise RuntimeError(
                        "Table ({}) not found for column ({}).".format(
                            column_ref_visitor.name[0], column_ref_visitor.name
                        )
                    )
                assert column_ref_visitor.table_name is not None
                alias_list = [self._alias_map[column_ref_visitor.table_name]]

            bound_cols = bound_cols + Binder._search_column_in_tables(
                column, column_ref_visitor, alias_list
            )

        return bound_cols

    @staticmethod
    def _search_column_in_tables(
        column, column_ref_visitor, alias_list: List[AliasContext]
    ) -> List[CatColumn]:
        found_cols: List[CatColumn] = []
        if column_ref_visitor.is_a_star:
            for alias_context in alias_list:
                found_cols = alias_context.get_columns()
                logging.debug(
                    "Bound all source columns in {}".format(alias_context.tables)
                )
        else:
            candidate_columns: List[CatColumn] = []
            for alias_context in alias_list:
                candidate_columns = candidate_columns + alias_context.get_columns(
                    [column_ref_visitor.column_name]
                )
            if len(candidate_columns) == 0:
                raise RuntimeError("{} not found in any table".format(column))
            elif len(candidate_columns) > 1:
                raise RuntimeError(
                    "Ambiguous column name ({}). Multiple matches found".format(
                        column_ref_visitor.name
                    )
                )
            logging.debug("Bound source column: {}".format(candidate_columns[0]))
            found_cols.append(candidate_columns[0])
        return found_cols


class SelectBinder(Binder):
    def __init__(
        self,
        catalog: Catalog,
        tables: List[AcceptingNode],
        columns: List[AcceptingNode],
        alias_map: Mapping[str, AliasContext] = {},
    ):
        super(SelectBinder, self).__init__(catalog, alias_map)
        self._table_nodes: List[AcceptingNode] = tables
        self._column_nodes: List[AcceptingNode] = columns

    @property
    def _visited_tables(self) -> List[AcceptingNode]:
        return self._table_nodes

    @property
    def _visited_columns(self) -> List[AcceptingNode]:
        return self._column_nodes

    def visit_select_stmt(self, node):
        table_visitor = TableVisitor()
        table_visitor.visit(node)
        self._table_nodes = table_visitor.sources
        self._column_nodes = table_visitor.columns
