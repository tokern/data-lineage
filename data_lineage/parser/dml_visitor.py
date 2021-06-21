import logging
from typing import Dict, List, Optional, Set

from dbcat.catalog import Catalog, CatColumn, CatTable

from data_lineage.parser.binder import SelectBinder, WithContext
from data_lineage.parser.node import AcceptingNode
from data_lineage.parser.table_visitor import (
    ColumnRefVisitor,
    RangeVarVisitor,
    TableVisitor,
)
from data_lineage.parser.visitor import Visitor


class DmlVisitor(Visitor):
    def __init__(self, name: str):
        self._name = name
        self._target_table: Optional[CatTable] = None
        self._target_columns: List[CatColumn] = []
        self._source_tables: Set[CatTable] = set()
        self._source_columns: List[CatColumn] = []
        self._select_tables: List[AcceptingNode] = []
        self._select_columns: List[AcceptingNode] = []
        self._with_aliases: Dict[str, List[AcceptingNode]] = {}
        self._alias_map: Dict[str, WithContext] = {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def target_table(self) -> CatTable:
        return self._target_table

    @property
    def target_columns(self) -> List[CatColumn]:
        return self._target_columns

    @property
    def source_tables(self) -> Set[CatTable]:
        return self._source_tables

    @property
    def source_columns(self) -> List[CatColumn]:
        return self._source_columns

    @property
    def select_tables(self) -> List[AcceptingNode]:
        return self._select_tables

    @property
    def select_columns(self) -> List[AcceptingNode]:
        return self._select_columns

    def visit_range_var(self, node):
        self._target_table = node

    def visit_res_target(self, node):
        self._target_columns.append(node.name.value)

    def visit_common_table_expr(self, node):
        with_alias = node.ctename.value
        table_visitor = TableVisitor()
        table_visitor.visit(node.ctequery)

        self._with_aliases[with_alias] = {
            "tables": table_visitor.sources,
            "columns": table_visitor.columns,
        }

    def bind(self, catalog: Catalog):
        self._bind_target(catalog)

        self._bind_with(catalog)
        binder = SelectBinder(
            catalog, self._select_tables, self._select_columns, self._alias_map
        )
        binder.bind()
        self._source_tables = binder.tables
        self._source_columns = binder.columns

    def _bind_target(self, catalog: Catalog):
        target_table_visitor = RangeVarVisitor()
        target_table_visitor.visit(self._target_table)
        logging.debug("Searching for: {}".format(target_table_visitor.search_string))
        self._target_table = catalog.search_table(**target_table_visitor.search_string)
        logging.debug("Bound target table: {}".format(self._target_table))
        if len(self._target_columns) == 0:
            self._target_columns = catalog.get_columns_for_table(self._target_table)
            logging.debug("Bound all columns in {}".format(self._target_table))
        else:
            bound_cols = catalog.get_columns_for_table(
                self._target_table, column_names=self._target_columns
            )
            # Handle error case
            if len(bound_cols) != len(self._target_columns):
                for column in self._target_columns:
                    found = False
                    for bound in bound_cols:
                        if column == bound.name:
                            found = True
                            break

                    if not found:
                        raise RuntimeError("'{}' column is not found".format(column))

            self._target_columns = bound_cols
            logging.debug("Bound {} target columns".format(len(bound_cols)))

    def _bind_with(self, catalog):
        if self._with_aliases:
            # Bind all the WITH expressions
            for key in self._with_aliases.keys():
                binder = SelectBinder(
                    catalog,
                    self._with_aliases[key]["tables"],
                    self._with_aliases[key]["columns"],
                )
                binder.bind()
                self._alias_map[key] = WithContext(
                    catalog=catalog,
                    alias=key,
                    tables=binder.tables,
                    columns=binder.columns,
                )

    def resolve(self):
        target_table_visitor = RangeVarVisitor()
        target_table_visitor.visit(self._target_table)

        self._target_table = target_table_visitor.fqdn

        bound_tables = []
        for table in self._select_tables:
            visitor = RangeVarVisitor()
            visitor.visit(table)
            bound_tables.append(visitor.fqdn)

        self._select_tables = bound_tables

        bound_cols = []
        for column in self._select_columns:
            column_ref_visitor = ColumnRefVisitor()
            column_ref_visitor.visit(column)
            bound_cols.append(column_ref_visitor.name[0])

        self._select_columns = bound_cols


class SelectSourceVisitor(DmlVisitor):
    def __init__(self, name):
        super(SelectSourceVisitor, self).__init__(name)

    def visit_select_stmt(self, node):
        table_visitor = TableVisitor()
        table_visitor.visit(node)
        self._select_tables = table_visitor.sources
        self._select_columns = table_visitor.columns


class SelectIntoVisitor(DmlVisitor):
    def __init__(self, name):
        super(SelectIntoVisitor, self).__init__(name)

    def visit_select_stmt(self, node):
        super(SelectIntoVisitor, self).visit(node.intoClause)
        table_visitor = TableVisitor()
        table_visitor.visit(node.targetList)
        table_visitor.visit(node.fromClause)
        self._select_tables = table_visitor.sources
        self._select_columns = table_visitor.columns


class CopyFromVisitor(DmlVisitor):
    def __init__(self, name):
        super(CopyFromVisitor, self).__init__(name)

    def visit_copy_stmt(self, node):
        if node.is_from:
            super(CopyFromVisitor, self).visit_copy_stmt(node)
