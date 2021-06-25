import logging
from typing import Any, Dict, List, Optional, Set, Tuple, Type

from dbcat.catalog import Catalog, CatColumn, CatSource, CatTable

from data_lineage import ColumnNotFound, SemanticError, TableNotFound
from data_lineage.parser.binder import ColumnContext, SelectBinder, WithContext
from data_lineage.parser.node import AcceptingNode
from data_lineage.parser.table_visitor import (
    ColumnRefVisitor,
    RangeVarVisitor,
    TableVisitor,
)
from data_lineage.parser.visitor import ExprVisitor, QueryVisitor


class DmlVisitor(QueryVisitor):
    def __init__(self, name: str, expr_visitor_clazz: Type[ExprVisitor]):
        self._name = name
        self._insert_table: Optional[AcceptingNode] = None
        self._insert_columns: List[str] = []
        self._target_table: Optional[CatTable] = None
        self._target_columns: List[CatColumn] = []
        self._source_tables: Set[CatTable] = set()
        self._source_columns: List[ColumnContext] = []
        self._select_tables: List[AcceptingNode] = []
        self._select_columns: List[ExprVisitor] = []
        self._with_aliases: Dict[str, Dict[str, Any]] = {}
        self._alias_map: Dict[str, WithContext] = {}
        self._column_alias_generator = ("_U{}".format(i) for i in range(0, 1000))
        self.expr_visitor_clazz = expr_visitor_clazz

    @property
    def name(self) -> str:
        return self._name

    @property
    def insert_table(self) -> Optional[AcceptingNode]:
        return self._insert_table

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
    def source_columns(self) -> List[ColumnContext]:
        return self._source_columns

    @property
    def select_tables(self) -> List[AcceptingNode]:
        return self._select_tables

    @property
    def select_columns(self) -> List[ExprVisitor]:
        return self._select_columns

    def visit_range_var(self, node):
        self._insert_table = node

    def visit_res_target(self, node):
        self._insert_columns.append(node.name.value)

    def visit_common_table_expr(self, node):
        with_alias = node.ctename.value
        table_visitor = TableVisitor(self.expr_visitor_clazz)
        table_visitor.visit(node.ctequery)

        self._with_aliases[with_alias] = {
            "tables": table_visitor.sources,
            "columns": table_visitor.columns,
        }

    def bind(self, catalog: Catalog, source: CatSource):
        self._bind_target(catalog, source)

        self._bind_with(catalog, source)
        binder = SelectBinder(
            catalog,
            source,
            self._select_tables,
            self._select_columns,
            self._column_alias_generator,
            self._alias_map,
        )
        binder.bind()

        if len(binder.tables) == 0:
            raise SemanticError("No source tables found")

        if len(binder.columns) == 0:
            raise SemanticError("No source columns found")

        if self.target_table is None:
            raise SemanticError("No target table found")

        if len(self.target_columns) == 0:
            raise SemanticError("No target columns found")

        if len(self.target_columns) != len(binder.columns):
            raise SemanticError(
                "No. of target columns({}) does not match no. of source columns({})".format(
                    len(self.target_columns), len(self.select_columns)
                )
            )

        self._source_tables = binder.tables
        self._source_columns = binder.columns

    def _bind_target(self, catalog: Catalog, source: CatSource):
        target_table_visitor = RangeVarVisitor()
        target_table_visitor.visit(self._insert_table)
        logging.debug("Searching for: {}".format(target_table_visitor.search_string))
        try:
            self._target_table = catalog.search_table(
                source_like=source.name, **target_table_visitor.search_string
            )
        except RuntimeError as error:
            logging.debug(str(error))
            raise TableNotFound(
                '"{schema_like}"."{table_like}" is not found'.format(
                    **target_table_visitor.search_string
                )
            )
        logging.debug("Bound target table: {}".format(self._target_table))
        if len(self._insert_columns) == 0:
            self._target_columns = catalog.get_columns_for_table(self._target_table)
            logging.debug("Bound all columns in {}".format(self._target_table))
        else:
            bound_cols = catalog.get_columns_for_table(
                self._target_table, column_names=self._insert_columns
            )
            # Handle error case
            if len(bound_cols) != len(self._insert_columns):
                for column in self._insert_columns:
                    found = False
                    for bound in bound_cols:
                        if column == bound.name:
                            found = True
                            break

                    if not found:
                        raise ColumnNotFound("{} column is not found".format(column))

            self._target_columns = bound_cols
            logging.debug("Bound {} target columns".format(len(bound_cols)))

    def _bind_with(self, catalog: Catalog, source: CatSource):
        if self._with_aliases:
            # Bind all the WITH expressions
            for key in self._with_aliases.keys():
                binder = SelectBinder(
                    catalog,
                    source,
                    self._with_aliases[key]["tables"],
                    self._with_aliases[key]["columns"],
                    self._column_alias_generator,
                )
                binder.bind()
                self._alias_map[key] = WithContext(
                    catalog=catalog,
                    alias=key,
                    tables=binder.tables,
                    columns=binder.columns,
                )

    def resolve(
        self,
    ) -> Tuple[
        Tuple[Optional[str], str],
        List[Tuple[Optional[str], str]],
        List[Tuple[Optional[str], str]],
    ]:
        target_table_visitor = RangeVarVisitor()
        target_table_visitor.visit(self._insert_table)

        bound_tables = []
        for table in self._select_tables:
            visitor = RangeVarVisitor()
            visitor.visit(table)
            bound_tables.append(visitor.fqdn)

        bound_cols = []
        for expr_visitor in self._select_columns:
            for column in expr_visitor.columns:
                column_ref_visitor = ColumnRefVisitor()
                column_ref_visitor.visit(column)
                bound_cols.append(column_ref_visitor.name[0])

        return target_table_visitor.fqdn, bound_tables, bound_cols


class SelectSourceVisitor(DmlVisitor):
    def __init__(self, name: str, expr_visitor_clazz: Type[ExprVisitor] = ExprVisitor):
        super(SelectSourceVisitor, self).__init__(name, expr_visitor_clazz)

    def visit_select_stmt(self, node):
        table_visitor = TableVisitor(self.expr_visitor_clazz)
        table_visitor.visit(node)
        self._select_tables = table_visitor.sources
        self._select_columns = table_visitor.columns


class SelectIntoVisitor(DmlVisitor):
    def __init__(self, name: str, expr_visitor_clazz: Type[ExprVisitor] = ExprVisitor):
        super(SelectIntoVisitor, self).__init__(name, expr_visitor_clazz)

    def visit_select_stmt(self, node):
        super(SelectIntoVisitor, self).visit(node.intoClause)
        table_visitor = TableVisitor(self.expr_visitor_clazz)
        table_visitor.visit(node.targetList)
        table_visitor.visit(node.fromClause)
        self._select_tables = table_visitor.sources
        self._select_columns = table_visitor.columns


class CopyFromVisitor(DmlVisitor):
    def __init__(self, name: str, expr_visitor_clazz: Type[ExprVisitor] = ExprVisitor):
        super(CopyFromVisitor, self).__init__(name, expr_visitor_clazz)

    def visit_copy_stmt(self, node):
        if node.is_from:
            super(CopyFromVisitor, self).visit_copy_stmt(node)
