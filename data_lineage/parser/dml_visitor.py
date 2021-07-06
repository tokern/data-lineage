import json
import logging
from typing import Any, Dict, List, Optional, Set, Tuple, Type

from dbcat.catalog import Catalog, CatColumn, CatSource, CatTable
from pglast import Node
from pglast.ast import IntoClause
from pglast.visitors import Ancestor, Continue, Skip, Visitor

from data_lineage import ColumnNotFound, SemanticError, TableNotFound
from data_lineage.parser.binder import (
    CatTableEncoder,
    ColumnContext,
    SelectBinder,
    WithContext,
)
from data_lineage.parser.visitor import (
    ColumnRefVisitor,
    ExprVisitor,
    RangeVarVisitor,
    TableVisitor,
)


class DmlVisitor(Visitor):
    def __init__(self, name: str, expr_visitor_clazz: Type[ExprVisitor]):
        self._name = name
        self._insert_table: Optional[Node] = None
        self._insert_columns: List[str] = []
        self._target_table: Optional[CatTable] = None
        self._target_columns: List[CatColumn] = []
        self._source_tables: Set[CatTable] = set()
        self._source_columns: List[ColumnContext] = []
        self._select_tables: List[Node] = []
        self._select_columns: List[ExprVisitor] = []
        self._with_aliases: Dict[str, Dict[str, Any]] = {}
        self._alias_map: Dict[str, WithContext] = {}
        self._column_alias_generator = ("_U{}".format(i) for i in range(0, 1000))
        self.expr_visitor_clazz = expr_visitor_clazz

    @property
    def name(self) -> str:
        return self._name

    @property
    def insert_table(self) -> Optional[Node]:
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
    def select_tables(self) -> List[Node]:
        return self._select_tables

    @property
    def select_columns(self) -> List[ExprVisitor]:
        return self._select_columns

    def visit_RangeVar(self, ancestors, node):
        self._insert_table = node
        return Skip

    def visit_ResTarget(self, ancestors, node):
        self._insert_columns.append(node.name)
        return Skip

    def visit_CommonTableExpr(self, ancestors, node):
        with_alias = node.ctename
        table_visitor = TableVisitor(self.expr_visitor_clazz)
        table_visitor(node.ctequery)

        self._with_aliases[with_alias] = {
            "tables": table_visitor.sources,
            "columns": table_visitor.columns,
        }
        return Skip

    def visit_CreateTableAsStmt(self, ancestors, node):
        """
            Do not process CTAS statement by default.
        :param ancestors:
        :type ancestors:
        :param node:
        :type node:
        :return:
        :rtype:
        """
        return Skip

    def bind(self, catalog: Catalog, source: CatSource):
        self._bind_target(catalog, source)

        self._bind_with(catalog, source)
        binder = SelectBinder(
            catalog,
            source,
            self._select_tables,
            self._select_columns,
            self._column_alias_generator,
            self.expr_visitor_clazz,
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
            raise SemanticError(
                "No target columns found in {}".format(
                    json.dumps(self.target_table, cls=CatTableEncoder)
                )
            )

        if len(self.target_columns) != len(binder.columns):
            raise SemanticError(
                "No. of target columns({}) does not match no. of source columns({})".format(
                    len(self.target_columns), len(binder.columns)
                )
            )

        self._source_tables = binder.tables
        self._source_columns = binder.columns

    def _bind_target(self, catalog: Catalog, source: CatSource):
        target_table_visitor = RangeVarVisitor()
        target_table_visitor(self._insert_table)
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
                        raise ColumnNotFound(
                            '"{}" not found in the following tables: {}'.format(
                                column,
                                json.dumps([self._target_table], cls=CatTableEncoder),
                            )
                        )

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
                    self.expr_visitor_clazz,
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
        target_table_visitor(self._insert_table)

        bound_tables = []
        for table in self._select_tables:
            visitor = RangeVarVisitor()
            visitor(table)
            bound_tables.append(visitor.fqdn)

        bound_cols = []
        for expr_visitor in self._select_columns:
            for column in expr_visitor.columns:
                column_ref_visitor = ColumnRefVisitor()
                column_ref_visitor(column)
                bound_cols.append(column_ref_visitor.name[0])

        return target_table_visitor.fqdn, bound_tables, bound_cols


class SelectSourceVisitor(DmlVisitor):
    def __init__(self, name: str, expr_visitor_clazz: Type[ExprVisitor] = ExprVisitor):
        super(SelectSourceVisitor, self).__init__(name, expr_visitor_clazz)

    def visit_SelectStmt(self, ancestors, node):
        table_visitor = TableVisitor(self.expr_visitor_clazz)
        table_visitor(node)
        self._select_tables = table_visitor.sources
        self._select_columns = table_visitor.columns
        for key in table_visitor.with_aliases.keys():
            self._with_aliases[key] = table_visitor.with_aliases[key]

        return Skip


class SelectIntoVisitor(DmlVisitor):
    def __init__(self, name: str, expr_visitor_clazz: Type[ExprVisitor] = ExprVisitor):
        super(SelectIntoVisitor, self).__init__(name, expr_visitor_clazz)

    def visit_SelectStmt(self, ancestors, node):
        super().__call__(node.intoClause)
        table_visitor = TableVisitor(self.expr_visitor_clazz)
        table_visitor(node.targetList)
        table_visitor(node.fromClause)
        self._select_tables = table_visitor.sources
        self._select_columns = table_visitor.columns
        for key in table_visitor.with_aliases.keys():
            self._with_aliases[key] = table_visitor.with_aliases[key]

        return Skip


class CTASVisitor(SelectSourceVisitor):
    def __init__(self, name: str, expr_visitor_clazz: Type[ExprVisitor] = ExprVisitor):
        super(CTASVisitor, self).__init__(name, expr_visitor_clazz)

    def visit_CreateTableAsStmt(self, ancestors, node):
        return Continue

    def visit_String(self, ancestors: Ancestor, node):
        # Check if parent is IntoClause
        parent = ancestors
        in_into_clause = False
        while parent is not None and not in_into_clause:
            in_into_clause = isinstance(parent.node, IntoClause)
            parent = parent.parent

        if in_into_clause:
            self._insert_columns.append(node.val)

    def _bind_target(self, catalog: Catalog, source: CatSource):
        target_table_visitor = RangeVarVisitor()
        target_table_visitor(self._insert_table)

        if target_table_visitor.is_qualified:
            schema = catalog.get_schema(
                source_name=source.name, schema_name=target_table_visitor.schema_name
            )
        elif source.default_schema is not None:
            schema = source.default_schema.schema
        else:
            raise SemanticError(
                "No default schema set for source {}".format(source.fqdn)
            )

        self._target_table = catalog.add_table(
            table_name=target_table_visitor.name, schema=schema
        )

        sort_order = 1
        for col in self._insert_columns:
            self._target_columns.append(
                catalog.add_column(
                    column_name=col,
                    data_type="varchar",
                    sort_order=sort_order,
                    table=self._target_table,
                )
            )
