from dbcat.catalog.orm import Catalog

from data_lineage.parser.table_visitor import (
    ColumnRefVisitor,
    RangeVarVisitor,
    TableVisitor,
)
from data_lineage.parser.visitor import Visitor


class DmlVisitor(Visitor):
    def __init__(self):
        self._target_table = None
        self._target_columns = []
        self._source_tables = []
        self._source_columns = []

    @property
    def target_table(self):
        return self._target_table

    @property
    def target_columns(self):
        return self._target_columns

    @property
    def source_tables(self):
        return self._source_tables

    @property
    def source_columns(self):
        return self._source_columns

    def visit_range_var(self, node):
        self._target_table = node

    def visit_res_target(self, node):
        self._target_columns.append(node.name.value)

    def resolve(self):
        target_table_visitor = RangeVarVisitor()
        target_table_visitor.visit(self._target_table)

        self._target_table = target_table_visitor.fqdn

        bound_tables = []
        for table in self._source_tables:
            visitor = RangeVarVisitor()
            visitor.visit(table)
            bound_tables.append(visitor.fqdn)

        self._source_tables = bound_tables

        bound_cols = []
        for column in self._source_columns:
            column_ref_visitor = ColumnRefVisitor()
            column_ref_visitor.visit(column)
            bound_cols.append(column_ref_visitor.name[0])

        self._source_columns = bound_cols

    def bind(self, catalog: Catalog):
        target_table_visitor = RangeVarVisitor()
        target_table_visitor.visit(self._target_table)

        self.logger.debug(
            "Searching for: {}".format(target_table_visitor.search_string)
        )
        candidate_tables = catalog.search_table(**target_table_visitor.search_string)
        if len(candidate_tables) == 0:
            raise RuntimeError("'{}' table not found".format(target_table_visitor.fqdn))
        elif len(candidate_tables) > 1:
            raise RuntimeError("Ambiguous table name. Multiple matches found")

        self._target_table = candidate_tables[0]
        self.logger.debug("Bound target table: {}".format(candidate_tables[0]))

        if len(self._target_columns) == 0:
            self._target_columns = catalog.get_columns_for_table(candidate_tables[0])
            self.logger.debug("Bound all columns in {}".format(self._target_table))
        else:
            bound_cols = []
            for column in self._target_columns:
                bound = catalog.get_columns_for_table(
                    candidate_tables[0], column_names=[column]
                )
                if len(bound) == 0:
                    raise RuntimeError("{} not found in table".format(column))
                elif len(bound) > 1:
                    raise RuntimeError("Ambiguous column name. Multiple matches found")

                self.logger.debug("Bound target column: {}".format(bound[0]))
                bound_cols.append(bound[0])

            self._target_columns = bound_cols

        alias_map = {}
        bound_tables = []
        for table in self._source_tables:
            visitor = RangeVarVisitor()
            visitor.visit(table)
            if visitor.alias is not None:
                alias_map[visitor.alias] = visitor.search_string

            self.logger.debug("Searching for: {}".format(visitor.search_string))

            candidate_tables = catalog.search_table(**visitor.search_string)
            if len(candidate_tables) == 0:
                raise RuntimeError("'{}' table not found".format(visitor.fqdn))
            elif len(candidate_tables) > 1:
                raise RuntimeError("Ambiguous table name. Multiple matches found")

            self.logger.debug("Bound source table: {}".format(candidate_tables[0]))
            bound_tables.append(candidate_tables[0])

        self._source_tables = bound_tables
        bound_cols = []
        for column in self._source_columns:
            column_ref_visitor = ColumnRefVisitor()
            column_ref_visitor.visit(column)
            if column_ref_visitor.name[0] in alias_map:
                table_name = alias_map[column_ref_visitor.name[0]]
            else:
                table_name = {"table_like": column_ref_visitor.name[0]}

            self.logger.debug("Searching for: {}".format(table_name))
            candidate_tables = catalog.search_table(**table_name)
            if len(candidate_tables) == 0:
                raise RuntimeError("'{}' table not found".format(visitor.fqdn))
            elif len(candidate_tables) > 1:
                raise RuntimeError("Ambiguous table name. Multiple matches found")

            bound = catalog.get_columns_for_table(
                table=candidate_tables[0], column_names=[column_ref_visitor.name[1]]
            )
            if len(bound) == 0:
                raise RuntimeError("{} not found in table".format(column))
            elif len(bound) > 1:
                raise RuntimeError("Ambiguous column name. Multiple matches found")

            self.logger.debug("Bound source column: {}".format(bound[0]))
            bound_cols.append(bound[0])

        self._source_columns = bound_cols


class SelectSourceVisitor(DmlVisitor):
    def visit_select_stmt(self, node):
        table_visitor = TableVisitor()
        table_visitor.visit(node)
        self._source_tables = table_visitor.sources
        self._source_columns = table_visitor.columns


class SelectIntoVisitor(DmlVisitor):
    def visit_select_stmt(self, node):
        super(SelectIntoVisitor, self).visit(node.intoClause)
        table_visitor = TableVisitor()
        table_visitor.visit(node)
        self._source_tables = table_visitor.sources
        self._source_columns = table_visitor.columns


class CopyFromVisitor(DmlVisitor):
    def visit_copy_stmt(self, node):
        if node.is_from:
            super(CopyFromVisitor, self).visit_copy_stmt(node)
