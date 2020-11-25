from data_lineage.parser.visitor import Visitor
from data_lineage.visitors.column_ref_visitor import ColumnRefVisitor
from data_lineage.visitors.range_var_visitor import RangeVarVisitor
from data_lineage.visitors.table_visitor import TableVisitor


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

    def bind(self, catalog):
        target_table_visitor = RangeVarVisitor()
        target_table_visitor.visit(self._target_table)

        table = catalog.get_table(target_table_visitor.fqdn)
        self._target_table = table.fqdn

        if len(self._target_columns) == 0:
            self._target_columns = [column.fqdn for column in table.columns]
        else:
            bound_cols = []
            for column in self._target_columns:
                column_ref_visitor = ColumnRefVisitor()
                column_ref_visitor.visit(column)
                bound = catalog.get_column(
                    self._target_table[0],
                    self._target_table[1],
                    column_ref_visitor.name[0],
                )
                if bound is None:
                    raise RuntimeError("{} not found in table".format(column))
                bound_cols.append(bound.fqdn)

            self._target_columns = bound_cols

        alias_map = {}
        bound_tables = []
        for table in self._source_tables:
            visitor = RangeVarVisitor()
            visitor.visit(table)
            if visitor.alias is not None:
                alias_map[visitor.alias] = visitor.fqdn
            bound_tables.append(catalog.get_table(visitor.fqdn).fqdn)

        self._source_tables = bound_tables
        bound_cols = []
        for column in self._source_columns:
            column_ref_visitor = ColumnRefVisitor()
            column_ref_visitor.visit(column)
            if column_ref_visitor.name[0] in alias_map:
                table = catalog.get_table(alias_map[column_ref_visitor.name[0]])
            else:
                table = catalog.get_table((None, column_ref_visitor.name[0]))
            fqdn = list(table.fqdn)
            fqdn.append(column_ref_visitor.name[1])
            bound = catalog.get_column(tuple(fqdn))
            if bound is None:
                raise RuntimeError("{} not found in table".format(column))
            bound_cols.append(bound.fqdn)

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
