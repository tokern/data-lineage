from data_lineage.parser.visitor import Visitor
from data_lineage.visitors.table_visitor import TableVisitor


class DmlVisitor(Visitor):
    def __init__(self):
        self._target = None
        self._sources = []

    @property
    def target(self):
        return self._target

    @property
    def sources(self):
        return self._sources

    def visit_range_var(self, node):
        if node.schemaname:
            self._target = (node.schemaname.value, node.relname.value)
        else:
            self._target = (None, node.relname.value)


class SelectSourceVisitor(DmlVisitor):
    def visit_select_stmt(self, node):
        table_visitor = TableVisitor()
        table_visitor.visit(node)
        self._sources = table_visitor.sources


class SelectIntoVisitor(DmlVisitor):
    def visit_select_stmt(self, node):
        super(SelectIntoVisitor, self).visit(node.intoClause)
        table_visitor = TableVisitor()
        table_visitor.visit(node)
        self._sources = table_visitor.sources


class CopyFromVisitor(DmlVisitor):
    def visit_copy_stmt(self, node):
        if node.is_from:
            super(CopyFromVisitor, self).visit_copy_stmt(node)
