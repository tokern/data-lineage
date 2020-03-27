import logging

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


class InsertVisitor(DmlVisitor):
    def visit_range_var(self, node):
        if node.schemaname:
            self._target = (node.schemaname, node.relname)
        else:
            self._target = (None, node.relname)

    def visit_select_stmt(self, node):
        table_visitor = TableVisitor()
        table_visitor.visit(node)
        self._sources = table_visitor.sources
