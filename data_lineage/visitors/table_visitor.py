from data_lineage.parser.visitor import Visitor


class TableVisitor(Visitor):
    def __init__(self):
        self._sources = []
        self._columns = []

    @property
    def sources(self):
        return self._sources

    @property
    def columns(self):
        return self._columns

    def visit_range_var(self, node):
        self._sources.append(node)

    def visit_into_clause(self, node):
        pass

    def visit_column_ref(self, node):
        self._columns.append(node)
