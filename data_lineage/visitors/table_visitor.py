from data_lineage.parser.visitor import Visitor


class TableVisitor(Visitor):
    def __init__(self):
        self._sources = []

    @property
    def sources(self):
        return self._sources

    def visit_range_var(self, node):
        if node.schemaname:
            self._sources.append((node.schemaname.value, node.relname.value))
        else:
            self._sources.append((None, node.relname.value))

    def visit_into_clause(self, node):
        pass
