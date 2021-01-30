# type: ignore

from data_lineage.parser.visitor import Visitor


class RangeVarVisitor(Visitor):
    def __init__(self):
        self._schema_name = None
        self._name = None
        self._alias = None

    @property
    def alias(self):
        return self._alias

    @property
    def fqdn(self):
        return self._schema_name, self._name

    @property
    def search_string(self):
        return {"schema_like": self._schema_name, "table_like": self._name}

    def visit_alias(self, node):
        self._alias = node.aliasname.value

    def visit_range_var(self, node):
        if node.schemaname:
            self._schema_name = node.schemaname.value
        self._name = node.relname.value
        self.visit(node.alias)
