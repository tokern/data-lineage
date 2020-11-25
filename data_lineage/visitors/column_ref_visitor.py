from data_lineage.parser.visitor import Visitor


class ColumnRefVisitor(Visitor):
    def __init__(self):
        self._name = []

    @property
    def name(self):
        return tuple(self._name)

    def visit_string(self, node):
        self._name.append(node.str.value)
