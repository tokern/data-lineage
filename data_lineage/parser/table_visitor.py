from typing import List, Optional, Tuple

from data_lineage.parser.node import AcceptingNode
from data_lineage.parser.visitor import Visitor


class TableVisitor(Visitor):
    def __init__(self):
        self._sources: List[AcceptingNode] = []
        self._columns: List[AcceptingNode] = []

    @property
    def sources(self) -> List[AcceptingNode]:
        return self._sources

    @property
    def columns(self) -> List[AcceptingNode]:
        return self._columns

    def visit_range_var(self, node):
        self._sources.append(node)

    def visit_column_ref(self, node):
        self._columns.append(node)


class ColumnRefVisitor(Visitor):
    def __init__(self):
        self._name: List[str] = []
        self._is_a_star: bool = False

    @property
    def name(self) -> Tuple:
        return tuple(self._name)

    @property
    def is_a_star(self) -> bool:
        return self._is_a_star

    @property
    def is_qualified(self) -> bool:
        return len(self._name) == 2 or (len(self._name) == 1 and self._is_a_star)

    @property
    def column_name(self) -> str:
        if len(self._name) == 2:
            return self._name[1]
        else:
            return self._name[0]

    @property
    def table_name(self) -> Optional[str]:
        if len(self._name) == 2 or (self._is_a_star and len(self._name) == 1):
            return self._name[0]

        return None

    def visit_string(self, node):
        self._name.append(node.str.value)

    def visit_a_star(self, node):
        self._is_a_star = True


class RangeVarVisitor(Visitor):
    def __init__(self):
        self._schema_name = None
        self._name = None
        self._alias = None

    @property
    def alias(self):
        if self._alias is not None:
            return self._alias
        elif self._schema_name is not None:
            return "{}.{}".format(self._schema_name, self._name)
        else:
            return self._name

    @property
    def fqdn(self):
        return self._schema_name, self._name

    @property
    def search_string(self):
        return {"schema_like": self._schema_name, "table_like": self._name}

    @property
    def is_qualified(self) -> bool:
        return self._schema_name is not None

    @property
    def name(self) -> str:
        return self._name

    def visit_alias(self, node):
        self._alias = node.aliasname.value

    def visit_range_var(self, node):
        if node.schemaname:
            self._schema_name = node.schemaname.value
        self._name = node.relname.value
        self.visit(node.alias)
