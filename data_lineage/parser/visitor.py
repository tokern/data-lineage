from typing import Any, Dict, List, Optional, Tuple, Type

from pglast import Node
from pglast.visitors import Skip, Visitor


class ExprVisitor(Visitor):
    def __init__(self, alias: str = None):
        self._alias: Optional[str] = alias
        self._columns: List[Node] = []

    @property
    def alias(self) -> Optional[str]:
        return self._alias

    @property
    def columns(self) -> List[Node]:
        return self._columns

    def visit_FuncCall(self, ancestors, node):
        super().__call__(node.args)

    def visit_TypeCast(self, ancestors, node):
        super().__call__(node.arg)

    def visit_A_Expr(self, ancestors, node):
        super().__call__(node.lexpr)
        super().__call__(node.rexpr)

    def visit_ColumnRef(self, ancestors, node):
        self._columns.append(node)


class RedshiftExprVisitor(ExprVisitor):
    class FuncNameVisitor(Visitor):
        def __init__(self):
            self._name = None

        @property
        def name(self):
            return self._name

        def visit_String(self, ancestors, obj):
            self._name = obj.val

    def visit_FuncCall(self, ancestors, node):
        name_visitor = RedshiftExprVisitor.FuncNameVisitor()
        name_visitor(node.funcname)
        if name_visitor.name == "dateadd":
            super().__call__(node.args[2])
            return Skip


class TableVisitor(Visitor):
    def __init__(self, expr_visitor_clazz: Type[ExprVisitor]):
        self._sources: List[Node] = []
        self._columns: List[ExprVisitor] = []
        self._expr_visitor_clazz = expr_visitor_clazz
        self._with_aliases: Dict[str, Dict[str, Any]] = {}

    @property
    def sources(self) -> List[Node]:
        return self._sources

    @property
    def columns(self) -> List[ExprVisitor]:
        return self._columns

    @property
    def with_aliases(self) -> Dict[str, Dict[str, Any]]:
        return self._with_aliases

    def visit_ResTarget(self, ancestors, node):
        name = None
        if node.name is not None:
            name = node.name

        expr_visitor = self._expr_visitor_clazz(name)
        expr_visitor(node.val)
        self._columns.append(expr_visitor)
        return Skip

    def visit_RangeVar(self, ancestors, node):
        self._sources.append(node)
        return Skip

    def visit_RangeSubselect(self, ancestors, node):
        self._sources.append(node)
        return Skip

    def visit_CommonTableExpr(self, ancestors, node):
        with_alias = node.ctename
        table_visitor = TableVisitor(self._expr_visitor_clazz)
        table_visitor(node.ctequery)

        self._with_aliases[with_alias] = {
            "tables": table_visitor.sources,
            "columns": table_visitor.columns,
        }
        return Skip


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
    def column_name(self) -> Optional[str]:
        if len(self._name) == 2:
            return self._name[1]
        elif len(self._name) == 1:
            return self._name[0]
        return None

    @property
    def table_name(self) -> Optional[str]:
        if len(self._name) == 2 or (self._is_a_star and len(self._name) == 1):
            return self._name[0]

        return None

    def visit_String(self, ancestors, node):
        self._name.append(node.val)

    def visit_A_Star(self, ancestors, node):
        self._is_a_star = True


class RangeVarVisitor(Visitor):
    def __init__(self):
        self._schema_name = None
        self._name = None
        self._alias = None

    @property
    def alias(self) -> Optional[str]:
        if self._alias is not None:
            return self._alias
        elif self._schema_name is not None and self._name is not None:
            return "{}.{}".format(self._schema_name, self._name)
        elif self._name is not None:
            return self._name
        return None

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
    def schema_name(self) -> Optional[str]:
        return self._schema_name

    @property
    def name(self) -> str:
        return self._name

    def visit_Alias(self, ancestors, node):
        self._alias = node.aliasname.lower()

    def visit_RangeVar(self, ancestors, node):
        if node.schemaname:
            self._schema_name = node.schemaname.lower()
        self._name = node.relname.lower()


class RangeSubselectVisitor(Visitor):
    def __init__(self, expr_visitor_clazz: Type[ExprVisitor]):
        self._alias: Optional[str] = None
        self._table_visitor: TableVisitor = TableVisitor(expr_visitor_clazz)

    @property
    def alias(self) -> Optional[str]:
        if self._alias is not None:
            return self._alias
        return None

    @property
    def sources(self) -> List[Node]:
        return self._table_visitor.sources

    @property
    def columns(self) -> List[ExprVisitor]:
        return self._table_visitor.columns

    def visit_Alias(self, ancestors, node):
        self._alias = node.aliasname

    def visit_RangeSubselect(self, ancestors, node):
        super().__call__(node.alias)
        self._table_visitor(node.subquery)
        return Skip
