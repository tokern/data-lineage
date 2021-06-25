import logging
from typing import List, Optional

import inflection

from data_lineage.parser.node import (
    AcceptingList,
    AcceptingNode,
    AcceptingScalar,
    Missing,
)


class Visitor:
    def visit(self, obj):
        if obj is None:
            return None
        if isinstance(obj, AcceptingNode):
            self.visit_node(obj)
        elif isinstance(obj, AcceptingList):
            self.visit_list(obj)
        elif isinstance(obj, AcceptingScalar):
            self.visit_scalar(obj)

    def visit_list(self, obj):
        for item in obj:
            item.accept(self)

    def visit_node(self, node):
        method_name = "visit_{}".format(inflection.underscore(node.node_tag))
        logging.debug("Method name: {}".format(method_name))
        try:
            method = getattr(self, method_name)
            if callable(method):
                method(node)
        except AttributeError:
            logging.debug("{} not found in class {}".format(method_name, node.node_tag))

    def visit_scalar(self, obj):
        pass


class ExprVisitor(Visitor):
    def __init__(self, alias: str = None):
        self._alias: Optional[str] = alias
        self._columns: List[AcceptingNode] = []

    @property
    def alias(self) -> Optional[str]:
        return self._alias

    @property
    def columns(self) -> List[AcceptingNode]:
        return self._columns

    def visit_func_call(self, node):
        self.visit(node.args)

    def visit_type_cast(self, node):
        self.visit(node.arg)

    def visit_a_expr(self, node):
        self.visit(node.lexpr)
        self.visit(node.rexpr)

    def visit_column_ref(self, node):
        self._columns.append(node)


class RedshiftExprVisitor(ExprVisitor):
    class FuncNameVisitor(Visitor):
        def __init__(self):
            self._name = None

        @property
        def name(self):
            return self._name

        def visit_string(self, obj):
            self._name = obj.str.value

    def visit_func_call(self, node):
        name_visitor = RedshiftExprVisitor.FuncNameVisitor()
        name_visitor.visit(node.funcname)
        if name_visitor.name == "dateadd":
            self.visit(node.args[2])
            return

        self.visit(node.args)


class QueryVisitor(Visitor):
    def visit_copy_stmt(self, node):
        self.visit(node.relation)

    def visit_insert_stmt(self, node):
        self.visit(node.withClause)

        self.visit(node.relation)
        self.visit(node.cols)
        self.visit(node.selectStmt)
        self.visit(node.onConflictClause)
        self.visit(node.returningList)

    def visit_with_clause(self, node):
        self.visit(node.ctes)

    def visit_common_table_expr(self, node):
        self.visit(node.ctename)
        self.visit(node.ctequery)

    def visit_join_expr(self, node):
        self.visit(node.larg)
        self.visit(node.rarg)

    def visit_raw_stmt(self, node):
        self.visit(node.stmt)

    def visit_select_stmt(self, node):
        self.visit(node.withClause)
        self.visit(node.intoClause)

        if node.valuesLists:
            self.visit(node.valuesLists)
        elif node.targetList is Missing:
            self.visit(node.larg)
            self.visit(node.op)
            self.visit(node.rarg)
        else:
            self.visit(node.distinctClause)
            self.visit(node.targetList)
            self.visit(node.fromClause)
            self.visit(node.whereClause)
            self.visit(node.groupClause)
            self.visit(node.havingClause)
            self.visit(node.windowClause)
            self.visit(node.sortClause)
            self.visit(node.limitCount)
            self.visit(node.limitOffset)
            self.visit(node.lockingClause)

    def visit_into_clause(self, node):
        self.visit(node.colNames)
        self.visit(node.rel)
        self.visit(node.tableSpaceName)

    def visit_create_table_as_stmt(self, node):
        self.visit(node.into)
        self.visit(node.query)

    def visit_range_subselect(self, node):
        self.visit(node.subquery)
        self.visit(node.alias)

    def visit_res_target(self, node):
        self.visit(node.val)

    def visit_column_ref(self, node):
        self.visit(node.fields)
