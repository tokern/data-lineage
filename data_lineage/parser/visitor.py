import logging

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

    def visit_copy_stmt(self, node):
        self.visit(node.relation)

    def visit_insert_stmt(self, node):
        self.visit(node.withClause)

        self.visit(node.relation)
        self.visit(node.cols)
        self.visit(node.selectStmt)
        self.visit(node.onConflictClause)
        self.visit(node.returningList)

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
