from data_lineage.parser.node import AcceptingBase, AcceptingList, AcceptingNode, AcceptingScalar


class Visitor:
    @classmethod
    def visit(cls, obj):
        if isinstance(obj, AcceptingNode):
            cls.visit_node(obj)
        elif isinstance(obj, AcceptingList):
            cls.visit_list(obj)
        elif isinstance(obj, AcceptingScalar):
            cls.visit_scalar(obj)

    @classmethod
    def visit_list(cls, obj):
        pass

    @classmethod
    def visit_node(cls, obj):
        pass

    @classmethod
    def visit_scalar(cls, obj):
        pass
