from data_lineage.graph.graph import Graph
from data_lineage.parser.parser import parse as parse_single
from data_lineage.visitors.dml_visitor import SelectSourceVisitor, SelectIntoVisitor, CopyFromVisitor


def parse(queries):
    parsed = []
    for query in queries:
        parsed.append(parse_single(query.sql))

    return parsed


def get_dml_queries(parsed):
    queries = []
    for node in parsed:
        select_source_visitor = SelectSourceVisitor()
        select_into_visitor = SelectIntoVisitor()
        copy_from_visitor = CopyFromVisitor()

        for visitor in [select_source_visitor, select_into_visitor, copy_from_visitor]:
            node.accept(visitor)
            if len(visitor.sources) > 0 and visitor.target is not None:
                queries.append(visitor)
                break

    return queries


def create_graph(dml_queries):
    graph = Graph()
    graph.create_graph(dml_queries)

    return graph
