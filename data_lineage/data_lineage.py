from data_lineage.catalog.query import Query
from data_lineage.graph.graph import ColumnGraph, Graph
from data_lineage.parser.parser import parse as parse_single
from data_lineage.visitors.dml_visitor import (
    CopyFromVisitor,
    SelectIntoVisitor,
    SelectSourceVisitor,
)


def parse(source):
    queries = Query.get_queries(source)
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
            if len(visitor.source_tables) > 0 and visitor.target_table is not None:
                queries.append(visitor)
                break

    return queries


def create_graph(dml_queries, columnar=False):
    if columnar:
        graph = ColumnGraph()
    else:
        graph = Graph()

    graph.create_graph(dml_queries)

    return graph


def get_graph(queries, catalog=None, columnar=False):
    parsed_queries = parse(queries)
    dml_queries = get_dml_queries(parsed_queries)

    if catalog is not None:
        [query.bind(catalog) for query in dml_queries]

    return create_graph(dml_queries, columnar)
