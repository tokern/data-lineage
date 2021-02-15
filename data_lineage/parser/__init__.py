from typing import List

from pglast.parser import parse_sql

from data_lineage.catalog import LineageCatalog
from data_lineage.graph import DbGraph
from data_lineage.parser.dml_visitor import (
    CopyFromVisitor,
    DmlVisitor,
    SelectIntoVisitor,
    SelectSourceVisitor,
)
from data_lineage.parser.node import AcceptingNode


def parse(sql: str) -> AcceptingNode:
    return AcceptingNode(parse_sql(sql))


def parse_queries(queries: List[str]) -> List[AcceptingNode]:
    return [parse(query) for query in queries]


def visit_dml_queries(
    catalog: LineageCatalog, parsed: List[AcceptingNode]
) -> List[DmlVisitor]:
    queries = []
    for parsed_node in parsed:
        select_source_visitor = SelectSourceVisitor()
        select_into_visitor = SelectIntoVisitor()
        copy_from_visitor = CopyFromVisitor()

        for visitor in [select_source_visitor, select_into_visitor, copy_from_visitor]:
            parsed_node.accept(visitor)
            if len(visitor.source_tables) > 0 and visitor.target_table is not None:
                visitor.bind(catalog)
                queries.append(visitor)
                break

    return queries


def create_graph(catalog: LineageCatalog, visited_queries: List[DmlVisitor]) -> DbGraph:
    graph = DbGraph(catalog)
    for query in visited_queries:
        for source, target in zip(query.source_columns, query.target_columns):
            graph.add_edge(source, target)

    return graph
