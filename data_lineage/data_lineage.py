from typing import Tuple

import yaml
from dbcat.catalog.orm import CatColumn

from data_lineage.catalog import ColumnEdge, LineageCatalog
from data_lineage.graph import DbGraph
from data_lineage.parser.dml_visitor import (
    CopyFromVisitor,
    SelectIntoVisitor,
    SelectSourceVisitor,
)


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


def catalog_connection(config: str) -> LineageCatalog:
    config_yaml = yaml.safe_load(config)
    return LineageCatalog(**config_yaml["catalog"])


def add_edge(
    catalog: LineageCatalog, source: CatColumn, target: CatColumn
) -> Tuple[ColumnEdge, bool]:
    return catalog.get_column_edge(source_name=source, target_name=target, payload={})


def load_graph(catalog: LineageCatalog) -> DbGraph:
    graph = DbGraph(catalog)
    graph.load()
    return graph
