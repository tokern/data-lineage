from typing import Tuple

import yaml
from dbcat.catalog.orm import CatColumn

from data_lineage.catalog import ColumnEdge, LineageCatalog
from data_lineage.graph import DbGraph


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
