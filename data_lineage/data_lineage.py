from dbcat.catalog import Catalog

from data_lineage.graph import DbGraph


def load_graph(catalog: Catalog) -> DbGraph:
    graph = DbGraph(catalog)
    graph.load()
    return graph
