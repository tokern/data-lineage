import logging
from typing import Dict, List

import networkx as nx


class LineageGraph:
    def __init__(
        self,
        nodes: List[Dict[str, str]],
        edges: List[Dict[str, str]],
        name: str = "Lineage",
    ):
        self.name = name
        self._graph = nx.DiGraph()
        for node in nodes:
            node_id = node["id"]
            node_attributes = {"name": node["name"], "type": node["type"]}
            logging.debug("Add Node: {}, {}".format(node_id, node_attributes))
            self._graph.add_node(node_id, **node_attributes)

        for edge in edges:
            logging.debug("Edge: <{}>, <{}>".format(edge["source"], edge["target"]))
            self._graph.add_edge(edge["source"], edge["target"])

    @property
    def graph(self):
        return self._graph

    @graph.setter
    def graph(self, new_graph):
        self._graph = new_graph
