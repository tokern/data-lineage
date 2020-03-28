import networkx as nx


class Graph:
    def __init__(self):
        self._graph = nx.DiGraph()

    @property
    def graph(self):
        return self._graph

    def create_graph(self, queries):
        for query in queries:
            if query.target not in self._graph:
                self._graph.add_node(query.target)

            for node in query.sources:
                if node not in self._graph:
                    self._graph.add_node(node)

                self._graph.add_edge(node, query.target)
