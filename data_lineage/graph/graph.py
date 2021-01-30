from collections import deque
from typing import Any, Dict, Tuple

import networkx as nx
from dbcat.catalog.orm import CatColumn

from data_lineage.graph.orm import LineageCatalog
from data_lineage.log_mixin import LogMixin


class Graph(LogMixin):
    def __init__(self, name="Data Lineage Graph"):
        self.name = name
        self._graph = nx.DiGraph()

    @property
    def graph(self):
        return self._graph

    @graph.setter
    def graph(self, new_graph):
        self._graph = new_graph

    def add_node(self, node: Tuple):
        self._graph.add_node(node)

    def add_edge(self, source: Tuple, target: Tuple, payload: Dict[Any, Any]):
        self._graph.add_edge(source, target)

    def create_graph(self, queries):
        for query in queries:
            if query.target_table not in self._graph:
                self.add_node(query.target_table)

            for node in query.source_tables:
                if node not in self._graph:
                    self.add_node(node)

                self.add_edge(node, query.target_table)

    def has_node(self, table):
        return (
            len(
                [
                    tup
                    for tup in self._graph.nodes
                    if all(x == y for x, y in zip(tup, table))
                ]
            )
            > 0
        )

    def sub_graphs(self, table):
        column_dg = nx.DiGraph()

        remaining_nodes = [
            tup for tup in self._graph.nodes if all(x == y for x, y in zip(tup, table))
        ]

        processed_nodes = set()

        while len(remaining_nodes) > 0:
            t = remaining_nodes.pop()
            if t not in processed_nodes:
                column_dg.add_node(t)
            pred = self._graph.predecessors(t)
            for n in pred:
                if n not in processed_nodes:
                    column_dg.add_node(n)
                    remaining_nodes.append(n)
                    processed_nodes.add(n)
                column_dg.add_edge(n, t)

        sub_graph = Graph(name="Data Lineage for {}".format(table))
        sub_graph.graph = column_dg
        return sub_graph

    def sub_graph(self, table):
        tableDG = nx.DiGraph()
        tableDG.add_node(table)

        remaining_nodes = [table]
        processed_nodes = set()
        processed_nodes.add(table)

        while len(remaining_nodes) > 0:
            t = remaining_nodes.pop()
            pred = self._graph.predecessors(t)
            for n in pred:
                if n not in processed_nodes:
                    tableDG.add_node(n)
                    remaining_nodes.append(n)
                    processed_nodes.add(n)
                tableDG.add_edge(n, t)

        sub_graph = Graph(name="Data Lineage for {}".format(table))
        sub_graph.graph = tableDG
        return sub_graph

    def _phases(self):
        remaining_nodes = {}
        for node in self._graph.nodes:
            remaining_nodes[node] = self._graph.out_degree(node)

        phases = deque()
        current_phase = []

        while remaining_nodes:
            for node, out_degree in remaining_nodes.items():
                if out_degree == 0:
                    current_phase.append(node)

            for node in current_phase:
                del remaining_nodes[node]
                for predecessor in self._graph.predecessors(node):
                    remaining_nodes[predecessor] -= 1

            phases.appendleft(current_phase)
            current_phase = []

        return phases

    def _set_node_positions(self, phases):
        x = 0
        for current_phase in phases:
            y = 0
            for n in sorted(current_phase):
                self._graph.nodes[n]["pos"] = [x, y]
                y += 1
            x += 1

    def fig(self):
        import plotly.graph_objects as go

        self._set_node_positions(self._phases())
        edge_x = []
        edge_y = []
        for edge in self._graph.edges():
            x0, y0 = self._graph.nodes[edge[0]]["pos"]
            x1, y1 = self._graph.nodes[edge[1]]["pos"]
            edge_x.append(x0)
            edge_x.append(x1)
            edge_x.append(None)
            edge_y.append(y0)
            edge_y.append(y1)
            edge_y.append(None)

        edge_trace = go.Scatter(
            x=edge_x,
            y=edge_y,
            line=dict(width=0.5, color="#888"),
            hoverinfo="none",
            mode="lines",
        )

        node_x = []
        node_y = []
        node_text = []
        for node in self._graph.nodes():
            x, y = self._graph.nodes[node]["pos"]
            node_x.append(x)
            node_y.append(y)
            node_text.append(node)

        node_trace = go.Scatter(
            x=node_x,
            y=node_y,
            text=node_text,
            mode="markers",
            hoverinfo="text",
            marker=dict(
                showscale=False,
                # colorscale options
                # 'Greys' | 'YlGnBu' | 'Greens' | 'YlOrRd' | 'Bluered' | 'RdBu' |
                # 'Reds' | 'Blues' | 'Picnic' | 'Rainbow' | 'Portland' | 'Jet' |
                # 'Hot' | 'Blackbody' | 'Earth' | 'Electric' | 'Viridis' |
                colorscale="YlGnBu",
                reversescale=True,
                color=[],
                size=10,
                line_width=2,
            ),
        )

        return go.Figure(
            data=[edge_trace, node_trace],
            layout=go.Layout(
                title=self.name,
                titlefont_size=16,
                showlegend=False,
                hovermode="closest",
                margin=dict(b=20, l=5, r=5, t=40),
                annotations=[
                    dict(
                        text="Generated using: <a href='https://tokern.io/data-lineage/'> Tokern Data Lineage</a>",
                        showarrow=False,
                        xref="paper",
                        yref="paper",
                        x=0.005,
                        y=-0.002,
                    )
                ],
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            ),
        )


class ColumnGraph(Graph):
    def create_graph(self, queries):
        for query in queries:
            for column in query.target_columns:
                if column not in self._graph:
                    self.add_node(column)

            for column in query.source_columns:
                if column not in self._graph:
                    self.add_node(column)

            for source, target in zip(query.source_columns, query.target_columns):
                self.add_edge(source, target)


class DbGraph(ColumnGraph):
    def __init__(self, connection: LineageCatalog, name: str = "Lineage"):
        super(DbGraph, self).__init__(name)
        self._connection = connection

    def add_node(self, node: CatColumn):
        self.logger.debug("Add column to graph - {}".format(node))
        self.graph.add_node(node)

    def add_edge(
        self, source: CatColumn, target: CatColumn, payload: Dict[Any, Any] = {}
    ):
        edge, created = self._connection.get_column_edge(source, target, payload)
        self.graph.add_edge(source, target)
        # self.logger.debug(edge)
