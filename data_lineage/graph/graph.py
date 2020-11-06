import networkx as nx


class Graph:
    def __init__(self, name="Data Lineage Graph"):
        self.name = name
        self._graph = nx.DiGraph()

    @property
    def graph(self):
        return self._graph

    @graph.setter
    def graph(self, new_graph):
        self._graph = new_graph

    def create_graph(self, queries):
        for query in queries:
            if query.target not in self._graph:
                self._graph.add_node(query.target)

            for node in query.sources:
                if node not in self._graph:
                    self._graph.add_node(node)

                self._graph.add_edge(node, query.target)

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
            remaining_nodes[node] = self._graph.in_degree(node)

        phases = []
        current_phase = []

        while remaining_nodes:
            for node, in_degree in remaining_nodes.items():
                if in_degree == 0:
                    current_phase.append(node)

            for node in current_phase:
                del remaining_nodes[node]
                for successor in self._graph.successors(node):
                    remaining_nodes[successor] -= 1

            phases.append(current_phase)
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
