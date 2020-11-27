import logging

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

from data_lineage.log_mixin import LogMixin


class Server(LogMixin):
    def __init__(self, port, graph):
        self.port = port
        self.external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]
        self.app = dash.Dash(__name__, external_stylesheets=self.external_stylesheets)
        self.graph = graph
        from networkx import nodes

        self.logger.debug(nodes(graph.graph))

        self.app.title = "Tokern Lineage Explorer"
        self.app.layout = html.Div(
            children=[
                html.H1(children="Hello Explorer!"),
                html.Div(
                    children="""
                Lineage: Explore data lineage of all your tables. Enter a table below
            """
                ),
                html.Label("Enter a table name"),
                dcc.Input(value="", type="text", id="table-input"),
                dcc.Graph(id="lineage-graph",),
            ]
        )

        self.app.callback(
            Output("lineage-graph", "figure"),
            [Input("table-input", "value")],
            prevent_initial_call=True,
        )(self.update_figure)

    def update_figure(self, table):
        logging.info(table)
        fqdn = tuple(table.split("."))
        if self.graph.has_node(fqdn):
            self.logger.debug("'{}' found in graph".format(table))
            sub_graph = self.graph.sub_graphs(fqdn)
            return sub_graph.fig()
        else:
            self.logger.debug("'{}' NOT found in graph".format(table))
            raise PreventUpdate

    def run_server(self):
        self.app.run_server(port=self.port)
