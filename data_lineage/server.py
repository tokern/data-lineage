import logging

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate


class Server:
    def __init__(self, port, graph):
        self.port = port
        self.external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]
        self.app = dash.Dash(__name__, external_stylesheets=self.external_stylesheets)
        self.graph = graph

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
        if self.graph.graph.has_node((None, table)):
            logging.info("'{}' found in graph".format(table))
            sub_graph = self.graph.sub_graph((None, table))
            return sub_graph.fig()
        else:
            logging.info("'{}' NOT found in graph".format(table))
            raise PreventUpdate

    def run_server(self):
        self.app.run_server(port=self.port)
