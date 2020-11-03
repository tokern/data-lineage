import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

from data_lineage.catalog.query import Query
from data_lineage.data_lineage import parse, get_dml_queries, create_graph

queries = [
    """
    INSERT INTO page_lookup_nonredirect
            SELECT  page.page_id as redircet_id, page.page_title as redirect_title, page.page_title true_title,
                    page.page_id, page.page_latest
            FROM page LEFT OUTER JOIN redirect ON page.page_id = redirect.rd_from
            WHERE redirect.rd_from IS NULL
    """,
    """
    insert into page_lookup_redirect
            select original_page.page_id redirect_id, original_page.page_title redirect_title,
                    final_page.page_title as true_title, final_page.page_id, final_page.page_latest
            from page final_page join redirect on (redirect.page_title = final_page.page_title)
                join page original_page on (redirect.rd_from = original_page.page_id)
    """,
    """
    INSERT INTO page_lookup
            SELECT redirect_id, redirect_title, true_title, page_id, page_version
            FROM (
                SELECT redirect_id, redirect_title, true_title, page_id, page_version
                FROM page_lookup_nonredirect
                UNION ALL
                SELECT redirect_id, redirect_title, true_title, page_id, page_version
                FROM page_lookup_redirect) u
    """,
    """
           INSERT INTO filtered_pagecounts
           SELECT regexp_replace (reflect ('java.net.URLDecoder','decode', reflect ('java.net.URLDecoder','decode',pvs.page_title)),'^\s*([a-zA-Z0-9]+).*','$1') page_title
                ,SUM (pvs.views) AS total_views, SUM (pvs.bytes_sent) AS total_bytes_sent
            FROM pagecounts as pvs
           WHERE not pvs.page_title LIKE '(MEDIA|SPECIAL||Talk|User|User_talk|Project|Project_talk|File|File_talk|MediaWiki|MediaWiki_talk|Template|Template_talk|Help|Help_talk|Category|Category_talk|Portal|Wikipedia|Wikipedia_talk|upload|Special)\:(.*)' and
                pvs.page_title LIKE '^([A-Z])(.*)' and
                not pvs.page_title LIKE '(.*).(jpg|gif|png|JPG|GIF|PNG|txt|ico)$' and
                pvs.page_title <> '404_error/' and
                pvs.page_title <> 'Main_Page' and
                pvs.page_title <> 'Hypertext_Transfer_Protocol' and
                pvs.page_title <> 'Favicon.ico' and
                pvs.page_title <> 'Search' and
                pvs.dt = '2020-01-01'
          GROUP BY
                regexp_replace (reflect ('java.net.URLDecoder','decode', reflect ('java.net.URLDecoder','decode',pvs.page_title)),'^\s*([a-zA-Z0-9]+).*','$1')
    """,
    """
    INSERT INTO normalized_pagecounts
           SELECT pl.page_id page_id, REGEXP_REPLACE(pl.true_title, '_', ' ') page_title, pl.true_title page_url, views, bytes_sent
           FROM page_lookup pl JOIN filtered_pagecounts fp
           ON fp.page_title = pl.redirect_title where fp.dt='2020-01-01'
    """
]

graph = create_graph(get_dml_queries(parse([Query(q) for q in queries])))
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


def main():

    app.layout = html.Div(children=[
        html.H1(children='Hello Dash'),

        html.Div(children='''
            Lineage: A web application to explore data lineage of all your tables.
        '''),
        html.Label('Enter a table name'),
        dcc.Input(value='', type='text', id='table-input'),
        dcc.Graph(
            id='lineage-graph',
        )
    ])

    app.run_server(debug=True)


@app.callback(
    Output('lineage-graph', 'figure'),
    [Input('table-input', 'value')], prevent_initial_call=True)
def update_figure(table):
    sub_graph = graph.sub_graph((None, table))
    if sub_graph is None:
        raise PreventUpdate
    return sub_graph.fig()


if __name__ == "__main__":
    main()
