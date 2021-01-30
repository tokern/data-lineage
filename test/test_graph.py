# type: ignore

import pytest
from networkx import edges, nodes

from data_lineage.catalog.catalog import Database
from data_lineage.catalog.sources import FileSource
from data_lineage.data_lineage import get_graph
from data_lineage.graph.graph import ColumnGraph
from data_lineage.parser.parser import parse as parse_single
from data_lineage.visitors.dml_visitor import SelectSourceVisitor


@pytest.fixture
def catalog():
    source = FileSource("test/catalog.json")
    return Database(source.name, **source.read())


@pytest.fixture
def queries():
    return FileSource("test/queries.json")


def test_no_insert_column_graph(catalog):
    query = """
        INSERT INTO page_lookup_nonredirect
        SELECT page.page_id as redirect_id, page.page_title as redirect_title,
            page.page_title true_title, page.page_id, page.page_latest
        FROM page
    """

    parsed = parse_single(query)
    visitor = SelectSourceVisitor()
    parsed.accept(visitor)
    visitor.bind(catalog)

    graph = ColumnGraph()
    graph.create_graph([visitor])
    assert list(nodes(graph.graph)) == [
        ("default", "page_lookup_nonredirect", "redirect_id"),
        ("default", "page_lookup_nonredirect", "redirect_title"),
        ("default", "page_lookup_nonredirect", "true_title"),
        ("default", "page_lookup_nonredirect", "page_id"),
        ("default", "page_lookup_nonredirect", "page_version"),
        ("default", "page", "page_id"),
        ("default", "page", "page_title"),
        ("default", "page", "page_latest"),
    ]
    assert list(edges(graph.graph)) == [
        (
            ("default", "page", "page_id"),
            ("default", "page_lookup_nonredirect", "redirect_id"),
        ),
        (
            ("default", "page", "page_id"),
            ("default", "page_lookup_nonredirect", "page_id"),
        ),
        (
            ("default", "page", "page_title"),
            ("default", "page_lookup_nonredirect", "redirect_title"),
        ),
        (
            ("default", "page", "page_title"),
            ("default", "page_lookup_nonredirect", "true_title"),
        ),
        (
            ("default", "page", "page_latest"),
            ("default", "page_lookup_nonredirect", "page_version"),
        ),
    ]


def test_basic_column_graph(catalog):
    query = "INSERT INTO page_lookup_nonredirect(page_id, page_version) SELECT page.page_id, page.page_latest FROM page"
    parsed = parse_single(query)
    visitor = SelectSourceVisitor()
    parsed.accept(visitor)
    visitor.bind(catalog)

    graph = ColumnGraph()
    graph.create_graph([visitor])
    assert list(nodes(graph.graph)) == [
        ("default", "page_lookup_nonredirect", "page_id"),
        ("default", "page_lookup_nonredirect", "page_version"),
        ("default", "page", "page_id"),
        ("default", "page", "page_latest"),
    ]

    assert list(edges(graph.graph)) == [
        (
            ("default", "page", "page_id"),
            ("default", "page_lookup_nonredirect", "page_id"),
        ),
        (
            ("default", "page", "page_latest"),
            ("default", "page_lookup_nonredirect", "page_version"),
        ),
    ]


def test_graph(catalog, queries):
    graph = get_graph(queries, catalog)

    assert list(nodes(graph.graph)) == [
        ("default", "page_lookup_nonredirect"),
        ("default", "page"),
        ("default", "redirect"),
        ("default", "page_lookup_redirect"),
        ("default", "page_lookup"),
        ("default", "filtered_pagecounts"),
        ("default", "pagecounts"),
        ("default", "normalized_pagecounts"),
    ]

    assert list(edges(graph.graph)) == [
        (("default", "page"), ("default", "page_lookup_nonredirect")),
        (("default", "page"), ("default", "page_lookup_redirect")),
        (("default", "redirect"), ("default", "page_lookup_nonredirect")),
        (("default", "redirect"), ("default", "page_lookup_redirect")),
        (("default", "page_lookup_redirect"), ("default", "page_lookup")),
        (("default", "page_lookup"), ("default", "normalized_pagecounts")),
        (("default", "filtered_pagecounts"), ("default", "normalized_pagecounts")),
        (("default", "pagecounts"), ("default", "filtered_pagecounts")),
    ]


def test_column_graph(catalog, queries):
    graph = get_graph(queries, catalog, True)

    assert list(nodes(graph.graph)) == [
        ("default", "page_lookup_nonredirect", "redirect_id"),
        ("default", "page_lookup_nonredirect", "redirect_title"),
        ("default", "page_lookup_nonredirect", "true_title"),
        ("default", "page_lookup_nonredirect", "page_id"),
        ("default", "page_lookup_nonredirect", "page_version"),
        ("default", "page", "page_id"),
        ("default", "page", "page_title"),
        ("default", "page", "page_latest"),
        ("default", "page_lookup_redirect", "redirect_id"),
        ("default", "page_lookup_redirect", "redirect_title"),
        ("default", "page_lookup_redirect", "true_title"),
        ("default", "page_lookup_redirect", "page_id"),
        ("default", "page_lookup_redirect", "page_version"),
        ("default", "page_lookup", "redirect_id"),
        ("default", "page_lookup", "redirect_title"),
        ("default", "page_lookup", "true_title"),
        ("default", "page_lookup", "page_id"),
        ("default", "page_lookup", "page_version"),
        ("default", "filtered_pagecounts", "group"),
        ("default", "filtered_pagecounts", "page_title"),
        ("default", "filtered_pagecounts", "views"),
        ("default", "filtered_pagecounts", "bytes_sent"),
        ("default", "normalized_pagecounts", "page_id"),
        ("default", "normalized_pagecounts", "page_title"),
        ("default", "normalized_pagecounts", "page_url"),
        ("default", "normalized_pagecounts", "views"),
        ("default", "normalized_pagecounts", "bytes_sent"),
    ]

    assert list(edges(graph.graph)) == [
        (
            ("default", "page", "page_id"),
            ("default", "page_lookup_nonredirect", "redirect_id"),
        ),
        (
            ("default", "page", "page_id"),
            ("default", "page_lookup_nonredirect", "page_id"),
        ),
        (
            ("default", "page", "page_id"),
            ("default", "page_lookup_redirect", "redirect_id"),
        ),
        (
            ("default", "page", "page_id"),
            ("default", "page_lookup_redirect", "page_id"),
        ),
        (
            ("default", "page", "page_title"),
            ("default", "page_lookup_nonredirect", "redirect_title"),
        ),
        (
            ("default", "page", "page_title"),
            ("default", "page_lookup_nonredirect", "true_title"),
        ),
        (
            ("default", "page", "page_title"),
            ("default", "page_lookup_redirect", "redirect_title"),
        ),
        (
            ("default", "page", "page_title"),
            ("default", "page_lookup_redirect", "true_title"),
        ),
        (
            ("default", "page", "page_latest"),
            ("default", "page_lookup_nonredirect", "page_version"),
        ),
        (
            ("default", "page", "page_latest"),
            ("default", "page_lookup_redirect", "page_version"),
        ),
        (
            ("default", "page_lookup_redirect", "redirect_id"),
            ("default", "page_lookup", "redirect_id"),
        ),
        (
            ("default", "page_lookup_redirect", "redirect_title"),
            ("default", "page_lookup", "redirect_title"),
        ),
        (
            ("default", "page_lookup_redirect", "true_title"),
            ("default", "page_lookup", "true_title"),
        ),
        (
            ("default", "page_lookup_redirect", "page_id"),
            ("default", "page_lookup", "page_id"),
        ),
        (
            ("default", "page_lookup_redirect", "page_version"),
            ("default", "page_lookup", "page_version"),
        ),
        (
            ("default", "page_lookup", "true_title"),
            ("default", "normalized_pagecounts", "page_title"),
        ),
        (
            ("default", "page_lookup", "page_id"),
            ("default", "normalized_pagecounts", "page_id"),
        ),
        (
            ("default", "filtered_pagecounts", "views"),
            ("default", "normalized_pagecounts", "page_url"),
        ),
        (
            ("default", "filtered_pagecounts", "bytes_sent"),
            ("default", "normalized_pagecounts", "views"),
        ),
    ]


def test_column_sub_graph(catalog, queries):
    graph = get_graph(queries, catalog, True)
    sub_graph = graph.sub_graphs(("default", "page_lookup_nonredirect"))

    assert list(nodes(sub_graph.graph)) == [
        ("default", "page_lookup_nonredirect", "page_version"),
        ("default", "page", "page_latest"),
        ("default", "page_lookup_nonredirect", "page_id"),
        ("default", "page", "page_id"),
        ("default", "page_lookup_nonredirect", "true_title"),
        ("default", "page", "page_title"),
        ("default", "page_lookup_nonredirect", "redirect_title"),
        ("default", "page_lookup_nonredirect", "redirect_id"),
    ]

    assert list(edges(sub_graph.graph)) == [
        (
            ("default", "page", "page_latest"),
            ("default", "page_lookup_nonredirect", "page_version"),
        ),
        (
            ("default", "page", "page_id"),
            ("default", "page_lookup_nonredirect", "page_id"),
        ),
        (
            ("default", "page", "page_id"),
            ("default", "page_lookup_nonredirect", "redirect_id"),
        ),
        (
            ("default", "page", "page_title"),
            ("default", "page_lookup_nonredirect", "true_title"),
        ),
        (
            ("default", "page", "page_title"),
            ("default", "page_lookup_nonredirect", "redirect_title"),
        ),
    ]


def test_phases(catalog, queries):
    graph = get_graph(queries, catalog)
    phases = graph._phases()

    assert list(phases) == [
        [("default", "page"), ("default", "redirect")],
        [("default", "page_lookup_redirect"), ("default", "pagecounts")],
        [("default", "page_lookup"), ("default", "filtered_pagecounts")],
        [("default", "page_lookup_nonredirect"), ("default", "normalized_pagecounts")],
    ]


def test_node_positions(queries, catalog):
    graph = get_graph(queries, catalog)
    graph._set_node_positions(graph._phases())

    assert graph.graph.nodes[("default", "page")]["pos"] == [0, 0]
    assert graph.graph.nodes[("default", "pagecounts")]["pos"] == [1, 1]
    assert graph.graph.nodes[("default", "redirect")]["pos"] == [0, 1]
    assert graph.graph.nodes[("default", "filtered_pagecounts")]["pos"] == [2, 0]
    assert graph.graph.nodes[("default", "page_lookup_nonredirect")]["pos"] == [3, 1]
    assert graph.graph.nodes[("default", "page_lookup_redirect")]["pos"] == [1, 0]
    assert graph.graph.nodes[("default", "page_lookup")]["pos"] == [2, 1]
    assert graph.graph.nodes[("default", "normalized_pagecounts")]["pos"] == [3, 0]


def test_sub_graph_node_positions(queries, catalog):
    graph = get_graph(queries, catalog, True)
    sub_graph = graph.sub_graphs(("default", "normalized_pagecounts"))
    sub_graph._set_node_positions(sub_graph._phases())

    assert sub_graph.graph.nodes[("default", "normalized_pagecounts", "views")][
        "pos"
    ] == [3, 4]
    assert sub_graph.graph.nodes[("default", "normalized_pagecounts", "page_url")][
        "pos"
    ] == [3, 3]
    assert sub_graph.graph.nodes[("default", "normalized_pagecounts", "page_title")][
        "pos"
    ] == [3, 2]
    assert sub_graph.graph.nodes[("default", "normalized_pagecounts", "page_id")][
        "pos"
    ] == [3, 1]
    assert sub_graph.graph.nodes[("default", "normalized_pagecounts", "bytes_sent")][
        "pos"
    ] == [3, 0]
    assert sub_graph.graph.nodes[("default", "page_lookup", "true_title")]["pos"] == [
        2,
        3,
    ]
    assert sub_graph.graph.nodes[("default", "page_lookup", "page_id")]["pos"] == [2, 2]
    assert sub_graph.graph.nodes[("default", "filtered_pagecounts", "views")][
        "pos"
    ] == [2, 1]
    assert sub_graph.graph.nodes[("default", "filtered_pagecounts", "bytes_sent")][
        "pos"
    ] == [2, 0]
    assert sub_graph.graph.nodes[("default", "page_lookup_redirect", "true_title")][
        "pos"
    ] == [1, 1]
    assert sub_graph.graph.nodes[("default", "page_lookup_redirect", "page_id")][
        "pos"
    ] == [1, 0]
    assert sub_graph.graph.nodes[("default", "page", "page_title")]["pos"] == [0, 1]
    assert sub_graph.graph.nodes[("default", "page", "page_id")]["pos"] == [0, 0]
