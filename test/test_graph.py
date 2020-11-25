from networkx import edges, nodes

from data_lineage.catalog.catalog import Database
from data_lineage.catalog.sources import FileSource
from data_lineage.data_lineage import create_graph, get_dml_queries, get_graph, parse
from data_lineage.graph.graph import ColumnGraph
from data_lineage.parser.parser import parse as parse_single
from data_lineage.visitors.dml_visitor import SelectSourceVisitor


def test_no_insert_column_graph():
    query = """
        INSERT INTO page_lookup_nonredirect
        SELECT page.page_id as redirect_id, page.page_title as redirect_title,
            page.page_title true_title, page.page_id, page.page_latest
        FROM page
    """
    source = FileSource("test/catalog.json")
    catalog = Database(source.name, **source.read())

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
        ("page", "page_id"),
        ("page", "page_title"),
        ("page", "page_latest"),
    ]
    assert list(edges(graph.graph)) == [
        (("page", "page_id"), ("default", "page_lookup_nonredirect", "redirect_id")),
        (("page", "page_id"), ("default", "page_lookup_nonredirect", "page_id")),
        (
            ("page", "page_title"),
            ("default", "page_lookup_nonredirect", "redirect_title"),
        ),
        (("page", "page_title"), ("default", "page_lookup_nonredirect", "true_title")),
        (
            ("page", "page_latest"),
            ("default", "page_lookup_nonredirect", "page_version"),
        ),
    ]


def test_basic_column_graph():
    query = "INSERT INTO page_lookup_nonredirect(page_id, latest) SELECT page.page_id, page.page_latest FROM page"
    parsed = parse_single(query)
    visitor = SelectSourceVisitor()
    parsed.accept(visitor)

    graph = ColumnGraph()
    graph.create_graph([visitor])
    assert list(nodes(graph.graph)) == [
        "page_id",
        "latest",
        ("page", "page_id"),
        ("page", "page_latest"),
    ]
    assert list(edges(graph.graph)) == [
        (("page", "page_id"), "page_id"),
        (("page", "page_latest"), "latest"),
    ]


def test_graph():
    source = FileSource("test/queries.json")
    parsed = parse(source)

    dml = get_dml_queries(parsed)
    graph = create_graph(dml)

    assert list(nodes(graph.graph)) == [
        (None, "page_lookup_nonredirect"),
        (None, "page"),
        (None, "redirect"),
        (None, "page_lookup_redirect"),
        (None, "page_lookup"),
        (None, "filtered_pagecounts"),
        (None, "pagecounts"),
        (None, "normalized_pagecounts"),
    ]

    assert list(edges(graph.graph)) == [
        ((None, "page_lookup_nonredirect"), (None, "page_lookup")),
        ((None, "page"), (None, "page_lookup_nonredirect")),
        ((None, "page"), (None, "page_lookup_redirect")),
        ((None, "redirect"), (None, "page_lookup_nonredirect")),
        ((None, "redirect"), (None, "page_lookup_redirect")),
        ((None, "page_lookup_redirect"), (None, "page_lookup")),
        ((None, "page_lookup"), (None, "normalized_pagecounts")),
        ((None, "filtered_pagecounts"), (None, "normalized_pagecounts")),
        ((None, "pagecounts"), (None, "filtered_pagecounts")),
    ]


def test_column_graph():
    queries = FileSource("test/queries.json")
    catalog_source = FileSource("test/catalog.json")
    catalog = Database(catalog_source.name, **catalog_source.read())

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


def test_column_sub_graph():
    queries = FileSource("test/queries.json")
    catalog_source = FileSource("test/catalog.json")
    catalog = Database(catalog_source.name, **catalog_source.read())

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


def test_phases():
    source = FileSource("test/queries.json")
    parsed = parse(source)

    graph = create_graph(get_dml_queries(parsed))
    phases = graph._phases()

    assert phases == [
        [(None, "page"), (None, "redirect"), (None, "pagecounts")],
        [
            (None, "page_lookup_nonredirect"),
            (None, "page_lookup_redirect"),
            (None, "filtered_pagecounts"),
        ],
        [(None, "page_lookup")],
        [(None, "normalized_pagecounts")],
    ]


def test_node_positions():
    source = FileSource("test/queries.json")
    graph = get_graph(source)
    graph._set_node_positions(graph._phases())

    assert graph.graph.nodes[(None, "page")]["pos"] == [0, 0]
    assert graph.graph.nodes[(None, "pagecounts")]["pos"] == [0, 1]
    assert graph.graph.nodes[(None, "redirect")]["pos"] == [0, 2]
    assert graph.graph.nodes[(None, "filtered_pagecounts")]["pos"] == [1, 0]
    assert graph.graph.nodes[(None, "page_lookup_nonredirect")]["pos"] == [1, 1]
    assert graph.graph.nodes[(None, "page_lookup_redirect")]["pos"] == [1, 2]
    assert graph.graph.nodes[(None, "page_lookup")]["pos"] == [2, 0]
    assert graph.graph.nodes[(None, "normalized_pagecounts")]["pos"] == [3, 0]
