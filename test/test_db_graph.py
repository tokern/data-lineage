import logging

import pytest
from dbcat.catalog import ColumnLineage
from networkx import edges, nodes

from data_lineage.parser import create_graph, parse, visit_dml_queries
from data_lineage.parser.dml_visitor import SelectSourceVisitor

logging.basicConfig(level=getattr(logging, "DEBUG"))


def test_no_insert_column_graph(save_catalog):
    catalog = save_catalog
    query = """
        INSERT INTO page_lookup_nonredirect
        SELECT page.page_id as redirect_id, page.page_title as redirect_title,
            page.page_title true_title, page.page_id, page.page_latest
        FROM page
    """

    parsed = parse(query)
    visitor = SelectSourceVisitor(parsed.name)
    parsed.node.accept(visitor)
    visitor.bind(catalog)

    graph = create_graph(catalog, [visitor])

    assert [node.fqdn for node in sorted(list(nodes(graph.graph)))] == [
        ("test", "default", "page", "page_id"),
        ("test", "default", "page", "page_latest"),
        ("test", "default", "page", "page_title"),
        ("test", "default", "page_lookup_nonredirect", "redirect_id"),
        ("test", "default", "page_lookup_nonredirect", "redirect_title"),
        ("test", "default", "page_lookup_nonredirect", "true_title"),
        ("test", "default", "page_lookup_nonredirect", "page_id"),
        ("test", "default", "page_lookup_nonredirect", "page_version"),
    ]

    expected_edges = [
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_nonredirect", "redirect_id"),
        ),
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_nonredirect", "page_id"),
        ),
        (
            ("test", "default", "page", "page_title"),
            ("test", "default", "page_lookup_nonredirect", "redirect_title"),
        ),
        (
            ("test", "default", "page", "page_title"),
            ("test", "default", "page_lookup_nonredirect", "true_title"),
        ),
        (
            ("test", "default", "page", "page_latest"),
            ("test", "default", "page_lookup_nonredirect", "page_version"),
        ),
    ]
    assert [
        (edge[0].fqdn, edge[1].fqdn) for edge in list(edges(graph.graph))
    ] == expected_edges

    session = catalog.scoped_session
    all_edges = session.query(ColumnLineage).all()
    assert set([(e.source.fqdn, e.target.fqdn) for e in all_edges]) == set(
        expected_edges
    )


def test_basic_column_graph(save_catalog):
    catalog = save_catalog

    query = "INSERT INTO page_lookup_nonredirect(page_id, page_version) SELECT page.page_id, page.page_latest FROM page"
    parsed = parse(query, "basic_column_graph")
    visitor = SelectSourceVisitor(parsed.name)
    parsed.node.accept(visitor)
    visitor.bind(catalog)

    graph = create_graph(catalog, [visitor])

    assert [node.fqdn for node in sorted(list(nodes(graph.graph)))] == [
        ("test", "default", "page", "page_id"),
        ("test", "default", "page", "page_latest"),
        ("test", "default", "page_lookup_nonredirect", "page_id"),
        ("test", "default", "page_lookup_nonredirect", "page_version"),
    ]

    expected_edges = [
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_nonredirect", "page_id"),
        ),
        (
            ("test", "default", "page", "page_latest"),
            ("test", "default", "page_lookup_nonredirect", "page_version"),
        ),
    ]

    assert [
        (edge[0].fqdn, edge[1].fqdn) for edge in list(edges(graph.graph))
    ] == expected_edges

    table = catalog.get_table(
        source_name="test", schema_name="default", table_name="page_lookup_nonredirect",
    )
    columns = catalog.get_columns_for_table(
        table, column_names=["page_id", "page_version"]
    )

    assert len(columns) == 2
    session = catalog.scoped_session

    all_edges = (
        session.query(ColumnLineage)
        .filter(ColumnLineage.target_id.in_([c.id for c in columns]))
        .all()
    )
    assert set([(e.source.fqdn, e.target.fqdn) for e in all_edges]) == set(
        expected_edges
    )


@pytest.fixture(scope="module")
def get_graph(save_catalog, parse_queries_fixture):
    catalog = save_catalog
    dml_queries = visit_dml_queries(catalog, parse_queries_fixture)

    graph = create_graph(catalog, dml_queries)

    yield graph, catalog


def test_column_graph(get_graph):
    graph, catalog = get_graph
    assert [node.fqdn for node in sorted(list(nodes(graph.graph)))] == [
        ("test", "default", "filtered_pagecounts", "views"),
        ("test", "default", "filtered_pagecounts", "bytes_sent"),
        ("test", "default", "normalized_pagecounts", "page_id"),
        ("test", "default", "normalized_pagecounts", "page_title"),
        ("test", "default", "normalized_pagecounts", "page_url"),
        ("test", "default", "normalized_pagecounts", "views"),
        ("test", "default", "page", "page_id"),
        ("test", "default", "page", "page_latest"),
        ("test", "default", "page", "page_title"),
        ("test", "default", "page_lookup", "redirect_id"),
        ("test", "default", "page_lookup", "redirect_title"),
        ("test", "default", "page_lookup", "true_title"),
        ("test", "default", "page_lookup", "page_id"),
        ("test", "default", "page_lookup", "page_version"),
        ("test", "default", "page_lookup_nonredirect", "redirect_id"),
        ("test", "default", "page_lookup_nonredirect", "redirect_title"),
        ("test", "default", "page_lookup_nonredirect", "true_title"),
        ("test", "default", "page_lookup_nonredirect", "page_id"),
        ("test", "default", "page_lookup_nonredirect", "page_version"),
        ("test", "default", "page_lookup_redirect", "redirect_id"),
        ("test", "default", "page_lookup_redirect", "redirect_title"),
        ("test", "default", "page_lookup_redirect", "true_title"),
        ("test", "default", "page_lookup_redirect", "page_id"),
        ("test", "default", "page_lookup_redirect", "page_version"),
    ]
    expected_edges = [
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_nonredirect", "redirect_id"),
        ),
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_nonredirect", "page_id"),
        ),
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_redirect", "redirect_id"),
        ),
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_redirect", "page_id"),
        ),
        (
            ("test", "default", "page", "page_title"),
            ("test", "default", "page_lookup_nonredirect", "redirect_title"),
        ),
        (
            ("test", "default", "page", "page_title"),
            ("test", "default", "page_lookup_nonredirect", "true_title"),
        ),
        (
            ("test", "default", "page", "page_title"),
            ("test", "default", "page_lookup_redirect", "redirect_title"),
        ),
        (
            ("test", "default", "page", "page_title"),
            ("test", "default", "page_lookup_redirect", "true_title"),
        ),
        (
            ("test", "default", "page", "page_latest"),
            ("test", "default", "page_lookup_nonredirect", "page_version"),
        ),
        (
            ("test", "default", "page", "page_latest"),
            ("test", "default", "page_lookup_redirect", "page_version"),
        ),
        (
            ("test", "default", "page_lookup_redirect", "redirect_id"),
            ("test", "default", "page_lookup", "redirect_id"),
        ),
        (
            ("test", "default", "page_lookup_redirect", "redirect_title"),
            ("test", "default", "page_lookup", "redirect_title"),
        ),
        (
            ("test", "default", "page_lookup_redirect", "true_title"),
            ("test", "default", "page_lookup", "true_title"),
        ),
        (
            ("test", "default", "page_lookup_redirect", "page_id"),
            ("test", "default", "page_lookup", "page_id"),
        ),
        (
            ("test", "default", "page_lookup_redirect", "page_version"),
            ("test", "default", "page_lookup", "page_version"),
        ),
        (
            ("test", "default", "page_lookup", "true_title"),
            ("test", "default", "normalized_pagecounts", "page_title"),
        ),
        (
            ("test", "default", "page_lookup", "page_id"),
            ("test", "default", "normalized_pagecounts", "page_id"),
        ),
        (
            ("test", "default", "filtered_pagecounts", "views"),
            ("test", "default", "normalized_pagecounts", "page_url"),
        ),
        (
            ("test", "default", "filtered_pagecounts", "bytes_sent"),
            ("test", "default", "normalized_pagecounts", "views"),
        ),
    ]
    assert [
        (edge[0].fqdn, edge[1].fqdn) for edge in list(edges(graph.graph))
    ] == expected_edges


def test_column_sub_graph(get_graph):
    graph, catalog = get_graph

    table = catalog.get_table(
        source_name="test", schema_name="default", table_name="page_lookup_nonredirect",
    )
    sub_graph = graph.sub_graphs(table)

    assert [node.fqdn for node in list(nodes(sub_graph.graph))] == [
        ("test", "default", "page_lookup_nonredirect", "page_version"),
        ("test", "default", "page", "page_latest"),
        ("test", "default", "page_lookup_nonredirect", "page_id"),
        ("test", "default", "page", "page_id"),
        ("test", "default", "page_lookup_nonredirect", "true_title"),
        ("test", "default", "page", "page_title"),
        ("test", "default", "page_lookup_nonredirect", "redirect_title"),
        ("test", "default", "page_lookup_nonredirect", "redirect_id"),
    ]

    assert set(
        [(edge[0].fqdn, edge[1].fqdn) for edge in list(edges(sub_graph.graph))]
    ) == {
        (
            ("test", "default", "page", "page_latest"),
            ("test", "default", "page_lookup_nonredirect", "page_version"),
        ),
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_nonredirect", "page_id"),
        ),
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_nonredirect", "redirect_id"),
        ),
        (
            ("test", "default", "page", "page_title"),
            ("test", "default", "page_lookup_nonredirect", "true_title"),
        ),
        (
            ("test", "default", "page", "page_title"),
            ("test", "default", "page_lookup_nonredirect", "redirect_title"),
        ),
    }


@pytest.mark.skip(reason="Phases has not been thought through for columns")
def test_phases(get_graph):
    graph, catalog = get_graph
    phases = graph._phases()

    assert [[node.fqdn for node in phase] for phase in list(phases)] == [
        [("default", "page"), ("default", "redirect")],
        [("default", "page_lookup_redirect"), ("default", "pagecounts")],
        [("default", "page_lookup"), ("default", "filtered_pagecounts")],
        [("default", "page_lookup_nonredirect"), ("default", "normalized_pagecounts")],
    ]


@pytest.mark.skip(reason="Phases has not been thought through for columns")
def test_node_positions(get_graph):
    graph = get_graph
    graph._set_node_positions(graph._phases())

    assert graph.graph.nodes[("default", "page")]["pos"] == [0, 0]
    assert graph.graph.nodes[("default", "pagecounts")]["pos"] == [1, 1]
    assert graph.graph.nodes[("default", "redirect")]["pos"] == [0, 1]
    assert graph.graph.nodes[("default", "filtered_pagecounts")]["pos"] == [2, 0]
    assert graph.graph.nodes[("default", "page_lookup_nonredirect")]["pos"] == [3, 1]
    assert graph.graph.nodes[("default", "page_lookup_redirect")]["pos"] == [1, 0]
    assert graph.graph.nodes[("default", "page_lookup")]["pos"] == [2, 1]
    assert graph.graph.nodes[("default", "normalized_pagecounts")]["pos"] == [3, 0]


@pytest.mark.skip(reason="Phases has not been thought through for columns")
def test_sub_graph_node_positions(get_graph):
    graph = get_graph
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
