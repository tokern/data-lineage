import logging

import pytest
from dbcat.catalog import ColumnLineage
from networkx import edges

from data_lineage import load_graph
from data_lineage.parser import extract_lineage, parse, visit_dml_query
from data_lineage.parser.dml_visitor import SelectSourceVisitor

logging.basicConfig(level=getattr(logging, "DEBUG"))


def test_no_insert_column_graph(save_catalog, graph_sdk):
    catalog = save_catalog
    query = """
        INSERT INTO page_lookup_nonredirect
        SELECT page.page_id as redirect_id, page.page_title as redirect_title,
            page.page_title true_title, page.page_id, page.page_latest
        FROM page
    """

    parsed = parse(
        query, name="LOAD page_lookup_nonredirect-test_no_insert_column_graph"
    )
    visitor = SelectSourceVisitor(parsed.name)
    parsed.node.accept(visitor)
    visitor.bind(catalog)

    job_execution = extract_lineage(catalog, visitor, parsed)
    graph = load_graph(graph_sdk, [job_execution.job_id])

    assert sorted([node[1]["name"] for node in list(graph.graph.nodes(data=True))]) == [
        "LOAD page_lookup_nonredirect-test_no_insert_column_graph",
        "test.default.page.page_id",
        "test.default.page.page_latest",
        "test.default.page.page_title",
        "test.default.page_lookup_nonredirect.page_id",
        "test.default.page_lookup_nonredirect.page_version",
        "test.default.page_lookup_nonredirect.redirect_id",
        "test.default.page_lookup_nonredirect.redirect_title",
        "test.default.page_lookup_nonredirect.true_title",
    ]

    expected_edges = [
        ("column:4", "task:1"),
        ("task:1", "column:9"),
        ("task:1", "column:10"),
        ("task:1", "column:11"),
        ("task:1", "column:12"),
        ("task:1", "column:13"),
        ("column:6", "task:1"),
        ("column:5", "task:1"),
    ]
    assert [(edge[0], edge[1]) for edge in list(edges(graph.graph))] == expected_edges

    expected_db_edges = [
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
    session = catalog.scoped_session
    all_edges = session.query(ColumnLineage).all()
    assert set([(e.source.fqdn, e.target.fqdn) for e in all_edges]) == set(
        expected_db_edges
    )


def test_basic_column_graph(save_catalog, graph_sdk):
    catalog = save_catalog

    query = "INSERT INTO page_lookup_nonredirect(page_id, page_version) SELECT page.page_id, page.page_latest FROM page"
    parsed = parse(query, "basic_column_graph")
    visitor = SelectSourceVisitor(parsed.name)
    parsed.node.accept(visitor)
    visitor.bind(catalog)

    job_execution = extract_lineage(catalog, visitor, parsed)
    graph = load_graph(graph_sdk, [job_execution.job_id])

    assert sorted([node[1]["name"] for node in list(graph.graph.nodes(data=True))]) == [
        "basic_column_graph",
        "test.default.page.page_id",
        "test.default.page.page_latest",
        "test.default.page_lookup_nonredirect.page_id",
        "test.default.page_lookup_nonredirect.page_version",
    ]

    expected_edges = [
        ("column:4", "task:2"),
        ("task:2", "column:12"),
        ("task:2", "column:13"),
        ("column:5", "task:2"),
    ]

    assert [(edge[0], edge[1]) for edge in list(edges(graph.graph))] == expected_edges

    table = catalog.get_table(
        source_name="test", schema_name="default", table_name="page_lookup_nonredirect",
    )
    columns = catalog.get_columns_for_table(
        table, column_names=["page_id", "page_version"]
    )

    assert len(columns) == 2
    session = catalog.scoped_session

    expected_db_edges = [
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_nonredirect", "page_id"),
        ),
        (
            ("test", "default", "page", "page_latest"),
            ("test", "default", "page_lookup_nonredirect", "page_version"),
        ),
    ]

    all_edges = (
        session.query(ColumnLineage)
        .filter(ColumnLineage.target_id.in_([c.id for c in columns]))
        .all()
    )
    assert set([(e.source.fqdn, e.target.fqdn) for e in all_edges]) == set(
        expected_db_edges
    )


@pytest.fixture(scope="module")
def get_graph(save_catalog, parse_queries_fixture, graph_sdk):
    catalog = save_catalog
    job_ids = []
    for parsed in parse_queries_fixture:
        visitor = visit_dml_query(catalog, parsed)
        job_execution = extract_lineage(catalog, visitor, parsed)
        job_ids.append(job_execution.job_id)
    graph = load_graph(graph_sdk, job_ids)
    yield graph, catalog


def test_column_graph(get_graph):
    graph, catalog = get_graph
    assert sorted([node[1]["name"] for node in list(graph.graph.nodes(data=True))]) == [
        "LOAD normalized_pagecounts",
        "LOAD page_lookup",
        "LOAD page_lookup_nonredirect",
        "LOAD page_lookup_redirect",
        "test.default.filtered_pagecounts.bytes_sent",
        "test.default.filtered_pagecounts.views",
        "test.default.normalized_pagecounts.page_id",
        "test.default.normalized_pagecounts.page_title",
        "test.default.normalized_pagecounts.page_url",
        "test.default.normalized_pagecounts.views",
        "test.default.page.page_id",
        "test.default.page.page_latest",
        "test.default.page.page_title",
        "test.default.page_lookup.page_id",
        "test.default.page_lookup.page_version",
        "test.default.page_lookup.redirect_id",
        "test.default.page_lookup.redirect_title",
        "test.default.page_lookup.true_title",
        "test.default.page_lookup_nonredirect.page_id",
        "test.default.page_lookup_nonredirect.page_version",
        "test.default.page_lookup_nonredirect.redirect_id",
        "test.default.page_lookup_nonredirect.redirect_title",
        "test.default.page_lookup_nonredirect.true_title",
        "test.default.page_lookup_redirect.page_id",
        "test.default.page_lookup_redirect.page_version",
        "test.default.page_lookup_redirect.redirect_id",
        "test.default.page_lookup_redirect.redirect_title",
        "test.default.page_lookup_redirect.true_title",
    ]
    # expected_edges = [
    #     ("column:4", "task:1"),
    #     ("column:4", "task:3"),
    #     ("task:1", "column:9"),
    #     ("task:1", "column:10"),
    #     ("task:1", "column:11"),
    #     ("task:1", "column:12"),
    #     ("task:1", "column:13"),
    #     ("column:6", "task:1"),
    #     ("column:6", "task:3"),
    #     ("column:5", "task:1"),
    #     ("column:5", "task:3"),
    #     ("column:14", "task:4"),
    #     ("task:3", "column:14"),
    #     ("task:3", "column:15"),
    #     ("task:3", "column:16"),
    #     ("task:3", "column:17"),
    #     ("task:3", "column:18"),
    #     ("column:15", "task:4"),
    #     ("column:16", "task:4"),
    #     ("column:17", "task:4"),
    #     ("column:18", "task:4"),
    #     ("task:4", "column:19"),
    #     ("task:4", "column:20"),
    #     ("task:4", "column:21"),
    #     ("task:4", "column:22"),
    #     ("task:4", "column:23"),
    #     ("column:21", "task:6"),
    #     ("column:22", "task:6"),
    #     ("task:6", "column:28"),
    #     ("task:6", "column:29"),
    #     ("task:6", "column:30"),
    #     ("task:6", "column:31"),
    #     ("column:26", "task:6"),
    #     ("column:27", "task:6"),
    # ]


#    assert [
#        (edge[0], edge[1]) for edge in list(edges(graph.graph))
#    ] == expected_edges
