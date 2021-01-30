import logging
from contextlib import closing

from networkx import edges, nodes

from data_lineage.graph.graph import DbGraph
from data_lineage.graph.orm import ColumnEdge
from data_lineage.parser.parser import parse as parse_single  # type: ignore
from data_lineage.visitors.dml_visitor import SelectSourceVisitor  # type: ignore

logging.basicConfig(level=getattr(logging, "DEBUG"))


def test_no_insert_column_graph(save_catalog):
    file_catalog, catalog = save_catalog
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

    graph = DbGraph(catalog)
    graph.create_graph([visitor])
    assert [node.fqdn for node in list(nodes(graph.graph))] == [
        ("test", "default", "page_lookup_nonredirect", "redirect_id"),
        ("test", "default", "page_lookup_nonredirect", "redirect_title"),
        ("test", "default", "page_lookup_nonredirect", "true_title"),
        ("test", "default", "page_lookup_nonredirect", "page_id"),
        ("test", "default", "page_lookup_nonredirect", "page_version"),
        ("test", "default", "page", "page_id"),
        ("test", "default", "page", "page_title"),
        ("test", "default", "page", "page_latest"),
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

    with closing(catalog.session) as session:
        all_edges = session.query(ColumnEdge).all()
        assert set([(e.source.fqdn, e.target.fqdn) for e in all_edges]) == set(
            expected_edges
        )


def test_basic_column_graph(save_catalog):
    file_catalog, catalog = save_catalog

    query = "INSERT INTO page_lookup_nonredirect(page_id, page_version) SELECT page.page_id, page.page_latest FROM page"
    parsed = parse_single(query)
    visitor = SelectSourceVisitor()
    parsed.accept(visitor)
    visitor.bind(catalog)

    graph = DbGraph(catalog)
    graph.create_graph([visitor])
    assert [node.fqdn for node in list(nodes(graph.graph))] == [
        ("test", "default", "page_lookup_nonredirect", "page_id"),
        ("test", "default", "page_lookup_nonredirect", "page_version"),
        ("test", "default", "page", "page_id"),
        ("test", "default", "page", "page_latest"),
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
        database_name="test",
        schema_name="default",
        table_name="page_lookup_nonredirect",
    )
    columns = catalog.get_columns_for_table(
        table, column_names=["page_id", "page_version"]
    )

    assert len(columns) == 2
    with closing(catalog.session) as session:
        all_edges = (
            session.query(ColumnEdge)
            .filter(ColumnEdge.target_id.in_([c.id for c in columns]))
            .all()
        )
        assert set([(e.source.fqdn, e.target.fqdn) for e in all_edges]) == set(
            expected_edges
        )
