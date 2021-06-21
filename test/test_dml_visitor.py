import pytest

from data_lineage.parser import parse, parse_queries, visit_dml_query
from data_lineage.parser.dml_visitor import (
    CopyFromVisitor,
    SelectIntoVisitor,
    SelectSourceVisitor,
)


@pytest.mark.parametrize(
    "target, sources, sql",
    [
        ((None, "c"), [(None, "a")], "insert into c select x,y from a"),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "insert into c select x,y from a join b on a.id = b.id",
        ),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "insert into c select x,y from a join b on a.id = b.id",
        ),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "insert into c select x,y from a as aa join b on " "aa.id = b.id",
        ),
    ],
)
def test_sanity_insert(target, sources, sql):
    parsed = parse(sql)
    insert_visitor = SelectSourceVisitor("test_sanity_insert")
    parsed.node.accept(insert_visitor)
    insert_visitor.resolve()

    assert insert_visitor.target_table == target
    assert insert_visitor.select_tables == sources


@pytest.mark.parametrize(
    "target, sources, sql",
    [
        ((None, "c"), [(None, "a")], "create table c as select x,y from a"),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "create table c as select x,y from a join b on a.id = b.id",
        ),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "create table c as select x,y from a join b on a.id = b.id",
        ),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "create table c as select x,y from a as aa join b on aa.id = b.id",
        ),
    ],
)
def test_sanity_ctas(target, sources, sql):
    parsed = parse(sql)
    visitor = SelectSourceVisitor("test_sanity_ctas")
    parsed.node.accept(visitor)
    visitor.resolve()
    assert visitor.target_table == target
    assert visitor.select_tables == sources


@pytest.mark.parametrize(
    "target, sources, sql",
    [
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "select x,y into c from a join b on a.id = b.id",
        ),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "select x,y into c from a join b on a.id = b.id",
        ),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "select x,y into c from a as aa join b on aa.id = b.id",
        ),
    ],
)
def test_sanity_select_into(target, sources, sql):
    parsed = parse(sql)
    visitor = SelectIntoVisitor("test_sanity_select_into")
    parsed.node.accept(visitor)
    visitor.resolve()

    assert visitor.target_table == target
    assert visitor.select_tables == sources


@pytest.mark.parametrize(
    "target, query",
    [
        ((None, "a"), "copy a from stdin"),
        # (("a", "b"), "copy a.b from 's3://bucket/dir' CREDENTIALS '' JSON AS 's3://bucket/schema.json' REGION AS 'region'"
        #             " MAXERROR 1 TRUNCATECOLUMNS TIMEFORMAT 'auto' ACCEPTINVCHARS"),
        # (("a", "b"), "copy a.b(c, d, e) from 's3://bucket/dir' CREDENTIALS '' delimiter ',' REMOVEQUOTES ACCEPTINVCHARS "
        #             "IGNOREHEADER 1")
    ],
)
def test_copy(target, query):
    parsed = parse(query)
    visitor = CopyFromVisitor("test_copy")
    parsed.node.accept(visitor)
    visitor.resolve()

    assert visitor.target_table == target


@pytest.mark.parametrize(
    "query",
    [
        "INSERT INTO page_lookup SELECT plr.redirect_id, plr.redirect_title, plr.true_title, plr.page_id, plr.page_version FROM page_lookup_redirect plr",
        "INSERT INTO page_lookup SELECT redirect_id, redirect_title, true_title, page_id, page_version FROM page_lookup_redirect",
        "INSERT INTO page_lookup SELECT page_lookup_redirect.* FROM page_lookup_redirect",
        "INSERT INTO page_lookup SELECT * FROM page_lookup_redirect",
    ],
)
def test_insert(save_catalog, query):
    parsed = parse(query)
    visitor = visit_dml_query(save_catalog, parsed)
    assert visitor is not None

    assert len(visitor.target_columns) == 5
    assert visitor.target_table.fqdn == ("test", "default", "page_lookup")
    assert len(visitor.source_columns) == 5
    assert [table.fqdn for table in visitor.source_tables] == [
        ("test", "default", "page_lookup_redirect")
    ]


def test_insert_cols(save_catalog):
    query = "INSERT INTO page_lookup_nonredirect(page_id, page_version) SELECT page.page_id, page.page_latest FROM page"
    parsed = parse(query)
    visitor = visit_dml_query(save_catalog, parsed)
    assert visitor is not None

    assert len(visitor.target_columns) == 2
    assert visitor.target_table.fqdn == ("test", "default", "page_lookup_nonredirect")
    assert len(visitor.source_columns) == 2
    assert [table.fqdn for table in visitor.source_tables] == [
        ("test", "default", "page")
    ]


def test_insert_with_join(save_catalog):
    query = "insert into page_lookup_redirect select original_page.page_id redirect_id, original_page.page_title redirect_title, final_page.page_title as true_title, final_page.page_id, final_page.page_latest from page final_page join redirect on (redirect.page_title = final_page.page_title) join page original_page on (redirect.rd_from = original_page.page_id)"
    parsed = parse(query)
    visitor = visit_dml_query(save_catalog, parsed)
    assert visitor is not None

    assert len(visitor.target_columns) == 5
    assert visitor.target_table.fqdn == ("test", "default", "page_lookup_redirect")
    assert len(visitor.source_columns) == 5
    assert sorted([table.fqdn for table in visitor.source_tables]) == [
        ("test", "default", "page"),
        ("test", "default", "redirect"),
    ]


@pytest.mark.parametrize(
    "query",
    [
        "with pln as (select redirect_title, true_title, page_id, page_version from page_lookup_nonredirect) insert into page_lookup_redirect (redirect_title, true_title, page_id, page_version) select redirect_title, true_title, page_id, page_version from pln;",
        "with pln as (select * from page_lookup_nonredirect) insert into page_lookup_redirect (redirect_title, true_title, page_id, page_version) select redirect_title, true_title, page_id, page_version from pln;",
        "with pln as (select redirect_title, true_title, page_id, page_version from page_lookup_nonredirect) insert into page_lookup_redirect (redirect_title, true_title, page_id, page_version) select * from pln;",
    ],
)
def test_with_clause(save_catalog, query):
    parsed = parse(query)
    visitor = visit_dml_query(save_catalog, parsed)
    assert visitor is not None

    assert len(visitor.target_columns) == 4
    assert visitor.target_table.fqdn == ("test", "default", "page_lookup_redirect")
    assert len(visitor.source_columns) == 4
    assert [table.fqdn for table in visitor.source_tables] == [
        ("test", "default", "page_lookup_nonredirect")
    ]


def test_syntax_errors():
    queries = [
        "INSERT INTO page_lookup_nonredirect(page_id, latest) SELECT page.page_id, page.page_latest FROM page",
        "select a from table(b)",
        "INSERT INTO page_lookup_nonredirect SELECT page.page_id, page.page_latest FROM page",
    ]

    parsed = parse_queries(queries)

    assert len(parsed) == 2
