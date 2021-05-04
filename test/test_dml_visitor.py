import pytest

from data_lineage.parser import parse, parse_queries
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
    assert insert_visitor.source_tables == sources


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
    assert visitor.source_tables == sources


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
    assert visitor.source_tables == sources


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


def test_insert():
    query = "INSERT INTO page_lookup_nonredirect SELECT page.page_id, page.page_latest FROM page"
    parsed = parse(query)
    visitor = SelectSourceVisitor("test_insert")
    parsed.node.accept(visitor)
    visitor.resolve()

    assert len(visitor.target_columns) == 0
    assert visitor.target_table == (None, "page_lookup_nonredirect")
    assert len(visitor.source_columns) == 2
    assert visitor.source_tables == [(None, "page")]


def test_insert_cols():
    query = "INSERT INTO page_lookup_nonredirect(page_id, latest) SELECT page.page_id, page.page_latest FROM page"
    parsed = parse(query)
    visitor = SelectSourceVisitor("test_insert_cols")
    parsed.node.accept(visitor)
    visitor.resolve()

    assert len(visitor.target_columns) == 2
    assert visitor.target_table == (None, "page_lookup_nonredirect")
    assert len(visitor.source_columns) == 2
    assert visitor.source_tables == [(None, "page")]


def test_syntax_errors():
    queries = [
        "INSERT INTO page_lookup_nonredirect(page_id, latest) SELECT page.page_id, page.page_latest FROM page",
        "select a from table(b)",
        "INSERT INTO page_lookup_nonredirect SELECT page.page_id, page.page_latest FROM page",
    ]

    parsed = parse_queries(queries)

    assert len(parsed) == 2
