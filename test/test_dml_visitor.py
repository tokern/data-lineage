import pytest

from data_lineage.parser.parser import parse
from data_lineage.visitors.dml_visitor import SelectSourceVisitor, SelectIntoVisitor, CopyFromVisitor


@pytest.mark.parametrize("target, sources, sql", [
    ((None, "c"), [(None, "a")], "insert into c select * from a"),
    ((None, "c"), [(None, "a"), (None, "b")], "insert into c select * from a join b on a.id = b.id"),
    ((None, "c"), [(None, "a"), (None, "b")], "insert into c select * from (select * from a join b on a.id = b.id) x"),
    ((None, "c"), [(None, "a"), (None, "b")], "insert into c select * from (select * from a as aa join b on "
                                              "aa.id = b.id) x")
])
def test_sanity_insert(target, sources, sql):
    node = parse(sql)
    insert_visitor = SelectSourceVisitor()
    node.accept(insert_visitor)

    assert insert_visitor.target == target
    assert insert_visitor.sources == sources


@pytest.mark.parametrize("target, sources, sql", [
    ((None, "c"), [(None, "a")], "create table c as select * from a"),
    ((None, "c"), [(None, "a"), (None, "b")], "create table c as select * from a join b on a.id = b.id"),
    ((None, "c"), [(None, "a"), (None, "b")], "create table c as select * from (select * from a join b on a.id = b.id)"
                                              " x"),
    ((None, "c"), [(None, "a"), (None, "b")], "create table c as select * from (select * from a as aa join b on "
                                              "aa.id = b.id) x")
])
def test_sanity_ctas(target, sources, sql):
    node = parse(sql)
    visitor = SelectSourceVisitor()
    node.accept(visitor)

    assert visitor.target == target
    assert visitor.sources == sources


@pytest.mark.parametrize("target, sources, sql", [
    ((None, "c"), [(None, "a"), (None, "b")], "select * into c from a join b on a.id = b.id"),
    ((None, "c"), [(None, "a"), (None, "b")], "select * into c from (select * from a join b on a.id = b.id) x"),
    ((None, "c"), [(None, "a"), (None, "b")], "select * into c from (select * from a as aa join b on aa.id = b.id) x")
])
def test_sanity_select_into(target, sources, sql):
    node = parse(sql)
    visitor = SelectIntoVisitor()
    node.accept(visitor)

    assert visitor.target == target
    assert visitor.sources == sources


@pytest.mark.parametrize("target, query", [
    ((None, "a"), "copy a from stdin"),
    # (("a", "b"), "copy a.b from 's3://bucket/dir' CREDENTIALS '' JSON AS 's3://bucket/schema.json' REGION AS 'region'"
    #             " MAXERROR 1 TRUNCATECOLUMNS TIMEFORMAT 'auto' ACCEPTINVCHARS"),
    # (("a", "b"), "copy a.b(c, d, e) from 's3://bucket/dir' CREDENTIALS '' delimiter ',' REMOVEQUOTES ACCEPTINVCHARS "
    #             "IGNOREHEADER 1")
])
def test_copy(target, query):
    node = parse(query)
    visitor = CopyFromVisitor()
    node.accept(visitor)

    assert visitor.target == target
