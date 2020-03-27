from data_lineage.parser.parser import parse
from data_lineage.visitors.dml_visitor import InsertVisitor


def test_sanity_insert():
    sql = "insert into a select c from b"
    node = parse(sql)
    insert_visitor = InsertVisitor()
    node.accept(insert_visitor)

    assert insert_visitor.target == (None, "a")
    assert insert_visitor.sources == [(None, "b")]
