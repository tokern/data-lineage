from data_lineage.catalog.sources import FileSource
from data_lineage.data_lineage import get_dml_queries, parse


def test_parser():
    source = FileSource("test/queries.json")
    parsed = parse(source)
    assert len(parsed) == len(source.read())


def test_visitor():
    source = FileSource("test/queries.json")
    parsed = parse(source)

    dml = get_dml_queries(parsed)
    assert len(dml) == len(source.read())

    for d in dml:
        assert len(d.source_tables) > 0 and d.target_table is not None
