from data_lineage.data_lineage import get_dml_queries


def test_parser(parse_queries):
    assert len(parse_queries) == 5


def test_visitor(parse_queries):
    dml = get_dml_queries(parse_queries)
    assert len(dml) == 5

    for d in dml:
        assert len(d.source_tables) > 0 and d.target_table is not None
