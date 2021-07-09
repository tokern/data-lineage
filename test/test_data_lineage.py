from data_lineage.parser import analyze_dml_query


def test_parser(parse_queries_fixture):
    assert len(parse_queries_fixture) == 5


def test_visitor(save_catalog, parse_queries_fixture):
    catalog = save_catalog
    with catalog.managed_session:
        source = catalog.get_source("test")

        dml = [
            analyze_dml_query(catalog, parsed, source)
            for parsed in parse_queries_fixture
        ]
        assert len(dml) == 5

        for d in dml:
            assert len(d.source_tables) > 0 and d.target_table is not None
