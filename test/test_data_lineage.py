from networkx import edges, nodes

from data_lineage.catalog.sources import FileSource
from data_lineage.data_lineage import create_graph, get_dml_queries, get_graph, parse


def test_parser():
    source = FileSource("test/queries.json")
    parsed = parse(source)
    assert len(parsed) == len(source.get_queries())


def test_visitor():
    source = FileSource("test/queries.json")
    parsed = parse(source)

    dml = get_dml_queries(parsed)
    assert len(dml) == len(source.get_queries())

    for d in dml:
        assert len(d.sources) > 0 and d.target is not None


def test_graph():
    source = FileSource("test/queries.json")
    parsed = parse(source)

    dml = get_dml_queries(parsed)
    graph = create_graph(dml)

    assert list(nodes(graph.graph)) == [
        (None, "page_lookup_nonredirect"),
        (None, "page"),
        (None, "redirect"),
        (None, "page_lookup_redirect"),
        (None, "page_lookup"),
        (None, "filtered_pagecounts"),
        (None, "pagecounts"),
        (None, "normalized_pagecounts"),
    ]

    assert list(edges(graph.graph)) == [
        ((None, "page_lookup_nonredirect"), (None, "page_lookup")),
        ((None, "page"), (None, "page_lookup_nonredirect")),
        ((None, "page"), (None, "page_lookup_redirect")),
        ((None, "redirect"), (None, "page_lookup_nonredirect")),
        ((None, "redirect"), (None, "page_lookup_redirect")),
        ((None, "page_lookup_redirect"), (None, "page_lookup")),
        ((None, "page_lookup"), (None, "normalized_pagecounts")),
        ((None, "filtered_pagecounts"), (None, "normalized_pagecounts")),
        ((None, "pagecounts"), (None, "filtered_pagecounts")),
    ]


def test_phases():
    source = FileSource("test/queries.json")
    parsed = parse(source)

    graph = create_graph(get_dml_queries(parsed))
    phases = graph._phases()

    assert phases == [
        [(None, "page"), (None, "redirect"), (None, "pagecounts")],
        [
            (None, "page_lookup_nonredirect"),
            (None, "page_lookup_redirect"),
            (None, "filtered_pagecounts"),
        ],
        [(None, "page_lookup")],
        [(None, "normalized_pagecounts")],
    ]


def test_node_positions():
    source = FileSource("test/queries.json")
    graph = get_graph(source)
    graph._set_node_positions(graph._phases())

    assert graph.graph.nodes[(None, "page")]["pos"] == [0, 0]
    assert graph.graph.nodes[(None, "pagecounts")]["pos"] == [0, 1]
    assert graph.graph.nodes[(None, "redirect")]["pos"] == [0, 2]
    assert graph.graph.nodes[(None, "filtered_pagecounts")]["pos"] == [1, 0]
    assert graph.graph.nodes[(None, "page_lookup_nonredirect")]["pos"] == [1, 1]
    assert graph.graph.nodes[(None, "page_lookup_redirect")]["pos"] == [1, 2]
    assert graph.graph.nodes[(None, "page_lookup")]["pos"] == [2, 0]
    assert graph.graph.nodes[(None, "normalized_pagecounts")]["pos"] == [3, 0]
