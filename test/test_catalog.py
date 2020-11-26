from data_lineage.catalog.catalog import Database
from data_lineage.catalog.sources import FileSource


def test_catalog():
    source = FileSource("test/catalog.json")
    catalog = Database(source.name, **source.read())
    assert catalog.name == "test/catalog.json"
    assert len(catalog.schemata) == 1

    default_schema = catalog.schemata[0]
    assert default_schema.name == "default"
    assert len(default_schema.tables) == 8
