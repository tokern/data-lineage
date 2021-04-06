from contextlib import closing

import pytest
import yaml
from dbcat import Catalog, catalog_connection
from dbcat.catalog import CatSource

from data_lineage.parser import parse_queries


@pytest.fixture(scope="session")
def load_queries():
    import json

    with open("test/queries.json", "r") as file:
        queries = json.load(file)

    yield queries


@pytest.fixture(scope="session")
def parse_queries_fixture(load_queries):
    parsed = parse_queries(load_queries)
    yield parsed


postgres_conf = """
catalog:
  type: postgres
  user: piiuser
  password: p11secret
  host: 127.0.0.1
  port: 5432
  database: piidb
"""


@pytest.fixture(scope="session")
def root_connection():
    config = yaml.safe_load(postgres_conf)
    with closing(Catalog(**config["catalog"])) as conn:
        yield conn


@pytest.fixture(scope="session")
def setup_catalog(root_connection):
    with root_connection.engine.connect() as conn:
        conn.execute("CREATE USER catalog_user PASSWORD 'catal0g_passw0rd'")
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(
            "CREATE DATABASE tokern"
        )
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(
            "GRANT ALL PRIVILEGES ON DATABASE tokern TO catalog_user"
        )

    yield root_connection

    with root_connection.engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(
            "DROP DATABASE tokern"
        )

        conn.execution_options(isolation_level="AUTOCOMMIT").execute(
            "DROP USER catalog_user"
        )


catalog_conf = """
catalog:
  type: postgres
  user: catalog_user
  password: catal0g_passw0rd
  host: 127.0.0.1
  port: 5432
  database: tokern
"""


@pytest.fixture(scope="session")
def open_catalog_connection(setup_catalog):
    with closing(catalog_connection(catalog_conf)) as conn:
        yield conn


class File:
    def __init__(self, name: str, path: str, catalog: Catalog):
        self.name = name
        self._path = path
        self._catalog = catalog

    @property
    def path(self):
        return self._path

    def scan(self):
        import json

        with open(self.path, "r") as file:
            content = json.load(file)

        source = self._catalog.add_source(name=content["name"], type=content["type"])
        for s in content["schemata"]:
            schema = self._catalog.add_schema(s["name"], source=source)

            for t in s["tables"]:
                table = self._catalog.add_table(t["name"], schema)

                index = 0
                for c in t["columns"]:
                    self._catalog.add_column(
                        column_name=c["name"],
                        type=c["type"],
                        sort_order=index,
                        table=table,
                    )
                    index += 1


@pytest.fixture(scope="session")
def save_catalog(open_catalog_connection):
    catalog = open_catalog_connection
    scanner = File("test", "test/catalog.json", catalog)
    scanner.scan()
    yield catalog
    session = catalog.scoped_session
    [session.delete(db) for db in session.query(CatSource).all()]
    session.commit()
