from contextlib import closing

import pytest
import yaml
from dbcat import PGCatalog as DbCatalog
from dbcat import catalog_connection, init_db
from dbcat.catalog import CatSource
from fakeredis import FakeStrictRedis

from data_lineage import Analyze, Catalog, Graph, Scan
from data_lineage.parser import parse
from data_lineage.server import create_server


@pytest.fixture(scope="session")
def load_queries():
    import json

    with open("test/queries.json", "r") as file:
        queries = json.load(file)

    yield queries


@pytest.fixture(scope="session")
def parse_queries_fixture(load_queries):
    parsed = [parse(sql=query["query"], name=query["name"]) for query in load_queries]
    yield parsed


postgres_conf = """
catalog:
  user: piiuser
  password: p11secret
  host: 127.0.0.1
  port: 5432
  database: piidb
"""


@pytest.fixture(scope="session")
def root_connection() -> DbCatalog:
    config = yaml.safe_load(postgres_conf)
    with closing(DbCatalog(**config["catalog"])) as conn:
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
  user: catalog_user
  password: catal0g_passw0rd
  host: 127.0.0.1
  port: 5432
  database: tokern
"""


@pytest.fixture(scope="session")
def open_catalog_connection(setup_catalog):
    with closing(catalog_connection(catalog_conf)) as conn:
        init_db(conn)
        yield conn


class File:
    def __init__(self, name: str, path: str, catalog: DbCatalog):
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

        with self._catalog.managed_session:
            source = self._catalog.add_source(
                name=content["name"], source_type=content["source_type"]
            )
            for s in content["schemata"]:
                schema = self._catalog.add_schema(s["name"], source=source)

                for t in s["tables"]:
                    table = self._catalog.add_table(t["name"], schema)

                    index = 0
                    for c in t["columns"]:
                        self._catalog.add_column(
                            column_name=c["name"],
                            data_type=c["data_type"],
                            sort_order=index,
                            table=table,
                        )
                        index += 1


@pytest.fixture(scope="session")
def save_catalog(open_catalog_connection):
    scanner = File("test", "test/catalog.json", open_catalog_connection)
    scanner.scan()
    yield open_catalog_connection
    with open_catalog_connection.managed_session as session:
        [session.delete(db) for db in session.query(CatSource).all()]
        session.commit()


@pytest.fixture(scope="function")
def managed_session(save_catalog):
    with save_catalog.managed_session:
        yield save_catalog


@pytest.fixture(scope="session")
def app(setup_catalog):
    config = yaml.safe_load(catalog_conf)
    app, catalog = create_server(
        config["catalog"], connection=FakeStrictRedis(), is_production=False
    )
    yield app
    catalog.close()


@pytest.fixture(scope="session")
def rest_catalog(live_server, save_catalog):
    yield Catalog("http://{}:{}".format(live_server.host, live_server.port))


@pytest.fixture(scope="session")
def graph_sdk(live_server):
    yield Graph("http://{}:{}".format(live_server.host, live_server.port))


@pytest.fixture(scope="session")
def parser_sdk(live_server):
    yield Analyze("http://{}:{}".format(live_server.host, live_server.port))


@pytest.fixture(scope="session")
def scan_sdk(live_server):
    yield Scan("http://{}:{}".format(live_server.host, live_server.port))
