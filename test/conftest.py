from contextlib import closing

import pytest
import yaml
from dbcat.catalog.orm import Catalog, CatDatabase
from dbcat.scanners.json import File

from data_lineage.catalog import LineageCatalog
from data_lineage.parser import parse  # type: ignore


@pytest.fixture(scope="session")
def load_catalog():
    scanner = File("test", "test/catalog.json")
    yield scanner.scan()


@pytest.fixture(scope="session")
def load_queries():
    import json

    with open("test/queries.json", "r") as file:
        queries = json.load(file)

    yield queries


@pytest.fixture(scope="session")
def parse_queries(load_queries):
    parsed = []
    for query in load_queries:
        parsed.append(parse(query))

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
def catalog_connection(setup_catalog):
    config = yaml.safe_load(catalog_conf)
    with closing(LineageCatalog(**config["catalog"])) as conn:
        yield conn


@pytest.fixture(scope="session")
def save_catalog(load_catalog, catalog_connection):
    file_catalog = load_catalog
    catalog_connection.save_catalog(file_catalog)
    yield file_catalog, catalog_connection
    with closing(catalog_connection.session) as session:
        [session.delete(db) for db in session.query(CatDatabase).all()]
