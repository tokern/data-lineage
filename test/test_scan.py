import psycopg2
import pytest
from fakeredis import FakeStrictRedis
from rq import Queue

pii_data_script = """
create table no_pii(a text, b text);
insert into no_pii values ('abc', 'def');
insert into no_pii values ('xsfr', 'asawe');

create table partial_pii(a text, b text);
insert into partial_pii values ('917-908-2234', 'plkj');
insert into partial_pii values ('215-099-2234', 'sfrf');

create table full_pii(name text, location text);
insert into full_pii values ('Jonathan Smith', 'Virginia');
insert into full_pii values ('Chase Ryan', 'Chennai');

"""


pii_data_load = [
    "create table no_pii(a text, b text)",
    "insert into no_pii values ('abc', 'def')",
    "insert into no_pii values ('xsfr', 'asawe')",
    "create table partial_pii(a text, b text)",
    "insert into partial_pii values ('917-908-2234', 'plkj')",
    "insert into partial_pii values ('215-099-2234', 'sfrf')",
    "create table full_pii(name text, location text)",
    "insert into full_pii values ('Jonathan Smith', 'Virginia')",
    "insert into full_pii values ('Chase Ryan', 'Chennai')",
]

pii_data_drop = ["DROP TABLE full_pii", "DROP TABLE partial_pii", "DROP TABLE no_pii"]


def pg_conn():
    return (
        psycopg2.connect(
            host="127.0.0.1", user="piiuser", password="p11secret", database="piidb"
        ),
        "public",
    )


@pytest.fixture(scope="module")
def load_all_data():
    params = [pg_conn()]
    for p in params:
        db_conn, expected_schema = p
        with db_conn.cursor() as cursor:
            for statement in pii_data_load:
                cursor.execute(statement)
            cursor.execute("commit")
    yield params
    for p in params:
        db_conn, expected_schema = p
        with db_conn.cursor() as cursor:
            for statement in pii_data_drop:
                cursor.execute(statement)
            cursor.execute("commit")

    for p in params:
        db_conn, expected_schema = p
        db_conn.close()


@pytest.fixture(scope="module")
def setup_catalog_and_data(load_all_data, rest_catalog):
    catalog = rest_catalog
    source = catalog.add_source(
        name="pg_scan",
        source_type="postgresql",
        uri="127.0.0.1",
        username="piiuser",
        password="p11secret",
        database="piidb",
        cluster="public",
    )
    yield catalog, source


@pytest.fixture(scope="module")
def fake_queue():
    yield Queue(is_async=False, connection=FakeStrictRedis())


def test_scan_source(setup_catalog_and_data, scan_sdk):
    catalog, source = setup_catalog_and_data
    scan_sdk.start(source)

    pg_source = catalog.get_source("pg_scan")
    assert pg_source is not None

    no_pii = catalog.get_table("pg_scan", "public", "no_pii")
    assert no_pii is not None
