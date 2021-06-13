import datetime

import pytest
from dbcat.catalog.models import ColumnLineage, Job, JobExecution, JobExecutionStatus


def test_get_sources(rest_catalog):
    sources = list(rest_catalog.get_sources())
    assert len(sources) == 1
    source = sources[0]
    assert source.name == "test"
    assert source.id is not None


def test_get_schemata(rest_catalog):
    schemata = list(rest_catalog.get_schemata())
    assert len(schemata) == 1
    assert schemata[0].name == "default"
    assert schemata[0].id is not None


def test_get_tables(rest_catalog):
    num = 0
    for table in rest_catalog.get_tables():
        assert table.id is not None
        assert table.name is not None
        num += 1
    assert num == 8


def test_get_columns(rest_catalog):
    num = 0
    for column in rest_catalog.get_columns():
        assert column.id is not None
        assert column.name is not None
        assert column.data_type is not None
        assert column.sort_order is not None
        num += 1

    assert num == 32


def test_get_source_by_id(rest_catalog):
    source = rest_catalog.get_source_by_id(1)
    assert source.name == "test"
    assert source.fqdn == "test"
    assert source.source_type == "json"


def test_get_schema_by_id(rest_catalog):
    schema = rest_catalog.get_schema_by_id(1)
    assert schema.name == "default"
    assert schema.fqdn == ["test", "default"]


def test_get_table_by_id(rest_catalog):
    table = rest_catalog.get_table_by_id(1)
    assert table.name == "pagecounts"
    assert table.fqdn == ["test", "default", "pagecounts"]


def test_get_column_by_id(rest_catalog):
    column = rest_catalog.get_column_by_id(1)
    assert column.name == "group"
    assert column.fqdn == ["test", "default", "pagecounts", "group"]


def test_get_source_by_name(rest_catalog):
    sources = list(rest_catalog.get_source_by_name("test"))
    assert len(sources) == 1
    source = sources[0]
    assert source.name == "test"
    assert source.id is not None


def test_get_schema_by_name(rest_catalog):
    schemata = list(rest_catalog.get_schema_by_name("default"))
    assert len(schemata) == 1
    assert schemata[0].name == "default"
    assert schemata[0].id is not None


def test_get_table_by_name(rest_catalog):
    num = 0
    for table in rest_catalog.get_table_by_name("normalized_pagecounts"):
        assert table.id is not None
        assert table.name == "normalized_pagecounts"
        num += 1
    assert num == 1


def test_get_column_by_name(rest_catalog):
    num = 0
    for column in rest_catalog.get_column_by_name("bytes_sent"):
        assert column.id is not None
        assert column.name is not None
        # assert column.type is not None
        assert column.sort_order is not None
        num += 1

    assert num == 2


def test_add_source_pg(rest_catalog):
    data = {
        "name": "pg",
        "source_type": "postgres",
        "database": "db_database",
        "username": "db_user",
        "password": "db_password",
        "port": "db_port",
        "uri": "db_uri",
    }

    pg_connection = rest_catalog.add_source(**data)
    assert pg_connection.name == "pg"
    assert pg_connection.source_type == "postgres"
    assert pg_connection.database == "db_database"
    assert pg_connection.username == "db_user"
    assert pg_connection.password == "db_password"
    assert pg_connection.port == "db_port"
    assert pg_connection.uri == "db_uri"


def test_add_source_mysql(rest_catalog):
    data = {
        "name": "mys",
        "source_type": "mysql",
        "database": "db_database",
        "username": "db_user",
        "password": "db_password",
        "port": "db_port",
        "uri": "db_uri",
    }

    mysql_conn = rest_catalog.add_source(**data)

    assert mysql_conn.name == "mys"
    assert mysql_conn.source_type == "mysql"
    assert mysql_conn.database == "db_database"
    assert mysql_conn.username == "db_user"
    assert mysql_conn.password == "db_password"
    assert mysql_conn.port == "db_port"
    assert mysql_conn.uri == "db_uri"


def test_add_source_bq(rest_catalog):
    bq_conn = rest_catalog.add_source(
        name="bq",
        source_type="bigquery",
        key_path="db_key_path",
        project_credentials="db_creds",
        project_id="db_project_id",
    )
    assert bq_conn.name == "bq"
    assert bq_conn.source_type == "bigquery"
    assert bq_conn.key_path == "db_key_path"
    assert bq_conn.project_credentials == "db_creds"
    assert bq_conn.project_id == "db_project_id"


def test_add_source_glue(rest_catalog):
    glue_conn = rest_catalog.add_source(name="gl", source_type="glue")
    assert glue_conn.name == "gl"
    assert glue_conn.source_type == "glue"


def test_add_source_snowflake(rest_catalog):
    sf_conn = rest_catalog.add_source(
        name="sf",
        source_type="snowflake",
        database="db_database",
        username="db_user",
        password="db_password",
        account="db_account",
        role="db_role",
        warehouse="db_warehouse",
    )
    assert sf_conn.name == "sf"
    assert sf_conn.source_type == "snowflake"
    assert sf_conn.database == "db_database"
    assert sf_conn.username == "db_user"
    assert sf_conn.password == "db_password"
    assert sf_conn.account == "db_account"
    assert sf_conn.role == "db_role"
    assert sf_conn.warehouse == "db_warehouse"


def load_edges(catalog, expected_edges, job_execution_id):
    column_edge_ids = []
    for edge in expected_edges:
        source = catalog.get_column(
            database_name=edge[0][0],
            schema_name=edge[0][1],
            table_name=edge[0][2],
            column_name=edge[0][3],
        )

        target = catalog.get_column(
            database_name=edge[1][0],
            schema_name=edge[1][1],
            table_name=edge[1][2],
            column_name=edge[1][3],
        )

        added_edge = catalog.add_column_lineage(source, target, job_execution_id, {})

        column_edge_ids.append(added_edge.id)
    return column_edge_ids


@pytest.fixture(scope="module")
def load_page_lookup_nonredirect_edges(save_catalog):
    catalog = save_catalog

    expected_edges = [
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_nonredirect", "redirect_id"),
        ),
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_nonredirect", "page_id"),
        ),
        (
            ("test", "default", "page", "page_title"),
            ("test", "default", "page_lookup_nonredirect", "redirect_title"),
        ),
        (
            ("test", "default", "page", "page_title"),
            ("test", "default", "page_lookup_nonredirect", "true_title"),
        ),
        (
            ("test", "default", "page", "page_latest"),
            ("test", "default", "page_lookup_nonredirect", "page_version"),
        ),
    ]

    job = catalog.add_job(
        "insert_page_lookup_nonredirect",
        {"sql": "insert into page_lookup_nonredirect select from page"},
    )
    e1 = catalog.add_job_execution(
        job=job,
        started_at=datetime.datetime.combine(
            datetime.date(2021, 4, 1), datetime.time(1, 0)
        ),
        ended_at=datetime.datetime.combine(
            datetime.date(2021, 4, 1), datetime.time(1, 15)
        ),
        status=JobExecutionStatus.SUCCESS,
    )

    executions = [e1.id]
    name = job.name

    print("Inserted job {}".format(name))
    print("Inserted executions {}".format(",".join(str(v) for v in executions)))

    column_edge_ids = load_edges(catalog, expected_edges, executions[0])
    print("Inserted edges {}".format(",".join(str(v) for v in column_edge_ids)))

    yield catalog, job, expected_edges

    session = catalog.scoped_session
    session.query(ColumnLineage).filter(ColumnLineage.id.in_(column_edge_ids)).delete(
        synchronize_session=False
    )
    print("DELETED edges {}".format(",".join(str(v) for v in column_edge_ids)))
    session.commit()

    session.query(JobExecution).filter(JobExecution.id.in_(executions)).delete(
        synchronize_session=False
    )
    print("DELETED executions {}".format(",".join(str(v) for v in executions)))
    session.commit()

    session.query(Job).filter(Job.name == name).delete(synchronize_session=False)
    print("DELETED job {}".format(name))
    session.commit()


def test_api_main(graph_sdk, load_page_lookup_nonredirect_edges):
    catalog, job, expected_edges = load_page_lookup_nonredirect_edges
    graph = graph_sdk.get([job.id])
    assert len(graph["edges"]) == 10
    assert len(graph["nodes"]) == 15


def test_parser(rest_catalog, parser_sdk, graph_sdk, save_catalog):
    data = {
        "name": "LOAD page_lookup",
        "query": "INSERT INTO page_lookup SELECT plr.redirect_id, plr.redirect_title, plr.true_title, plr.page_id, "
        "plr.page_version FROM page_lookup_redirect plr",
    }

    job_execution = parser_sdk.parse(**data)
    assert job_execution is not None

    graph = graph_sdk.get([job_execution.job_id])

    assert len(graph["edges"]) == 10
    assert len(graph["nodes"]) == 15

    column_lineages = rest_catalog.get_column_lineage([job_execution.job_id])
    assert (len(column_lineages)) == 10
