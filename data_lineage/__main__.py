import logging

import click

from data_lineage import __version__
from data_lineage.server import run_server


@click.command()
@click.version_option(__version__)
@click.option("-l", "--log-level", help="Logging Level", default="INFO")
@click.option(
    "--catalog-user", help="Database user name", envvar="CATALOG_USER", required=True
)
@click.option(
    "--catalog-password",
    help="Database Password",
    envvar="CATALOG_PASSWORD",
    required=True,
)
@click.option(
    "--catalog-host", help="Database Host", envvar="CATALOG_HOST", default="localhost"
)
@click.option(
    "--catalog-port", help="Database Password", envvar="CATALOG_PORT", default=5432
)
@click.option(
    "--catalog-db", help="Postgres Database", envvar="CATALOG_PORT", default="tokern"
)
@click.option(
    "--server-address",
    help="The socket to bind to",
    envvar="SERVER_ADDRESS",
    default="127.0.0.1:4142",
)
@click.pass_context
def main(
    ctx,
    log_level,
    catalog_user,
    catalog_password,
    catalog_host,
    catalog_port,
    catalog_db,
    server_address,
):
    logging.basicConfig(level=getattr(logging, log_level.upper()))
    catalog = {
        "user": catalog_user,
        "password": catalog_password,
        "host": catalog_host,
        "port": catalog_port,
        "database": catalog_db,
    }
    options = {"bind": server_address}
    run_server(catalog, options)


config_template = """
# Instructions for configuring Tokern Data Lineage Server.
# This configuration file is in YAML format.

# This configuration file is in YAML format.

# Copy paste the section of the relevant database and fill in the values.

# The configuration file consists of
# - a catalog sink where metadata is stored.
# - a list of database connections to scan.


# The following catalog types are supported:
# - Files
# - Postgres
# - MySQL
# Choose one of them

catalog:
  type: postgres
  user: db_user
  password: db_password
  host: db_host
  port: db_port

connections:
  - name: pg
    type: postgres
    database: db_database
    username: db_user
    password: db_password
    port: db_port
    uri: db_uri
  - name: mys
    type: mysql
    database: db_database
    username: db_user
    password: db_password
    port: db_port
    uri: db_uri
  - name: bq
    type: bigquery
    key_path: db_key_path
    project_credentials:  db_creds
    project_id: db_project_id
  - name: gl
    type: glue
  - name: sf
    type: snowflake
    database: db_database
    username: db_user
    password: db_password
    account: db_account
    role: db_role
    warehouse: db_warehouse
"""


if __name__ == "__main__":
    main()
