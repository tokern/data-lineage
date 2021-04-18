import logging

import click
from dbcat.log_mixin import LogMixin

from data_lineage import __version__
from data_lineage.server import run_server


@click.group()
@click.version_option(__version__)
@click.option("-l", "--log-level", help="Logging Level", default="INFO")
@click.option(
    "-c",
    "--config",
    help="Path to config file",
    required=True,
    type=click.Path(dir_okay=False, writable=True, resolve_path=True),
)
@click.pass_context
def main(ctx, log_level, config):
    logging.basicConfig(level=getattr(logging, log_level.upper()))
    ctx.obj = config


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


@main.command("init", short_help="Initialize config file")
@click.pass_obj
def init(obj):
    with open(obj, "w") as f:
        f.write(config_template)
    logger = LogMixin()
    logger.logger.info("Created a configuration file at {}".format(obj))


@main.command("runserver", short_help="Start the data lineage server")
@click.pass_obj
def runserver(obj):
    run_server(obj)


if __name__ == "__main__":
    main()
