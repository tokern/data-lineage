import logging

import click

from data_lineage import __version__
from data_lineage.server import create_server


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
@click.option(
    "--is-production/--not-production",
    help="Run server in development mode",
    default=True,
)
def main(
    log_level,
    catalog_user,
    catalog_password,
    catalog_host,
    catalog_port,
    catalog_db,
    server_address,
    is_production,
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
    app, catalog = create_server(catalog, options, is_production=is_production)
    if is_production:
        app.run()
    else:
        app.run(debug=True)


if __name__ == "__main__":
    main()
