import click
import yaml

from data_lineage import __version__
from data_lineage.catalog.sources import FileSource
from data_lineage.data_lineage import get_graph
from data_lineage.server import Server


@click.group()
@click.version_option(__version__)
@click.option("-l", "--log-level", help="Logging Level", default="WARNING")
@click.option(
    "-c",
    "--config",
    help="Path to config file",
    required=True,
    type=click.Path(dir_okay=False, writable=True, resolve_path=True),
)
@click.pass_context
def main(ctx, log_level, config):
    ctx.obj = config


config_template = """
# Instructions for configuring Tokern Data Lineage Server.
# This configuration file is in YAML format.

# The server can read queries from a json file containing an array of SQL statements.
# The other option is to read from snowflake.
# There is a section for each option. Uncomment the section and fill in the values.
# ####
# File
# ####
# file: <path to file>

# #########
# Snowflake
# #########
# snowflake:
#   user: USER
#   password: PASSWORD
#   account: ACCOUNT
#   warehouse: WAREHOUSE
#   database: DATABASE
#   schema: SCHEMA
"""


@main.command("init", short_help="Initialize config file")
@click.pass_obj
def init(obj):
    with open(obj, "w") as f:
        f.write(config_template)


@main.command("runserver", short_help="Start the data lineage server")
@click.option("--port", help="Port to listen to", default=8050, type=int)
@click.pass_obj
def runserver(obj, port):
    with open(obj, "r") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    source = None
    print(config)
    if "file" in config:
        source = FileSource(config["file"])
    #    elif config.snowflake is not None:
    #        source = Snowflake(config.file)
    server = Server(port, get_graph(source))
    server.run_server()


if __name__ == "__main__":
    main()
