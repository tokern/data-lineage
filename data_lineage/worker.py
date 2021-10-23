import logging
from contextlib import closing

from dbcat import DbScanner, PGCatalog


def scan(connection_args, source_id):
    logging.info("{}".format(connection_args))
    catalog = PGCatalog(
        **connection_args,
        connect_args={"application_name": "data-lineage:worker"},
        max_overflow=40,
        pool_size=20,
        pool_pre_ping=True
    )

    with closing(catalog):
        with catalog.managed_session:
            source = catalog.get_source_by_id(source_id)
            DbScanner(catalog, source).scan()
