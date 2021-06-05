import logging
from typing import Any, Dict, Tuple

import flask_restless
import gunicorn.app.base
from dbcat import Catalog
from dbcat.catalog import CatColumn
from dbcat.catalog.models import CatSchema, CatSource, CatTable, Job, JobExecution
from flask import Flask
from flask_restful import Api, Resource


class Kedro(Resource):
    def __init__(self, catalog: Catalog):
        self._catalog = catalog

    def get(self):
        nodes = []
        edges = []

        column_edges = self._catalog.get_column_lineages()
        for edge in column_edges:
            nodes.append(self._column_info(edge.source))
            nodes.append(self._column_info(edge.target))
            nodes.append(self._job_info(edge.job_execution.job))
            edges.append(
                {
                    "source": "column:{}".format(edge.source_id),
                    "target": "task:{}".format(edge.job_execution.job_id),
                }
            )
            edges.append(
                {
                    "source": "task:{}".format(edge.job_execution.job_id),
                    "target": "column:{}".format(edge.target_id),
                }
            )

        return {"nodes": nodes, "edges": edges}

    @staticmethod
    def _column_info(node: CatColumn):
        return {
            "id": "column:{}".format(node.id),
            "name": ".".join(node.fqdn),
            "type": "data",
        }

    @staticmethod
    def _job_info(node: Job):
        return {"id": "task:{}".format(node.id), "name": node.name, "type": "task"}


class Server(gunicorn.app.base.BaseApplication):
    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        config = {
            key: value
            for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application


def create_server(
    catalog_options: Dict[str, str], options: Dict[str, str], is_production=True
) -> Tuple[Any, Catalog]:
    logging.debug(catalog_options)
    catalog = Catalog(**catalog_options)

    app = Flask(__name__)

    # Create CRUD APIs
    methods = ["DELETE", "GET", "PATCH", "POST"]
    api_manager = flask_restless.APIManager(app, catalog.scoped_session)
    api_manager.create_api(
        CatSource,
        methods=methods,
        url_prefix="/api/v1/catalog",
        additional_attributes=["fqdn"],
    )
    api_manager.create_api(
        CatSchema,
        methods=methods,
        url_prefix="/api/v1/catalog",
        additional_attributes=["fqdn"],
    )
    api_manager.create_api(
        CatTable,
        methods=methods,
        url_prefix="/api/v1/catalog",
        additional_attributes=["fqdn"],
    )
    api_manager.create_api(
        CatColumn,
        methods=methods,
        url_prefix="/api/v1/catalog",
        additional_attributes=["fqdn"],
    )
    api_manager.create_api(Job, methods=methods, url_prefix="/api/v1/catalog")
    api_manager.create_api(JobExecution, methods=methods, url_prefix="/api/v1/catalog")

    restful_manager = Api(app)
    restful_manager.add_resource(
        Kedro, "/api/main", resource_class_kwargs={"catalog": catalog}
    )

    for rule in app.url_map.iter_rules():
        rule_methods = ",".join(rule.methods)
        logging.debug("{:50s} {:20s} {}".format(rule.endpoint, rule_methods, rule))

    if is_production:
        return Server(app=app, options=options), catalog
    else:
        return app, catalog
