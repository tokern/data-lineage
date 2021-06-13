import datetime
import logging
from typing import Any, Dict, List, Tuple

import flask_restless
import gunicorn.app.base
from dbcat import Catalog
from dbcat.catalog import CatColumn
from dbcat.catalog.db import DbScanner
from dbcat.catalog.models import (
    CatSchema,
    CatSource,
    CatTable,
    ColumnLineage,
    Job,
    JobExecution,
    JobExecutionStatus,
)
from flask import Flask
from flask_restful import Api, Resource, reqparse

from data_lineage.parser import extract_lineage, parse, visit_dml_query


class Kedro(Resource):
    def __init__(self, catalog: Catalog):
        self._catalog = catalog
        self._parser = reqparse.RequestParser()
        self._parser.add_argument(
            "job_ids", action="append", help="List of job ids for a sub graph"
        )

    def get(self):
        nodes = []
        edges = []

        args = self._parser.parse_args()
        column_edges = self._catalog.get_column_lineages(args["job_ids"])
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


class Scanner(Resource):
    def __init__(self, catalog: Catalog):
        self._catalog = catalog
        self._parser = reqparse.RequestParser()
        self._parser.add_argument("id", required=True, help="ID of the resource")

    def post(self):
        args = self._parser.parse_args()
        logging.debug("Args for scanning: {}".format(args))
        source = self._catalog.get_source_by_id(int(args["id"]))
        DbScanner(self._catalog, source).scan()
        return "Scanned {}".format(source.fqdn), 200


class Parser(Resource):
    def __init__(self, catalog: Catalog):
        self._catalog = catalog
        self._parser = reqparse.RequestParser()
        self._parser.add_argument("query", required=True, help="Query to parse")
        self._parser.add_argument("name", help="Name of the ETL job")

    def post(self):
        args = self._parser.parse_args()
        logging.debug("Parse query: {}".format(args["query"]))
        parsed = parse(args["query"], args["name"])

        chosen_visitor = visit_dml_query(self._catalog, parsed)

        if chosen_visitor is not None:
            job_execution = extract_lineage(self._catalog, chosen_visitor, parsed)

            return (
                {
                    "data": {
                        "id": job_execution.id,
                        "type": "job_executions",
                        "attributes": {
                            "job_id": job_execution.job_id,
                            "started_at": job_execution.started_at.strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
                            "ended_at": job_execution.ended_at.strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
                            "status": job_execution.status.name,
                        },
                    }
                },
                200,
            )

        return {"data": {"error": "Query is not a DML Query"}}, 400


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


def job_execution_serializer(instance: JobExecution, only: List[str]):
    return {
        "id": instance.id,
        "type": "job_executions",
        "attributes": {
            "job_id": instance.job_id,
            "started_at": instance.started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "ended_at": instance.ended_at.strftime("%Y-%m-%d %H:%M:%S"),
            "status": instance.status.name,
        },
    }


def job_execution_deserializer(data: Dict["str", Any]):
    attributes = data["data"]["attributes"]
    logging.debug(attributes)
    job_execution = JobExecution()
    job_execution.job_id = int(attributes["job_id"])
    job_execution.started_at = datetime.datetime.strptime(
        attributes["started_at"], "%Y-%m-%d %H:%M:%S"
    )
    job_execution.ended_at = datetime.datetime.strptime(
        attributes["ended_at"], "%Y-%m-%d %H:%M:%S"
    )
    job_execution.status = (
        JobExecutionStatus.SUCCESS
        if attributes["status"] == "SUCCESS"
        else JobExecutionStatus.SUCCESS
    )

    logging.debug(job_execution)
    logging.debug(job_execution.status == JobExecutionStatus.SUCCESS)
    return job_execution


def create_server(
    catalog_options: Dict[str, str], options: Dict[str, str], is_production=True
) -> Tuple[Any, Catalog]:
    logging.debug(catalog_options)
    catalog = Catalog(**catalog_options)

    app = Flask(__name__)

    # Create CRUD APIs
    methods = ["DELETE", "GET", "PATCH", "POST"]
    url_prefix = "/api/v1/catalog"
    api_manager = flask_restless.APIManager(app, catalog.scoped_session)
    api_manager.create_api(
        CatSource,
        methods=methods,
        url_prefix=url_prefix,
        additional_attributes=["fqdn"],
    )
    api_manager.create_api(
        CatSchema,
        methods=methods,
        url_prefix=url_prefix,
        additional_attributes=["fqdn"],
    )
    api_manager.create_api(
        CatTable,
        methods=methods,
        url_prefix=url_prefix,
        additional_attributes=["fqdn"],
    )
    api_manager.create_api(
        CatColumn,
        methods=methods,
        url_prefix=url_prefix,
        additional_attributes=["fqdn"],
    )
    api_manager.create_api(Job, methods=methods, url_prefix=url_prefix)
    api_manager.create_api(
        JobExecution,
        methods=methods,
        url_prefix=url_prefix,
        serializer=job_execution_serializer,
        deserializer=job_execution_deserializer,
    )
    api_manager.create_api(
        ColumnLineage,
        methods=methods,
        url_prefix=url_prefix,
        collection_name="column_lineage",
    )

    restful_manager = Api(app)
    restful_manager.add_resource(
        Kedro, "/api/main", resource_class_kwargs={"catalog": catalog}
    )
    restful_manager.add_resource(
        Scanner,
        "{}/scanner".format(url_prefix),
        resource_class_kwargs={"catalog": catalog},
    )
    restful_manager.add_resource(
        Parser, "/api/v1/parser", resource_class_kwargs={"catalog": catalog}
    )

    for rule in app.url_map.iter_rules():
        rule_methods = ",".join(rule.methods)
        logging.debug("{:50s} {:20s} {}".format(rule.endpoint, rule_methods, rule))

    if is_production:
        return Server(app=app, options=options), catalog
    else:
        return app, catalog
