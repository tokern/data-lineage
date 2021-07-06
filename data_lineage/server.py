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
    DefaultSchema,
    Job,
    JobExecution,
    JobExecutionStatus,
)
from flask import Flask
from flask_restful import Api, Resource, reqparse
from pglast.parser import ParseError
from werkzeug.exceptions import NotFound, UnprocessableEntity

from data_lineage import ColumnNotFound, SemanticError, TableNotFound
from data_lineage.parser import (
    analyze_dml_query,
    extract_lineage,
    parse,
    parse_dml_query,
)


class TableNotFoundHTTP(NotFound):
    """Table not found in catalog"""

    code = 441


class ColumnNotFoundHTTP(NotFound):
    """Column not found in catalog"""

    code = 442


class ParseErrorHTTP(UnprocessableEntity):
    """Parser Error"""


class SemanticErrorHTTP(UnprocessableEntity):
    """Semantic Error"""

    code = 443


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
        try:
            args = self._parser.parse_args()
            logging.debug("Args for scanning: {}".format(args))
            source = self._catalog.get_source_by_id(int(args["id"]))
            DbScanner(self._catalog, source).scan()
            return "Scanned {}".format(source.fqdn), 200
        finally:
            self._catalog.scoped_session.remove()


class Parse(Resource):
    def __init__(self, catalog: Catalog):
        self._catalog = catalog
        self._parser = reqparse.RequestParser()
        self._parser.add_argument("query", required=True, help="Query to parse")
        self._parser.add_argument(
            "source_id", help="Source database of the query", required=True
        )

    def post(self):
        args = self._parser.parse_args()
        logging.debug("Parse query: {}".format(args["query"]))
        try:
            parsed = parse(args["query"], "parse_api")
        except ParseError as error:
            raise ParseErrorHTTP(description=str(error))

        try:
            source = self._catalog.get_source_by_id(args["source_id"])
            logging.debug("Parsing query for source {}".format(source))
            binder = parse_dml_query(
                catalog=self._catalog, parsed=parsed, source=source
            )

            return (
                {
                    "select_tables": [table.name for table in binder.tables],
                    "select_columns": [context.alias for context in binder.columns],
                },
                200,
            )
        except TableNotFound as table_error:
            raise TableNotFoundHTTP(description=str(table_error))
        except ColumnNotFound as column_error:
            raise ColumnNotFoundHTTP(description=str(column_error))
        except SemanticError as semantic_error:
            raise SemanticErrorHTTP(description=str(semantic_error))
        finally:
            self._catalog.scoped_session.remove()


class Analyze(Resource):
    def __init__(self, catalog: Catalog):
        self._catalog = catalog
        self._parser = reqparse.RequestParser()
        self._parser.add_argument("query", required=True, help="Query to parse")
        self._parser.add_argument("name", help="Name of the ETL job")
        self._parser.add_argument(
            "start_time", required=True, help="Start time of the task"
        )
        self._parser.add_argument(
            "end_time", required=True, help="End time of the task"
        )
        self._parser.add_argument(
            "source_id", help="Source database of the query", required=True
        )

    def post(self):
        args = self._parser.parse_args()
        logging.debug("Parse query: {}".format(args["query"]))
        try:
            parsed = parse(args["query"], args["name"])
        except ParseError as error:
            raise ParseErrorHTTP(description=str(error))

        try:
            source = self._catalog.get_source_by_id(args["source_id"])
            logging.debug("Parsing query for source {}".format(source))
            chosen_visitor = analyze_dml_query(self._catalog, parsed, source)
            job_execution = extract_lineage(
                catalog=self._catalog,
                visited_query=chosen_visitor,
                source=source,
                parsed=parsed,
                start_time=datetime.datetime.fromisoformat(args["start_time"]),
                end_time=datetime.datetime.fromisoformat(args["end_time"]),
            )

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
        except TableNotFound as table_error:
            raise TableNotFoundHTTP(description=str(table_error))
        except ColumnNotFound as column_error:
            raise ColumnNotFoundHTTP(description=str(column_error))
        except SemanticError as semantic_error:
            raise SemanticErrorHTTP(description=str(semantic_error))
        finally:
            self._catalog.scoped_session.remove()


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
    catalog = Catalog(
        **catalog_options,
        connect_args={"application_name": "data-lineage:flask-restless"},
        max_overflow=40,
        pool_size=20
    )

    restful_catalog = Catalog(
        **catalog_options, connect_args={"application_name": "data-lineage:restful"}
    )

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

    api_manager.create_api(
        DefaultSchema,
        methods=methods,
        url_prefix=url_prefix,
        collection_name="default_schema",
        primary_key="source_id",
    )

    restful_manager = Api(app)
    restful_manager.add_resource(
        Kedro, "/api/main", resource_class_kwargs={"catalog": restful_catalog}
    )
    restful_manager.add_resource(
        Scanner,
        "{}/scanner".format(url_prefix),
        resource_class_kwargs={"catalog": restful_catalog},
    )
    restful_manager.add_resource(
        Analyze, "/api/v1/analyze", resource_class_kwargs={"catalog": restful_catalog}
    )

    restful_manager.add_resource(
        Parse, "/api/v1/parse", resource_class_kwargs={"catalog": restful_catalog}
    )

    for rule in app.url_map.iter_rules():
        rule_methods = ",".join(rule.methods)
        logging.debug("{:50s} {:20s} {}".format(rule.endpoint, rule_methods, rule))

    if is_production:
        return Server(app=app, options=options), catalog
    else:
        return app, catalog
