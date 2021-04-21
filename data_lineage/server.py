from typing import Dict, Optional

import gunicorn.app.base
from dbcat import Catalog
from dbcat import __version__ as dbcat_version
from dbcat.catalog import CatColumn
from dbcat.catalog.models import Job
from dbcat.log_mixin import LogMixin
from flask import Flask, abort, jsonify

from . import __version__ as data_lineage_version

app = Flask(__name__)
_CATALOG: Optional[Catalog] = None


def _column_info(node: CatColumn):
    return {
        "id": "column:{}".format(node.id),
        "name": ".".join(node.fqdn),
        "type": "data",
    }


def _job_info(node: Job):
    return {"id": "task:{}".format(node.id), "name": node.name, "type": "task"}


def _generate_graph_response():
    nodes = []
    edges = []

    assert _CATALOG is not None

    column_edges = _CATALOG.get_column_lineages()
    for edge in column_edges:
        nodes.append(_column_info(edge.source))
        nodes.append(_column_info(edge.target))
        nodes.append(_job_info(edge.job_execution.job))
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


@app.route("/")
def root():
    return jsonify({"data-lineage": data_lineage_version, "dbcat": dbcat_version})


@app.route("/api/main")
def nodes_json():
    """Serve the data for all tables.
    This includes basic node data amongst others edges, tags, and layers.
    """
    return jsonify(_generate_graph_response())


@app.route("/api/nodes/<string:node_id>")
def nodes_metadata(node_id: str):
    """Serve the metadata for node and dataset."""
    type, type_id = node_id.split(":")

    assert _CATALOG is not None

    if type == "column":
        column = _CATALOG.get_column_by_id(type_id)
        return jsonify(
            {
                "fqdn": ".".join(column.fqdn),
                "type": column.type,
                "sort_order": column.sort_order,
            }
        )
    elif type == "task":
        job = _CATALOG.get_job_by_id(type_id)
        return jsonify({"name": job.name})
    abort(404, description="Invalid Node ID.")


@app.errorhandler(404)
def resource_not_found(error):
    """Returns HTTP 404 on resource not found."""
    return jsonify(error=str(error)), 404


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


def run_server(catalog_options: Dict[str, str], options: Dict[str, str]):
    global _CATALOG

    logger = LogMixin()
    logger.logger.debug(catalog_options)
    _CATALOG = Catalog(type="postgresql", **catalog_options)
    Server(app=app, options=options).run()
