import json
import logging
from collections import namedtuple
from typing import Any, Dict

import requests
from dbcat.catalog import Catalog
from furl import furl

from data_lineage.graph import DbGraph


def load_graph(catalog: Catalog) -> DbGraph:
    graph = DbGraph(catalog)
    graph.load()
    return graph


class RestCatalog:
    def __init__(self, url: str):
        self._base_url = furl(url) / "api/v1/catalog"
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/vnd.api+json"})
        self._session.headers.update({"Content-Type": "application/vnd.api+json"})

    def _build_url(self, *urls) -> str:
        built_url = self._base_url
        for url in urls:
            built_url = furl(built_url) / url
        logging.debug(built_url)
        return built_url

    def _iterate(self, payload, clazz):
        while payload is not None:
            for item in payload["data"]:
                keys = list(item["attributes"].keys())
                keys.append("id")
                values = list(item["attributes"].values())
                values.append(item["id"])
                yield namedtuple(clazz, keys)(*values)

            if payload["links"]["next"] is not None:
                response = self._session.get(payload["links"]["next"])
                payload = response.json()
            else:
                payload = None

    def _index(self, path: str, clazz: str):
        response = self._session.get(self._build_url(path))
        return self._iterate(response.json(), clazz)

    def _get(self, path: str, obj_id: int, clazz: str):
        response = self._session.get(self._build_url(path, str(obj_id)))
        payload = response.json()["data"]
        keys = list(payload["attributes"].keys())
        keys.append("id")
        values = list(payload["attributes"].values())
        values.append(payload["id"])
        return namedtuple(clazz, keys)(*values)

    def _search(self, path: str, search_string: str, clazz: str):
        filters = [dict(name="name", op="like", val="%{}%".format(search_string))]
        params = {"filter[objects]": json.dumps(filters)}
        response = self._session.get(self._build_url(path), params=params)
        return self._iterate(response.json(), clazz)

    def _post(self, path: str, data: Dict[str, str], type: str, clazz: str):
        payload = {"data": {"type": type, "attributes": data}}
        response = self._session.post(
            url=self._build_url(path), data=json.dumps(payload)
        )
        payload = response.json()["data"]
        keys = list(payload["attributes"].keys())
        keys.append("id")
        values = list(payload["attributes"].values())
        values.append(payload["id"])
        return namedtuple(clazz, keys)(*values)

    def get_sources(self):
        return self._index("sources", "Source")

    def get_schemata(self):
        return self._index("schemata", "Schema")

    def get_tables(self):
        return self._index("tables", "Table")

    def get_columns(self):
        return self._index("columns", "Column")

    def get_jobs(self):
        return self._index("jobs", "Job")

    def get_job_executions(self):
        return self._index("job_executions", "JobExecution")

    def get_column_lineages(self):
        return self._index("column_lineages", "ColumnLineage")

    def get_source_by_id(self, obj_id):
        return self._get("sources", obj_id, "Source")

    def get_schema_by_id(self, obj_id):
        return self._get("schemata", obj_id, "Schema")

    def get_table_by_id(self, obj_id):
        return self._get("tables", obj_id, "Table")

    def get_column_by_id(self, obj_id):
        return self._get("columns", obj_id, "Column")

    def get_job_by_id(self, obj_id):
        return self._get("jobs", obj_id, "Job")

    def get_job_execution_by_id(self, obj_id):
        return self._get("job_executions", obj_id, "JobExecution")

    def get_column_lineage_by_id(self, obj_id):
        return self._get("column_lineages", obj_id, "ColumnLineage")

    def get_source_by_name(self, name):
        return self._search("sources", name, "Source")

    def get_schema_by_name(self, name):
        return self._search("schemata", name, "Schema")

    def get_table_by_name(self, name):
        return self._search("tables", name, "Table")

    def get_column_by_name(self, name):
        return self._search("columns", name, "Column")

    def add_source(self, name: str, source_type: str, **kwargs) -> Dict[str, Any]:
        data = {"name": name, "source_type": source_type, **kwargs}
        return self._post(path="sources", data=data, type="sources", clazz="Source")
