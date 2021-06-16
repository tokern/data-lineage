# flake8: noqa
__version__ = "0.7.0"

import datetime
import json
import logging
from typing import Any, Dict, Generator, List, Optional, Type

import requests
from dbcat.catalog.models import JobExecutionStatus
from furl import furl

from data_lineage.graph import LineageGraph


class Graph:
    def __init__(self, url: str):
        self._base_url = furl(url) / "api/main"
        self._session = requests.Session()

    def get(self, job_ids: set = None) -> Dict[str, List[Dict[str, str]]]:
        if job_ids is not None:
            response = self._session.get(
                self._base_url, params={"job_ids": list(job_ids)}
            )
        else:
            response = self._session.get(self._base_url)
        return response.json()


def load_graph(graphSDK: Graph, job_ids: set = None) -> LineageGraph:
    data = graphSDK.get(job_ids)
    return LineageGraph(nodes=data["nodes"], edges=data["edges"])


class BaseModel:
    def __init__(self, session, attributes, obj_id, relationships):
        self._session = session
        self._attributes = attributes
        self._obj_id = obj_id
        self._relationships = relationships

    def __getattr__(self, item):
        if item == "id":
            return self._obj_id
        elif item in self._attributes.keys():
            return self._attributes[item]
        raise AttributeError


class Source(BaseModel):
    def __init__(self, session, attributes, obj_id, relationships):
        super().__init__(session, attributes, obj_id, relationships)


class Schema(BaseModel):
    def __init__(self, session, attributes, obj_id, relationships):
        super().__init__(session, attributes, obj_id, relationships)


class Table(BaseModel):
    def __init__(self, session, attributes, obj_id, relationships):
        super().__init__(session, attributes, obj_id, relationships)


class Column(BaseModel):
    def __init__(self, session, attributes, obj_id, relationships):
        super().__init__(session, attributes, obj_id, relationships)


class Job(BaseModel):
    def __init__(self, session, attributes, obj_id, relationships):
        super().__init__(session, attributes, obj_id, relationships)


class JobExecution(BaseModel):
    def __init__(self, session, attributes, obj_id, relationships):
        super().__init__(session, attributes, obj_id, relationships)


class ColumnLineage(BaseModel):
    def __init__(self, session, attributes, obj_id, relationships):
        super().__init__(session, attributes, obj_id, relationships)


class Catalog:
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

    def _obj_factory(
        self, payload: Dict[str, Any], clazz: Type[BaseModel]
    ) -> BaseModel:
        return clazz(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=payload["relationships"],
        )

    def _iterate(self, payload: Dict[str, Any], clazz: Type[BaseModel]):
        res: Optional[Dict[str, Any]] = payload
        while res is not None:
            for item in res["data"]:
                yield self._obj_factory(payload=item, clazz=clazz)

            if res["links"]["next"] is not None:
                response = self._session.get(res["links"]["next"])
                res = response.json()
            else:
                res = None

    def _index(self, path: str, clazz: Type[BaseModel]):
        response = self._session.get(self._build_url(path))
        logging.debug(response.json())
        return self._iterate(response.json(), clazz)

    def _get(self, path: str, obj_id: int) -> Dict[Any, Any]:
        response = self._session.get(self._build_url(path, str(obj_id)))
        logging.debug(response.json())
        return response.json()["data"]

    def _search(self, path: str, search_string: str, clazz: Type[BaseModel]):
        filters = [dict(name="name", op="like", val="%{}%".format(search_string))]
        params = {"filter[objects]": json.dumps(filters)}
        response = self._session.get(self._build_url(path), params=params)
        return self._iterate(response.json(), clazz)

    def _post(self, path: str, data: Dict[str, Any], type: str) -> Dict[Any, Any]:
        payload = {"data": {"type": type, "attributes": data}}
        response = self._session.post(
            url=self._build_url(path), data=json.dumps(payload, default=str)
        )
        logging.debug(response.json())
        return response.json()["data"]

    def get_sources(self) -> Generator[Any, Any, None]:
        return self._index("sources", Source)

    def get_schemata(self):
        return self._index("schemata", Schema)

    def get_tables(self):
        return self._index("tables", Table)

    def get_columns(self):
        return self._index("columns", Column)

    def get_jobs(self):
        return self._index("jobs", Job)

    def get_job_executions(self):
        return self._index("job_executions", JobExecution)

    def get_column_lineages(self):
        return self._index("column_lineages", ColumnLineage)

    def get_source_by_id(self, obj_id) -> Source:
        payload = self._get("sources", obj_id)
        return Source(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=payload["relationships"],
        )

    def get_schema_by_id(self, obj_id) -> Schema:
        payload = self._get("schemata", obj_id)
        return Schema(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=payload["relationships"],
        )

    def get_table_by_id(self, obj_id) -> Table:
        payload = self._get("tables", obj_id)
        return Table(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=payload["relationships"],
        )

    def get_column_by_id(self, obj_id) -> Column:
        payload = self._get("columns", obj_id)
        return Column(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=payload["relationships"],
        )

    def get_job_by_id(self, obj_id) -> Job:
        payload = self._get("jobs", obj_id)
        return Job(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=None,
        )

    def get_job_execution_by_id(self, obj_id) -> JobExecution:
        payload = self._get("job_executions", obj_id)
        return JobExecution(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=None,
        )

    def get_column_lineage(self, job_ids: List[int]) -> List[ColumnLineage]:
        params = {"job_ids": job_ids}
        response = self._session.get(self._build_url("column_lineage"), params=params)
        logging.debug(response.json())
        return [
            ColumnLineage(
                session=self._session,
                attributes=item["attributes"],
                obj_id=item["id"],
                relationships=item["relationships"],
            )
            for item in response.json()["data"]
        ]

    def get_source_by_name(self, name):
        return self._search("sources", name, Source)

    def get_schema_by_name(self, name):
        return self._search("schemata", name, Schema)

    def get_table_by_name(self, name):
        return self._search("tables", name, Table)

    def get_column_by_name(self, name):
        return self._search("columns", name, Column)

    def get_column(self, source_name, schema_name, table_name, column_name):
        # Get Source
        name_filter = dict(name="name", op="eq", val="{}".format(source_name))
        params = {"filter[singleton]": 1, "filter[objects]": json.dumps([name_filter])}
        response = self._session.get(self._build_url("sources"), params=params)
        logging.debug(json.dumps(response.json(), indent=2))
        payload = response.json()["data"][0]
        source = Source(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=payload["relationships"],
        )

        schema_name_filter = dict(name="name", op="eq", val="{}".format(schema_name))
        source_filter = dict(name="source_id", op="eq", val="{}".format(source.id))
        schema_params = {
            "filter[singleton]": 1,
            "filter[objects]": json.dumps(
                [{"and": [schema_name_filter, source_filter]}]
            ),
        }
        response = self._session.get(self._build_url("schemata"), params=schema_params)
        logging.debug(json.dumps(response.json(), indent=2))
        payload = response.json()["data"][0]
        schema = Schema(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=payload["relationships"],
        )

        table_name_filter = dict(name="name", op="eq", val="{}".format(table_name))
        schema_filter = dict(name="schema_id", op="eq", val="{}".format(schema.id))
        table_params = {
            "filter[singleton]": 1,
            "filter[objects]": json.dumps(
                [{"and": [table_name_filter, schema_filter]}]
            ),
        }
        response = self._session.get(self._build_url("tables"), params=table_params)
        logging.debug(json.dumps(response.json(), indent=2))
        payload = response.json()["data"][0]
        table = Table(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=payload["relationships"],
        )

        column_name_filter = dict(name="name", op="eq", val="{}".format(column_name))
        table_filter = dict(name="table_id", op="eq", val="{}".format(table.id))
        column_params = {
            "filter[singleton]": 1,
            "filter[objects]": json.dumps(
                [{"and": [column_name_filter, table_filter]}]
            ),
        }
        response = self._session.get(self._build_url("columns"), params=column_params)
        logging.debug(json.dumps(response.json(), indent=2))
        payload = response.json()["data"][0]
        column = Column(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=payload["relationships"],
        )

        return column

    def add_source(self, name: str, source_type: str, **kwargs) -> Source:
        data = {"name": name, "source_type": source_type, **kwargs}
        payload = self._post(path="sources", data=data, type="sources")
        return Source(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=payload["relationships"],
        )

    def scan_source(self, source: Source) -> bool:
        payload = {"id": source.id}
        response = self._session.post(
            url=self._build_url("scanner"), data=json.dumps(payload)
        )
        return response.status_code == 200

    def add_job(self, name: str, context: Dict[Any, Any]) -> Job:
        data = {"name": name, "context": context}
        payload = self._post(path="jobs", data=data, type="jobs")
        print(payload)
        return Job(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=None,
        )

    def add_job_execution(
        self,
        job: Job,
        started_at: datetime.datetime,
        ended_at: datetime.datetime,
        status: JobExecutionStatus,
    ) -> JobExecution:
        data = {
            "job_id": job.id,
            "started_at": started_at,
            "ended_at": ended_at,
            "status": status.name,
        }
        payload = self._post(path="job_executions", data=data, type="job_executions")
        return JobExecution(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=None,
        )

    def add_column_lineage(
        self,
        source: Column,
        target: Column,
        job_execution_id: int,
        context: Dict[Any, Any],
    ) -> ColumnLineage:
        data = {
            "source_id": source.id,
            "target_id": target.id,
            "job_execution_id": job_execution_id,
            "context": context,
        }
        payload = self._post(path="column_lineage", data=data, type="column_lineage")
        return ColumnLineage(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=None,
        )


class Parser:
    def __init__(self, url: str):
        self._base_url = furl(url) / "api/v1/parser"
        self._session = requests.Session()

    def parse(self, query: str, name: str) -> JobExecution:
        response = self._session.post(
            self._base_url, params={"query": query, "name": name}
        )
        logging.debug(response.json())
        payload = response.json()["data"]
        return JobExecution(
            session=self._session,
            attributes=payload["attributes"],
            obj_id=payload["id"],
            relationships=None,
        )
