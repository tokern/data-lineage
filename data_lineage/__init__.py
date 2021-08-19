# flake8: noqa
__version__ = "0.8.3"

import datetime
import json
import logging
from typing import Any, Dict, Generator, Generic, List, Optional, Type, TypeVar

import requests
from dbcat.catalog.models import JobExecutionStatus
from furl import furl
from requests import HTTPError

from data_lineage.graph import LineageGraph


class SourceNotFound(Exception):
    """Source not found in catalog"""


class SchemaNotFound(Exception):
    """Schema not found in catalog"""


class TableNotFound(Exception):
    """Table not found in catalog"""


class ColumnNotFound(Exception):
    """Column not found in catalog"""


class ParseError(Exception):
    """Parser Error"""


class SemanticError(Exception):
    """Error due to mismatch in catalog data"""


class NoResultFound(Exception):
    """Raised when function returns no results"""


class MultipleResultsFound(Exception):
    """Raised when multiple results are found but expected only one or zero results"""


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
        logging.debug("Attributes: {}".format(self._attributes))
        if item == "id":
            return self._obj_id
        elif self._attributes and item in self._attributes.keys():
            return self._attributes[item]
        elif self._relationships and item in self._relationships.keys():
            return self._relationships[item]
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


class DefaultSchema(BaseModel):
    def __init__(self, session, attributes, obj_id, relationships):
        super().__init__(session, attributes, obj_id, relationships)


ModelType = TypeVar("ModelType", bound=BaseModel)


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

    str_to_type = {
        "sources": Source,
        "schemata": Schema,
    }

    def _resolve_relationships(self, relationships) -> Dict[str, BaseModel]:
        resolved: Dict[str, BaseModel] = {}
        for key, value in relationships.items():
            logging.debug("Resolving {}:{}".format(key, value))
            if value["data"]:
                resolved[key] = self._obj_factory(
                    value["data"],
                    Catalog.str_to_type[value["data"]["type"]],
                    resolve_relationships=False,
                )

        return resolved

    def _obj_factory(
        self,
        payload: Dict[str, Any],
        clazz: Type[ModelType],
        resolve_relationships=False,
    ) -> ModelType:
        resolved = None
        if resolve_relationships and payload.get("relationships"):
            resolved = self._resolve_relationships(payload.get("relationships"))

        return clazz(
            session=self._session,
            attributes=payload.get("attributes"),
            obj_id=payload.get("id"),
            relationships=resolved,
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

    def _get(
        self,
        path: str,
        obj_id: int,
        clazz: Type[ModelType],
        resolve_relationships=False,
    ) -> ModelType:
        response = self._session.get(self._build_url(path, str(obj_id)))
        json_response = response.json()
        logging.debug(json_response)
        response.raise_for_status()
        return self._obj_factory(
            json_response["data"], clazz, resolve_relationships=resolve_relationships
        )

    @staticmethod
    def _one(response):
        json_response = response.json()
        logging.debug(json_response)
        num_results = json_response["meta"]["total"]
        if num_results == 0:
            raise NoResultFound
        elif num_results > 1:
            raise MultipleResultsFound

        return json_response["data"][0]

    def _search_one(self, path: str, filters):
        params = {"filter[objects]": json.dumps(filters)}
        response = self._session.get(self._build_url(path), params=params)
        response.raise_for_status()
        return Catalog._one(response)

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
        response.raise_for_status()
        logging.debug(response.text)
        json_response = response.json()
        return json_response["data"]

    def _patch(self, path: str, obj_id: int, data: Dict[str, Any], type: str):
        payload = {"data": {"type": type, "attributes": data, "id": obj_id}}
        response = self._session.patch(
            url=self._build_url(path, str(obj_id)),
            data=json.dumps(payload, default=str),
        )
        response.raise_for_status()
        return

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
        return self._get("sources", obj_id, Source)

    def get_schema_by_id(self, obj_id) -> Schema:
        return self._get("schemata", obj_id, Schema)

    def get_table_by_id(self, obj_id) -> Table:
        return self._get("tables", obj_id, Table)

    def get_column_by_id(self, obj_id) -> Column:
        return self._get("columns", obj_id, Column)

    def get_job_by_id(self, obj_id) -> Job:
        return self._get("jobs", obj_id, Job)

    def get_job_execution_by_id(self, obj_id) -> JobExecution:
        return self._get("job_executions", obj_id, JobExecution)

    def get_column_lineage(self, job_ids: List[int]) -> List[ColumnLineage]:
        params = {"job_ids": job_ids}
        response = self._session.get(self._build_url("column_lineage"), params=params)
        logging.debug(response.json())
        response.raise_for_status()
        return [
            ColumnLineage(
                session=self._session,
                attributes=item["attributes"],
                obj_id=item["id"],
                relationships=item["relationships"],
            )
            for item in response.json()["data"]
        ]

    def get_source(self, name) -> Source:
        filters = [dict(name="name", op="eq", val="{}".format(name))]
        try:
            payload = self._search_one("sources", filters)
        except NoResultFound:
            raise SourceNotFound("Source not found: source_name={}".format(name))

        return self._obj_factory(payload, Source)

    def get_schema(self, source_name: str, schema_name: str) -> Schema:
        name_filter = dict(name="name", op="eq", val=schema_name)
        source_filter = dict(
            name="source", op="has", val=dict(name="name", op="eq", val=source_name)
        )
        filters = {"and": [name_filter, source_filter]}
        logging.debug(filters)
        try:
            payload = self._search_one("schemata", [filters])
        except NoResultFound:
            raise SchemaNotFound(
                "Schema not found, (source_name={}, schema_name={})".format(
                    source_name, schema_name
                )
            )
        return self._obj_factory(payload, Schema)

    def get_table(self, source_name: str, schema_name: str, table_name: str) -> Table:
        schema = self.get_schema(source_name, schema_name)

        name_filter = dict(name="name", op="eq", val=table_name)
        schema_id_filter = dict(name="schema_id", op="eq", val=str(schema.id))
        filters = {"and": [name_filter, schema_id_filter]}
        logging.debug(filters)
        try:
            payload = self._search_one("tables", [filters])
        except NoResultFound:
            raise TableNotFound(
                "Table not found, (source_name={}, schema_name={}, table_name={})".format(
                    source_name, schema_name, table_name
                )
            )
        return self._obj_factory(payload, Table)

    def get_columns_for_table(self, table: Table):
        return self._index("tables/{}/columns".format(table.id), Column)

    def get_column(self, source_name, schema_name, table_name, column_name) -> Column:
        table = self.get_table(source_name, schema_name, table_name)
        name_filter = dict(name="name", op="eq", val=column_name)
        table_filter = dict(name="table_id", op="eq", val=str(table.id))
        filters = {"and": [name_filter, table_filter]}
        logging.debug(filters)
        try:
            payload = self._search_one("columns", [filters])
        except NoResultFound:
            raise ColumnNotFound(
                "Column not found, (source_name={}, schema_name={}, table_name={}, column_name={})".format(
                    source_name, schema_name, table_name, column_name
                )
            )
        return self._obj_factory(payload, Column)

    def add_source(self, name: str, source_type: str, **kwargs) -> Source:
        data = {"name": name, "source_type": source_type, **kwargs}
        payload = self._post(path="sources", data=data, type="sources")
        return self._obj_factory(payload, Source)

    def scan_source(self, source: Source) -> bool:
        payload = {"id": source.id}
        response = self._session.post(
            url=self._build_url("scanner"), data=json.dumps(payload)
        )
        response.raise_for_status()
        return response.status_code == 200

    def add_schema(self, name: str, source: Source) -> Schema:
        data = {"name": name, "source_id": source.id}
        payload = self._post(path="schemata", data=data, type="schemata")
        return self._obj_factory(payload, Schema)

    def add_table(self, name: str, schema: Schema) -> Table:
        data = {"name": name, "schema_id": schema.id}
        payload = self._post(path="tables", data=data, type="tables")
        return self._obj_factory(payload, Table)

    def add_column(
        self, name: str, data_type: str, sort_order: int, table: Table
    ) -> Column:
        data = {
            "name": name,
            "table_id": table.id,
            "data_type": data_type,
            "sort_order": sort_order,
        }
        payload = self._post(path="columns", data=data, type="columns")
        return self._obj_factory(payload, Column)

    def add_job(self, name: str, context: Dict[Any, Any]) -> Job:
        data = {"name": name, "context": context}
        payload = self._post(path="jobs", data=data, type="jobs")
        return self._obj_factory(payload, Job)

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
        return self._obj_factory(payload, JobExecution)

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
        return self._obj_factory(payload, ColumnLineage)

    def update_source(self, source: Source, schema: Schema) -> DefaultSchema:
        try:
            current_obj = self._get(
                path="default_schema",
                obj_id=source.id,
                clazz=DefaultSchema,
                resolve_relationships=True,
            )
            if current_obj.schema.id == schema.id:
                return current_obj
        except HTTPError as error:
            if error.response.status_code == 404:
                data = {"source_id": source.id, "schema_id": schema.id}
                payload = self._post(
                    path="default_schema", data=data, type="default_schema"
                )
                return self._obj_factory(
                    payload, DefaultSchema, resolve_relationships=True
                )

        # Patch
        data = {"schema_id": schema.id}
        self._patch(
            path="default_schema", data=data, type="default_schema", obj_id=source.id
        )
        return self._get(
            path="default_schema",
            obj_id=source.id,
            clazz=DefaultSchema,
            resolve_relationships=True,
        )


class Analyze:
    def __init__(self, url: str):
        self._base_url = furl(url) / "api/v1/analyze"
        self._session = requests.Session()

    def analyze(
        self,
        query: str,
        source: Source,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        name: str = None,
    ) -> JobExecution:
        response = self._session.post(
            self._base_url,
            params={
                "query": query,
                "name": name,
                "source_id": source.id,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
            },
        )
        if response.status_code == 441:
            raise TableNotFound(response.json()["message"])
        elif response.status_code == 442:
            raise ColumnNotFound(response.json()["message"])
        elif response.status_code == 422:
            raise ParseError(response.json()["message"])
        elif response.status_code == 443:
            raise SemanticError(response.json()["message"])

        logging.debug(response.text)
        response.raise_for_status()
        payload = response.json()["data"]
        return JobExecution(
            session=self._session,
            attributes=payload.get("attributes"),
            obj_id=payload.get("id"),
            relationships=None,
        )


class Parse:
    def __init__(self, url: str):
        self._base_url = furl(url) / "api/v1/parse"
        self._session = requests.Session()

    def parse(self, query: str, source: Source):
        response = self._session.post(
            self._base_url, params={"query": query, "source_id": source.id},
        )
        logging.debug(response.text)
        response.raise_for_status()
        return response.json()
