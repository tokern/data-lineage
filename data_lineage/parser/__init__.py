from datetime import datetime
from typing import List

from dbcat.catalog import Catalog
from dbcat.catalog.models import JobExecutionStatus
from dbcat.log_mixin import LogMixin
from pglast.parser import ParseError

from data_lineage.graph import DbGraph
from data_lineage.parser.dml_visitor import (
    CopyFromVisitor,
    DmlVisitor,
    SelectIntoVisitor,
    SelectSourceVisitor,
)
from data_lineage.parser.node import Parsed, parse


def parse_queries(queries: List[str]) -> List[Parsed]:
    parsed: List[Parsed] = []
    logger = LogMixin().logger

    for query in queries:
        try:
            parsed.append(parse(query))
        except ParseError as e:
            logger.warn("Syntax error while parsing {}.\n{}".format(query, e))

    return parsed


def visit_dml_queries(catalog: Catalog, parsed_list: List[Parsed]) -> List[DmlVisitor]:
    queries = []
    for parsed in parsed_list:
        select_source_visitor: DmlVisitor = SelectSourceVisitor(parsed.name)
        select_into_visitor: DmlVisitor = SelectIntoVisitor(parsed.name)
        copy_from_visitor: DmlVisitor = CopyFromVisitor(parsed.name)

        for visitor in [select_source_visitor, select_into_visitor, copy_from_visitor]:
            parsed.node.accept(visitor)
            if len(visitor.source_tables) > 0 and visitor.target_table is not None:
                visitor.bind(catalog)
                queries.append(visitor)
                break

    return queries


def create_graph(catalog: Catalog, visited_queries: List[DmlVisitor]) -> DbGraph:
    logger = LogMixin()
    job_ids = set()
    for query in visited_queries:
        job = catalog.add_job(query.name, {})
        job_execution = catalog.add_job_execution(
            job, datetime.now(), datetime.now(), JobExecutionStatus.SUCCESS
        )
        for source, target in zip(query.source_columns, query.target_columns):
            edge = catalog.add_column_lineage(source, target, job_execution.id, {})
            job_ids.add(job.id)
            logger.logger.debug("Added {}".format(edge))

    graph = DbGraph(catalog, job_ids)
    graph.load()
    return graph
