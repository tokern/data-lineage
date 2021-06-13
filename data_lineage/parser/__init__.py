import logging
from datetime import datetime
from typing import List, Optional

from dbcat.catalog import Catalog
from dbcat.catalog.models import JobExecution, JobExecutionStatus
from pglast.parser import ParseError

from data_lineage.parser.dml_visitor import (
    CopyFromVisitor,
    DmlVisitor,
    SelectIntoVisitor,
    SelectSourceVisitor,
)
from data_lineage.parser.node import Parsed, parse


def parse_queries(queries: List[str]) -> List[Parsed]:
    parsed: List[Parsed] = []

    for query in queries:
        try:
            parsed.append(parse(query))
        except ParseError as e:
            logging.warning("Syntax error while parsing {}.\n{}".format(query, e))

    return parsed


def visit_dml_query(catalog: Catalog, parsed: Parsed) -> Optional[DmlVisitor]:
    select_source_visitor: DmlVisitor = SelectSourceVisitor(parsed.name)
    select_into_visitor: DmlVisitor = SelectIntoVisitor(parsed.name)
    copy_from_visitor: DmlVisitor = CopyFromVisitor(parsed.name)

    for visitor in [select_source_visitor, select_into_visitor, copy_from_visitor]:
        parsed.node.accept(visitor)
        if len(visitor.source_tables) > 0 and visitor.target_table is not None:
            visitor.bind(catalog)
            return visitor
    return None


def extract_lineage(
    catalog: Catalog, visited_query: DmlVisitor, parsed: Parsed
) -> JobExecution:
    job = catalog.add_job(parsed.name, {"query": parsed.query})
    job_execution = catalog.add_job_execution(
        job, datetime.now(), datetime.now(), JobExecutionStatus.SUCCESS
    )
    for source, target in zip(
        visited_query.source_columns, visited_query.target_columns
    ):
        edge = catalog.add_column_lineage(source, target, job_execution.id, {})
        logging.debug("Added {}".format(edge))

    return job_execution
