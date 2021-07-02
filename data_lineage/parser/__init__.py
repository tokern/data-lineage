import logging
from typing import List

from dbcat.catalog import Catalog
from dbcat.catalog.models import CatSource, JobExecution, JobExecutionStatus
from pglast.parser import ParseError

from data_lineage import SemanticError
from data_lineage.parser.binder import SelectBinder
from data_lineage.parser.dml_visitor import (
    CopyFromVisitor,
    DmlVisitor,
    SelectIntoVisitor,
    SelectSourceVisitor,
)
from data_lineage.parser.node import Parsed, parse
from data_lineage.parser.visitor import ExprVisitor, RedshiftExprVisitor


def parse_queries(queries: List[str]) -> List[Parsed]:
    parsed: List[Parsed] = []

    for query in queries:
        try:
            parsed.append(parse(query))
        except ParseError as e:
            logging.warning("Syntax error while parsing {}.\n{}".format(query, e))

    return parsed


def analyze_dml_query(
    catalog: Catalog, parsed: Parsed, source: CatSource,
) -> DmlVisitor:
    chosen_visitor = visit_dml_query(parsed, source)
    chosen_visitor.bind(catalog=catalog, source=source)
    return chosen_visitor


def parse_dml_query(
    catalog: Catalog, parsed: Parsed, source: CatSource,
) -> SelectBinder:
    chosen_visitor = visit_dml_query(parsed, source)

    select_binder = SelectBinder(
        catalog,
        source,
        chosen_visitor.select_tables,
        chosen_visitor.select_columns,
        ("_U{}".format(i) for i in range(0, 1000)),
    )
    select_binder.bind()
    return select_binder


def visit_dml_query(parsed: Parsed, source: CatSource,) -> DmlVisitor:

    expr_visitor_clazz = ExprVisitor
    if source.source_type == "redshift":
        expr_visitor_clazz = RedshiftExprVisitor

    select_source_visitor: DmlVisitor = SelectSourceVisitor(
        parsed.name, expr_visitor_clazz
    )
    select_into_visitor: DmlVisitor = SelectIntoVisitor(parsed.name, expr_visitor_clazz)
    copy_from_visitor: DmlVisitor = CopyFromVisitor(parsed.name, expr_visitor_clazz)

    for v in [select_source_visitor, select_into_visitor, copy_from_visitor]:
        parsed.node.accept(v)
        if len(v.select_tables) > 0 and v.insert_table is not None:
            return v
    raise SemanticError("Query is not a DML Query")


def extract_lineage(
    catalog: Catalog,
    visited_query: DmlVisitor,
    source: CatSource,
    parsed: Parsed,
    start_time,
    end_time,
) -> JobExecution:
    job = catalog.add_job(
        name=parsed.name, source=source, context={"query": parsed.query}
    )
    job_execution = catalog.add_job_execution(
        job=job,
        started_at=start_time,
        ended_at=end_time,
        status=JobExecutionStatus.SUCCESS,
    )
    for source, target in zip(
        visited_query.source_columns, visited_query.target_columns
    ):
        for column in source.columns:
            edge = catalog.add_column_lineage(column, target, job_execution.id, {})
            logging.debug("Added {}".format(edge))

    return job_execution
