from pglast import parse_sql

from data_lineage.parser.node import AcceptingNode


def parse(sql):
    return AcceptingNode(parse_sql(sql))
