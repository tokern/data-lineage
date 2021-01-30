from data_lineage.parser.dml_visitor import (  # type: ignore
    CopyFromVisitor,
    SelectIntoVisitor,
    SelectSourceVisitor,
)


def get_dml_queries(parsed):
    queries = []
    for node in parsed:
        select_source_visitor = SelectSourceVisitor()
        select_into_visitor = SelectIntoVisitor()
        copy_from_visitor = CopyFromVisitor()

        for visitor in [select_source_visitor, select_into_visitor, copy_from_visitor]:
            node.accept(visitor)
            if len(visitor.source_tables) > 0 and visitor.target_table is not None:
                queries.append(visitor)
                break

    return queries
