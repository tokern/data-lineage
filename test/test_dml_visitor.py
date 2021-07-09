import pytest

from data_lineage.parser import analyze_dml_query, parse, parse_dml_query, parse_queries
from data_lineage.parser.dml_visitor import (
    CTASVisitor,
    SelectIntoVisitor,
    SelectSourceVisitor,
)


@pytest.mark.parametrize(
    "target, sources, sql",
    [
        ((None, "c"), [(None, "a")], "insert into c select x,y from a"),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "insert into c select x,y from a join b on a.id = b.id",
        ),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "insert into c select x,y from a join b on a.id = b.id",
        ),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "insert into c select x,y from a as aa join b on " "aa.id = b.id",
        ),
    ],
)
def test_sanity_insert(target, sources, sql):
    parsed = parse(sql)
    insert_visitor = SelectSourceVisitor("test_sanity_insert")
    insert_visitor(parsed.node)
    bound_target, bound_tables, bound_cols = insert_visitor.resolve()

    assert bound_target == target
    assert bound_tables == sources


@pytest.mark.parametrize(
    "target, sources, sql",
    [
        ((None, "c"), [(None, "a")], "create table c as select x,y from a"),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "create table c as select x,y from a join b on a.id = b.id",
        ),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "create table c as select x,y from a join b on a.id = b.id",
        ),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "create table c as select x,y from a as aa join b on aa.id = b.id",
        ),
    ],
)
def test_sanity_ctas(target, sources, sql):
    parsed = parse(sql)
    visitor = CTASVisitor("test_sanity_ctas")
    visitor(parsed.node)
    bound_target, bound_tables, bound_cols = visitor.resolve()

    assert bound_target == target
    assert bound_tables == sources


@pytest.mark.parametrize(
    "target, sources, sql",
    [
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "select x,y into c from a join b on a.id = b.id",
        ),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "select x,y into c from a join b on a.id = b.id",
        ),
        (
            (None, "c"),
            [(None, "a"), (None, "b")],
            "select x,y into c from a as aa join b on aa.id = b.id",
        ),
    ],
)
def test_sanity_select_into(target, sources, sql):
    parsed = parse(sql)
    visitor = SelectIntoVisitor("test_sanity_select_into")
    visitor(parsed.node)
    bound_target, bound_tables, bound_cols = visitor.resolve()

    assert bound_target == target
    assert bound_tables == sources


@pytest.mark.parametrize(
    "query",
    [
        "INSERT INTO page_lookup SELECT plr.redirect_id, plr.redirect_title, plr.true_title, plr.page_id, plr.page_version FROM page_lookup_redirect plr",
        "INSERT INTO page_lookup SELECT redirect_id, redirect_title, true_title, page_id, page_version FROM page_lookup_redirect",
        "INSERT INTO page_lookup SELECT page_lookup_redirect.* FROM page_lookup_redirect",
        "INSERT INTO page_lookup SELECT * FROM page_lookup_redirect",
        'INSERT INTO "default".page_lookup SELECT * FROM page_lookup_redirect',
        "SELECT * INTO page_lookup from page_lookup_redirect",
        'SELECT * INTO "default".page_lookup from page_lookup_redirect',
        """
            INSERT INTO page_lookup
            SELECT * FROM (
                    select redirect_id, redirect_title, true_title, page_id, page_version FROM page_lookup_redirect
                    ) plr
        """,
        """
            INSERT INTO page_lookup
            SELECT plr.* FROM (
                    select redirect_id, redirect_title, true_title, page_id, page_version FROM page_lookup_redirect
                    ) plr
        """,
        """
            INSERT INTO page_lookup
            SELECT redirect_id, redirect_title, true_title, page_id, page_version FROM (
                    select redirect_id, redirect_title, true_title, page_id, page_version FROM page_lookup_redirect
                    ) plr
        """,
        """
            INSERT INTO page_lookup
            SELECT plr.redirect_id, plr.redirect_title, plr.true_title, plr.page_id, plr.page_version FROM (
                    select redirect_id, redirect_title, true_title, page_id, page_version FROM page_lookup_redirect
                    ) plr
        """,
    ],
)
def test_insert(managed_session, query):
    source = managed_session.get_source("test")
    parsed = parse(query)
    visitor = analyze_dml_query(managed_session, parsed, source)
    assert visitor is not None

    assert len(visitor.target_columns) == 5
    assert visitor.target_table.fqdn == ("test", "default", "page_lookup")
    assert len(visitor.source_columns) == 5
    assert [table.fqdn for table in visitor.source_tables] == [
        ("test", "default", "page_lookup_redirect")
    ]


def test_insert_cols(managed_session):
    source = managed_session.get_source("test")
    query = "INSERT INTO page_lookup_nonredirect(page_id, page_version) SELECT page.page_id, page.page_latest FROM page"
    parsed = parse(query)
    visitor = analyze_dml_query(managed_session, parsed, source)
    assert visitor is not None

    assert len(visitor.target_columns) == 2
    assert visitor.target_table.fqdn == ("test", "default", "page_lookup_nonredirect")
    assert len(visitor.source_columns) == 2
    assert [table.fqdn for table in visitor.source_tables] == [
        ("test", "default", "page")
    ]


def test_insert_with_join(managed_session):
    source = managed_session.get_source("test")
    query = "insert into page_lookup_redirect select original_page.page_id redirect_id, original_page.page_title redirect_title, final_page.page_title as true_title, final_page.page_id, final_page.page_latest from page final_page join redirect on (redirect.page_title = final_page.page_title) join page original_page on (redirect.rd_from = original_page.page_id)"
    parsed = parse(query)
    visitor = analyze_dml_query(managed_session, parsed, source)
    assert visitor is not None

    assert len(visitor.target_columns) == 5
    assert visitor.target_table.fqdn == ("test", "default", "page_lookup_redirect")
    assert len(visitor.source_columns) == 5
    assert sorted([table.fqdn for table in visitor.source_tables]) == [
        ("test", "default", "page"),
        ("test", "default", "redirect"),
    ]


@pytest.mark.parametrize(
    "query",
    [
        "with pln as (select redirect_title, true_title, page_id, page_version from page_lookup_nonredirect) insert into page_lookup_redirect (redirect_title, true_title, page_id, page_version) select redirect_title, true_title, page_id, page_version from pln;",
        "with pln as (select * from page_lookup_nonredirect) insert into page_lookup_redirect (redirect_title, true_title, page_id, page_version) select redirect_title, true_title, page_id, page_version from pln;",
        "with pln as (select redirect_title, true_title, page_id, page_version from page_lookup_nonredirect) insert into page_lookup_redirect (redirect_title, true_title, page_id, page_version) select * from pln;",
        "with pln as (select redirect_title as t1, true_title as t2, page_id as t3, page_version as t4 from page_lookup_nonredirect) insert into page_lookup_redirect (redirect_title, true_title, page_id, page_version) select t1, t2, t3, t4 from pln;",
        "insert into page_lookup_redirect (redirect_title, true_title, page_id, page_version) with pln as (select redirect_title, true_title, page_id, page_version from page_lookup_nonredirect) select redirect_title, true_title, page_id, page_version from pln;",
    ],
)
def test_with_clause(managed_session, query):
    source = managed_session.get_source("test")
    parsed = parse(query)
    visitor = analyze_dml_query(managed_session, parsed, source)
    assert visitor is not None

    assert len(visitor.target_columns) == 4
    assert visitor.target_table.fqdn == ("test", "default", "page_lookup_redirect")
    assert len(visitor.source_columns) == 4
    assert [table.fqdn for table in visitor.source_tables] == [
        ("test", "default", "page_lookup_nonredirect")
    ]


def test_col_exprs(managed_session):
    query = """
        INSERT INTO page_lookup_redirect(true_title)
        SELECT
            BTRIM(TO_CHAR(DATEADD (MONTH,-1,('20' ||MAX ("redirect_id") || '-01')::DATE)::DATE,'YY-MM')) AS "max_month"
        FROM page_lookup_nonredirect;
    """
    source = managed_session.get_source("test")
    parsed = parse(query)
    visitor = analyze_dml_query(catalog=managed_session, parsed=parsed, source=source)
    assert visitor is not None

    assert len(visitor.target_columns) == 1
    assert visitor.target_table.fqdn == ("test", "default", "page_lookup_redirect")
    assert len(visitor.source_columns) == 1
    assert [table.fqdn for table in visitor.source_tables] == [
        ("test", "default", "page_lookup_nonredirect")
    ]


def test_syntax_errors():
    queries = [
        "INSERT INTO page_lookup_nonredirect(page_id, latest) SELECT page.page_id, page.page_latest FROM page",
        "select a from table(b)",
        "INSERT INTO page_lookup_nonredirect SELECT page.page_id, page.page_latest FROM page",
    ]

    parsed = parse_queries(queries)

    assert len(parsed) == 2


def test_parse_query(managed_session):
    query = """
    SELECT BTRIM(TO_CHAR(DATEADD (MONTH,-1,(\'20\' ||MAX ("group") || \'-01\')::DATE)::DATE,\'YY-MM\')) AS "max_month",
        DATEADD(YEAR,-1,DATEADD (MONTH,-3,LAST_DAY (DATEADD (MONTH,-1,(\'20\' ||MAX ("group") || \'-01\')::DATE)::DATE))::DATE)::DATE AS "min_date",
        DATEADD(MONTH,-3,LAST_DAY (DATEADD (MONTH,-1,(\'20\' ||MAX ("group") || \'-01\')::DATE)::DATE))::DATE AS "max_date",
        page_title,
        bytes_sent as mb_sent
    INTO "new_table"
    FROM pagecounts;
    """
    source = managed_session.get_source("test")
    parsed = parse(query)
    binder = parse_dml_query(catalog=managed_session, parsed=parsed, source=source)
    assert [context.alias for context in binder.columns] == [
        "max_month",
        "min_date",
        "max_date",
        "page_title",
        "mb_sent",
    ]


def test_ctas(managed_session):
    query = """
        CREATE TEMP TABLE temp_table_x(page_title) AS select redirect_title from page_lookup_nonredirect
        where redirect_title is not null
    """
    source = managed_session.get_source("test")
    schema = managed_session.get_schema("test", "default")
    managed_session.update_source(source, schema)
    parsed = parse(query)
    visitor = analyze_dml_query(managed_session, parsed, source)
    assert visitor is not None

    assert len(visitor.target_columns) == 1
    assert visitor.target_table.fqdn == ("test", "default", "temp_table_x")
    assert len(visitor.source_columns) == 1
    assert [table.fqdn for table in visitor.source_tables] == [
        ("test", "default", "page_lookup_nonredirect")
    ]
