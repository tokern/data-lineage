--
-- PostgreSQL database dump
--

-- Dumped from database version 13.2 (Debian 13.2-1.pgdg100+1)
-- Dumped by pg_dump version 13.2 (Ubuntu 13.2-1.pgdg20.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: jobexecutionstatus; Type: TYPE; Schema: public; Owner: catalog_user
--

CREATE TYPE public.jobexecutionstatus AS ENUM (
    'SUCCESS',
    'FAILURE'
);


ALTER TYPE public.jobexecutionstatus OWNER TO catalog_user;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: column_lineage; Type: TABLE; Schema: public; Owner: catalog_user
--

CREATE TABLE public.column_lineage (
    id integer NOT NULL,
    context jsonb,
    source_id integer,
    target_id integer,
    job_execution_id integer
);


ALTER TABLE public.column_lineage OWNER TO catalog_user;

--
-- Name: column_lineage_id_seq; Type: SEQUENCE; Schema: public; Owner: catalog_user
--

CREATE SEQUENCE public.column_lineage_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.column_lineage_id_seq OWNER TO catalog_user;

--
-- Name: column_lineage_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: catalog_user
--

ALTER SEQUENCE public.column_lineage_id_seq OWNED BY public.column_lineage.id;


--
-- Name: columns; Type: TABLE; Schema: public; Owner: catalog_user
--

CREATE TABLE public.columns (
    id integer NOT NULL,
    name character varying,
    type character varying,
    sort_order integer,
    table_id integer
);


ALTER TABLE public.columns OWNER TO catalog_user;

--
-- Name: columns_id_seq; Type: SEQUENCE; Schema: public; Owner: catalog_user
--

CREATE SEQUENCE public.columns_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.columns_id_seq OWNER TO catalog_user;

--
-- Name: columns_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: catalog_user
--

ALTER SEQUENCE public.columns_id_seq OWNED BY public.columns.id;


--
-- Name: job_executions; Type: TABLE; Schema: public; Owner: catalog_user
--

CREATE TABLE public.job_executions (
    id integer NOT NULL,
    job_id integer,
    started_at timestamp without time zone,
    ended_at timestamp without time zone,
    status public.jobexecutionstatus
);


ALTER TABLE public.job_executions OWNER TO catalog_user;

--
-- Name: job_executions_id_seq; Type: SEQUENCE; Schema: public; Owner: catalog_user
--

CREATE SEQUENCE public.job_executions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.job_executions_id_seq OWNER TO catalog_user;

--
-- Name: job_executions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: catalog_user
--

ALTER SEQUENCE public.job_executions_id_seq OWNED BY public.job_executions.id;


--
-- Name: jobs; Type: TABLE; Schema: public; Owner: catalog_user
--

CREATE TABLE public.jobs (
    id integer NOT NULL,
    name character varying,
    context jsonb
);


ALTER TABLE public.jobs OWNER TO catalog_user;

--
-- Name: jobs_id_seq; Type: SEQUENCE; Schema: public; Owner: catalog_user
--

CREATE SEQUENCE public.jobs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.jobs_id_seq OWNER TO catalog_user;

--
-- Name: jobs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: catalog_user
--

ALTER SEQUENCE public.jobs_id_seq OWNED BY public.jobs.id;


--
-- Name: schemata; Type: TABLE; Schema: public; Owner: catalog_user
--

CREATE TABLE public.schemata (
    id integer NOT NULL,
    name character varying,
    source_id integer
);


ALTER TABLE public.schemata OWNER TO catalog_user;

--
-- Name: schemata_id_seq; Type: SEQUENCE; Schema: public; Owner: catalog_user
--

CREATE SEQUENCE public.schemata_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.schemata_id_seq OWNER TO catalog_user;

--
-- Name: schemata_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: catalog_user
--

ALTER SEQUENCE public.schemata_id_seq OWNED BY public.schemata.id;


--
-- Name: sources; Type: TABLE; Schema: public; Owner: catalog_user
--

CREATE TABLE public.sources (
    id integer NOT NULL,
    type character varying,
    name character varying,
    dialect character varying,
    uri character varying,
    port character varying,
    username character varying,
    password character varying,
    database character varying,
    instance character varying,
    cluster character varying,
    project_id character varying,
    project_credentials character varying,
    page_size character varying,
    filter_key character varying,
    included_tables_regex character varying,
    key_path character varying,
    account character varying,
    role character varying,
    warehouse character varying
);


ALTER TABLE public.sources OWNER TO catalog_user;

--
-- Name: sources_id_seq; Type: SEQUENCE; Schema: public; Owner: catalog_user
--

CREATE SEQUENCE public.sources_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sources_id_seq OWNER TO catalog_user;

--
-- Name: sources_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: catalog_user
--

ALTER SEQUENCE public.sources_id_seq OWNED BY public.sources.id;


--
-- Name: tables; Type: TABLE; Schema: public; Owner: catalog_user
--

CREATE TABLE public.tables (
    id integer NOT NULL,
    name character varying,
    schema_id integer
);


ALTER TABLE public.tables OWNER TO catalog_user;

--
-- Name: tables_id_seq; Type: SEQUENCE; Schema: public; Owner: catalog_user
--

CREATE SEQUENCE public.tables_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tables_id_seq OWNER TO catalog_user;

--
-- Name: tables_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: catalog_user
--

ALTER SEQUENCE public.tables_id_seq OWNED BY public.tables.id;


--
-- Name: column_lineage id; Type: DEFAULT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.column_lineage ALTER COLUMN id SET DEFAULT nextval('public.column_lineage_id_seq'::regclass);


--
-- Name: columns id; Type: DEFAULT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.columns ALTER COLUMN id SET DEFAULT nextval('public.columns_id_seq'::regclass);


--
-- Name: job_executions id; Type: DEFAULT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.job_executions ALTER COLUMN id SET DEFAULT nextval('public.job_executions_id_seq'::regclass);


--
-- Name: jobs id; Type: DEFAULT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.jobs ALTER COLUMN id SET DEFAULT nextval('public.jobs_id_seq'::regclass);


--
-- Name: schemata id; Type: DEFAULT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.schemata ALTER COLUMN id SET DEFAULT nextval('public.schemata_id_seq'::regclass);


--
-- Name: sources id; Type: DEFAULT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.sources ALTER COLUMN id SET DEFAULT nextval('public.sources_id_seq'::regclass);


--
-- Name: tables id; Type: DEFAULT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.tables ALTER COLUMN id SET DEFAULT nextval('public.tables_id_seq'::regclass);


--
-- Data for Name: column_lineage; Type: TABLE DATA; Schema: public; Owner: catalog_user
--

COPY public.column_lineage (id, context, source_id, target_id, job_execution_id) FROM stdin;
1	{}	4	9	1
2	{}	6	10	1
3	{}	6	11	1
4	{}	4	12	1
5	{}	5	13	1
6	{}	4	14	2
7	{}	6	15	2
8	{}	6	16	2
9	{}	4	17	2
10	{}	5	18	2
11	{}	14	19	3
12	{}	15	20	3
13	{}	16	21	3
14	{}	17	22	3
15	{}	18	23	3
16	{}	22	28	5
17	{}	21	29	5
18	{}	26	30	5
19	{}	27	31	5
\.


--
-- Data for Name: columns; Type: TABLE DATA; Schema: public; Owner: catalog_user
--

COPY public.columns (id, name, type, sort_order, table_id) FROM stdin;
1	group	STRING	0	1
2	page_title	STRING	1	1
3	views	BIGINT	2	1
4	page_id	BIGINT	0	2
5	page_latest	BIGINT	1	2
6	page_title	STRING	2	2
7	rd_from	BIGINT	0	3
8	page_title	STRING	1	3
9	redirect_id	BIGINT	0	4
10	redirect_title	STRING	1	4
11	true_title	STRING	2	4
12	page_id	BIGINT	3	4
13	page_version	BIGINT	4	4
14	redirect_id	BIGINT	0	5
15	redirect_title	STRING	1	5
16	true_title	STRING	2	5
17	page_id	BIGINT	3	5
18	page_version	BIGINT	4	5
19	redirect_id	bigint	0	6
20	redirect_title	STRING	1	6
21	true_title	STRING	2	6
22	page_id	BIGINT	3	6
23	page_version	BIGINT	4	6
24	group	STRING	0	7
25	page_title	STRING	1	7
26	views	BIGINT	2	7
27	bytes_sent	BIGINT	3	7
28	page_id	BIGINT	0	8
29	page_title	STRING	1	8
30	page_url	STRING	2	8
31	views	BIGINT	3	8
32	bytes_sent	BIGINT	4	8
\.


--
-- Data for Name: job_executions; Type: TABLE DATA; Schema: public; Owner: catalog_user
--

COPY public.job_executions (id, job_id, started_at, ended_at, status) FROM stdin;
1	1	2021-04-20 19:53:23.209047	2021-04-20 19:53:23.209056	SUCCESS
2	2	2021-04-20 19:53:23.322106	2021-04-20 19:53:23.322114	SUCCESS
3	3	2021-04-20 19:53:23.443469	2021-04-20 19:53:23.443476	SUCCESS
4	4	2021-04-20 19:53:23.610752	2021-04-20 19:53:23.61076	SUCCESS
5	5	2021-04-20 19:53:23.622216	2021-04-20 19:53:23.622226	SUCCESS
\.


--
-- Data for Name: jobs; Type: TABLE DATA; Schema: public; Owner: catalog_user
--

COPY public.jobs (id, name, context) FROM stdin;
1	LOAD page_lookup_nonredirect	{}
2	LOAD page_lookup_redirect	{}
3	LOAD page_lookup	{}
4	LOAD filtered_pagecounts	{}
5	LOAD normalized_pagecounts	{}
\.


--
-- Data for Name: schemata; Type: TABLE DATA; Schema: public; Owner: catalog_user
--

COPY public.schemata (id, name, source_id) FROM stdin;
1	default	1
\.


--
-- Data for Name: sources; Type: TABLE DATA; Schema: public; Owner: catalog_user
--

COPY public.sources (id, type, name, dialect, uri, port, username, password, database, instance, cluster, project_id, project_credentials, page_size, filter_key, included_tables_regex, key_path, account, role, warehouse) FROM stdin;
1	json	test	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
\.


--
-- Data for Name: tables; Type: TABLE DATA; Schema: public; Owner: catalog_user
--

COPY public.tables (id, name, schema_id) FROM stdin;
1	pagecounts	1
2	page	1
3	redirect	1
4	page_lookup_nonredirect	1
5	page_lookup_redirect	1
6	page_lookup	1
7	filtered_pagecounts	1
8	normalized_pagecounts	1
\.


--
-- Name: column_lineage_id_seq; Type: SEQUENCE SET; Schema: public; Owner: catalog_user
--

SELECT pg_catalog.setval('public.column_lineage_id_seq', 19, true);


--
-- Name: columns_id_seq; Type: SEQUENCE SET; Schema: public; Owner: catalog_user
--

SELECT pg_catalog.setval('public.columns_id_seq', 32, true);


--
-- Name: job_executions_id_seq; Type: SEQUENCE SET; Schema: public; Owner: catalog_user
--

SELECT pg_catalog.setval('public.job_executions_id_seq', 5, true);


--
-- Name: jobs_id_seq; Type: SEQUENCE SET; Schema: public; Owner: catalog_user
--

SELECT pg_catalog.setval('public.jobs_id_seq', 5, true);


--
-- Name: schemata_id_seq; Type: SEQUENCE SET; Schema: public; Owner: catalog_user
--

SELECT pg_catalog.setval('public.schemata_id_seq', 1, true);


--
-- Name: sources_id_seq; Type: SEQUENCE SET; Schema: public; Owner: catalog_user
--

SELECT pg_catalog.setval('public.sources_id_seq', 1, true);


--
-- Name: tables_id_seq; Type: SEQUENCE SET; Schema: public; Owner: catalog_user
--

SELECT pg_catalog.setval('public.tables_id_seq', 8, true);


--
-- Name: column_lineage column_lineage_pkey; Type: CONSTRAINT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.column_lineage
    ADD CONSTRAINT column_lineage_pkey PRIMARY KEY (id);


--
-- Name: columns columns_pkey; Type: CONSTRAINT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.columns
    ADD CONSTRAINT columns_pkey PRIMARY KEY (id);


--
-- Name: job_executions job_executions_pkey; Type: CONSTRAINT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.job_executions
    ADD CONSTRAINT job_executions_pkey PRIMARY KEY (id);


--
-- Name: jobs jobs_name_key; Type: CONSTRAINT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_name_key UNIQUE (name);


--
-- Name: jobs jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (id);


--
-- Name: schemata schemata_pkey; Type: CONSTRAINT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.schemata
    ADD CONSTRAINT schemata_pkey PRIMARY KEY (id);


--
-- Name: sources sources_pkey; Type: CONSTRAINT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.sources
    ADD CONSTRAINT sources_pkey PRIMARY KEY (id);


--
-- Name: tables tables_pkey; Type: CONSTRAINT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.tables
    ADD CONSTRAINT tables_pkey PRIMARY KEY (id);


--
-- Name: column_lineage column_lineage_job_execution_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.column_lineage
    ADD CONSTRAINT column_lineage_job_execution_id_fkey FOREIGN KEY (job_execution_id) REFERENCES public.job_executions(id);


--
-- Name: column_lineage column_lineage_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.column_lineage
    ADD CONSTRAINT column_lineage_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.columns(id);


--
-- Name: column_lineage column_lineage_target_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.column_lineage
    ADD CONSTRAINT column_lineage_target_id_fkey FOREIGN KEY (target_id) REFERENCES public.columns(id);


--
-- Name: columns columns_table_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.columns
    ADD CONSTRAINT columns_table_id_fkey FOREIGN KEY (table_id) REFERENCES public.tables(id);


--
-- Name: job_executions job_executions_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.job_executions
    ADD CONSTRAINT job_executions_job_id_fkey FOREIGN KEY (job_id) REFERENCES public.jobs(id);


--
-- Name: schemata schemata_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.schemata
    ADD CONSTRAINT schemata_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(id);


--
-- Name: tables tables_schema_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: catalog_user
--

ALTER TABLE ONLY public.tables
    ADD CONSTRAINT tables_schema_id_fkey FOREIGN KEY (schema_id) REFERENCES public.schemata(id);


--
-- PostgreSQL database dump complete
--

