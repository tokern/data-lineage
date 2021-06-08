--
-- PostgreSQL database dump
--

-- Dumped from database version 13.2 (Debian 13.2-1.pgdg100+1)
-- Dumped by pg_dump version 13.3 (Ubuntu 13.3-1.pgdg20.04+1)

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

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: filtered_pagecounts; Type: TABLE; Schema: public; Owner: etl_dev
--

CREATE TABLE public.filtered_pagecounts (
    "group" character varying,
    page_title character varying,
    views bigint,
    bytes_sent bigint
);


ALTER TABLE public.filtered_pagecounts OWNER TO etl_dev;

--
-- Name: lookup; Type: TABLE; Schema: public; Owner: etl_dev
--

CREATE TABLE public.lookup (
    redirect_id bigint,
    redirect_title bigint,
    true_title character varying,
    page_id bigint,
    page_version bigint
);


ALTER TABLE public.lookup OWNER TO etl_dev;

--
-- Name: normalized_pagecounts; Type: TABLE; Schema: public; Owner: etl_dev
--

CREATE TABLE public.normalized_pagecounts (
    "group" character varying,
    page_title character varying,
    views bigint,
    bytes_sent bigint
);


ALTER TABLE public.normalized_pagecounts OWNER TO etl_dev;

--
-- Name: page; Type: TABLE; Schema: public; Owner: etl_dev
--

CREATE TABLE public.page (
    pageid bigint,
    page_latest bigint,
    page_title character varying
);


ALTER TABLE public.page OWNER TO etl_dev;

--
-- Name: page_lookup_nonredirect; Type: TABLE; Schema: public; Owner: etl_dev
--

CREATE TABLE public.page_lookup_nonredirect (
    redirect_id bigint,
    redirect_title bigint,
    true_title character varying,
    page_id bigint,
    page_version bigint
);


ALTER TABLE public.page_lookup_nonredirect OWNER TO etl_dev;

--
-- Name: page_lookup_redirect; Type: TABLE; Schema: public; Owner: etl_dev
--

CREATE TABLE public.page_lookup_redirect (
    redirect_id bigint,
    redirect_title bigint,
    true_title character varying,
    page_id bigint,
    page_version bigint
);


ALTER TABLE public.page_lookup_redirect OWNER TO etl_dev;

--
-- Name: pagecounts; Type: TABLE; Schema: public; Owner: etl_dev
--

CREATE TABLE public.pagecounts (
    "group" character varying,
    page_title character varying,
    views bigint,
    bytes_sent bigint
);


ALTER TABLE public.pagecounts OWNER TO etl_dev;

--
-- Name: redirect; Type: TABLE; Schema: public; Owner: etl_dev
--

CREATE TABLE public.redirect (
    rd_from bigint,
    page_title character varying
);


ALTER TABLE public.redirect OWNER TO etl_dev;

--
-- Data for Name: filtered_pagecounts; Type: TABLE DATA; Schema: public; Owner: etl_dev
--

COPY public.filtered_pagecounts ("group", page_title, views, bytes_sent) FROM stdin;
\.


--
-- Data for Name: lookup; Type: TABLE DATA; Schema: public; Owner: etl_dev
--

COPY public.lookup (redirect_id, redirect_title, true_title, page_id, page_version) FROM stdin;
\.


--
-- Data for Name: normalized_pagecounts; Type: TABLE DATA; Schema: public; Owner: etl_dev
--

COPY public.normalized_pagecounts ("group", page_title, views, bytes_sent) FROM stdin;
\.


--
-- Data for Name: page; Type: TABLE DATA; Schema: public; Owner: etl_dev
--

COPY public.page (pageid, page_latest, page_title) FROM stdin;
\.


--
-- Data for Name: page_lookup_nonredirect; Type: TABLE DATA; Schema: public; Owner: etl_dev
--

COPY public.page_lookup_nonredirect (redirect_id, redirect_title, true_title, page_id, page_version) FROM stdin;
\.


--
-- Data for Name: page_lookup_redirect; Type: TABLE DATA; Schema: public; Owner: etl_dev
--

COPY public.page_lookup_redirect (redirect_id, redirect_title, true_title, page_id, page_version) FROM stdin;
\.


--
-- Data for Name: pagecounts; Type: TABLE DATA; Schema: public; Owner: etl_dev
--

COPY public.pagecounts ("group", page_title, views, bytes_sent) FROM stdin;
\.


--
-- Data for Name: redirect; Type: TABLE DATA; Schema: public; Owner: etl_dev
--

COPY public.redirect (rd_from, page_title) FROM stdin;
\.


--
-- PostgreSQL database dump complete
--

