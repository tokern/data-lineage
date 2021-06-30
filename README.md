# Tokern Lineage Engine

[![CircleCI](https://circleci.com/gh/tokern/data-lineage.svg?style=svg)](https://circleci.com/gh/tokern/data-lineage)
[![codecov](https://codecov.io/gh/tokern/data-lineage/branch/master/graph/badge.svg)](https://codecov.io/gh/tokern/data-lineage)
[![PyPI](https://img.shields.io/pypi/v/data-lineage.svg)](https://pypi.python.org/pypi/data-lineage)
[![image](https://img.shields.io/pypi/l/data-lineage.svg)](https://pypi.org/project/data-lineage/)
[![image](https://img.shields.io/pypi/pyversions/data-lineage.svg)](https://pypi.org/project/data-lineage/)


Tokern Lineage Engine is _fast_ and _easy to use_ application to collect, visualize and analyze 
column-level data lineage in databases, data warehouses and data lakes in AWS and GCP.

Tokern Lineage helps you browse column-level data lineage 
* visually using [kedro-viz](https://github.com/quantumblacklabs/kedro-viz)
* analyze lineage graphs programmatically using the powerful [networkx graph library](https://networkx.org/)

## Resources

* Demo of Tokern Lineage App

![data-lineage](https://user-images.githubusercontent.com/1638298/118261607-688a7100-b4d1-11eb-923a-5d2407d6bd8d.gif)

* Checkout an [example data lineage notebook](http://tokern.io/docs/data-lineage/example/).

* Check out [the post on using data lineage for cost control](https://tokern.io/blog/data-lineage-on-redshift/) for an 
example of how data lineage can be used in production.

## Quick Start

### Install a demo of using Docker and Docker Compose

Download the docker-compose file from Github repository.


    # in a new directory run
    wget https://raw.githubusercontent.com/tokern/data-lineage/master/install-manifests/docker-compose/catalog-demo.yml
    # or run
    curl https://raw.githubusercontent.com/tokern/data-lineage/master/install-manifests/docker-compose/catalog-demo.yml -o docker-compose.yml


Run docker-compose
   

    docker-compose up -d


Check that the containers are running.


    docker ps
    CONTAINER ID   IMAGE                                    CREATED        STATUS       PORTS                    NAMES
    3f4e77845b81   tokern/data-lineage-viz:latest   ...   4 hours ago    Up 4 hours   0.0.0.0:8000->80/tcp     tokern-data-lineage-visualizer
    1e1ce4efd792   tokern/data-lineage:latest       ...   5 days ago     Up 5 days                             tokern-data-lineage
    38be15bedd39   tokern/demodb:latest             ...   2 weeks ago    Up 2 weeks                            tokern-demodb

Try out Tokern Lineage App

Head to `http://localhost:8000/` to open the Tokern Lineage app

### Install Tokern Lineage Engine

    # in a new directory run
    wget https://raw.githubusercontent.com/tokern/data-lineage/master/install-manifests/docker-compose/tokern-lineage-engine.yml
    # or run
    curl https://raw.githubusercontent.com/tokern/data-lineage/master/install-manifests/docker-compose/catalog-demo.yml -o tokern-lineage-engine.yml

Run docker-compose
   

    docker-compose up -d


If you want to use an external Postgres database, replace the following parameters in `tokern-lineage-engine.yml`:

* CATALOG_HOST
* CATALOG_USER
* CATALOG_PASSWORD
* CATALOG_DB

## Supported Technologies

* Postgres
* AWS Redshift
* Snowflake

### Coming Soon

* SparkSQL
* Presto

## Documentation

For advanced usage, please refer to [data-lineage documentation](https://tokern.io/docs/data-lineage/index.html)
## Survey

Please take this [survey](https://forms.gle/p2oEQBJnpEguhrp3A) if you are a user or considering using data-lineage. Responses will help us prioritize features better. 
