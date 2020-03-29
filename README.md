[![CircleCI](https://circleci.com/gh/tokern/data-lineage.svg?style=svg)](https://circleci.com/gh/tokern/data-lineage)
[![codecov](https://codecov.io/gh/tokern/data-lineage/branch/master/graph/badge.svg)](https://codecov.io/gh/tokern/data-lineage)
[![PyPI](https://img.shields.io/pypi/v/data-lineage.svg)](https://pypi.python.org/pypi/data-lineage)
[![image](https://img.shields.io/pypi/l/data-lineage.svg)](https://pypi.org/project/data-lineage/)
[![image](https://img.shields.io/pypi/pyversions/data-lineage.svg)](https://pypi.org/project/data-lineage/)

# Data Lineage for Databases and Data Lakes

Data Lineage is an open source application to query and visualize data lineage in databases, 
data warehouses and data lakes in AWS and GCP.


# Features
* Generate lineage from SQL query history.
* Supports ANSI SQL queries
* Integrate with Jupyter Notebook
* Visualize data lineage using Plotly. 
* Select source or target table.
* Pan, Zoom, Select graph

Checkout an [example data lineage notebook](http://tokern.io/docs/data-lineage/example/).

## Use Cases

Data Lineage enables the following use cases:

* Business Rules Verification
* Change Impact Analysis
* Data Quality Verification

Check out [the post on using data lineage for cost control](https://tokern.io/blog/data-lineage-on-redshift/) for an 
example of how data lineage can be used in production.

## Quick Start
```shell script
# Install packages
pip install data-lineage
pip install jupyter

jupyter notebook

# Checkout example notebook: http://tokern.io/docs/data-lineage/example/ 

```

## Supported Technologies

* Postgres

### Coming Soon

* MySQL
* AWS Redshift
* SparkSQL
* Presto

## Developer Setup
```shell script
# Install dependencies
pipenv install --dev

# Setup pre-commit and pre-push hooks
pipenv run pre-commit install -t pre-commit
pipenv run pre-commit install -t pre-push
```

