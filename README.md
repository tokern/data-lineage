[![CircleCI](https://circleci.com/gh/tokern/data-lineage.svg?style=svg)](https://circleci.com/gh/tokern/data-lineage)
[![codecov](https://codecov.io/gh/tokern/data-lineage/branch/master/graph/badge.svg)](https://codecov.io/gh/tokern/data-lineage)
[![PyPI](https://img.shields.io/pypi/v/data-lineage.svg)](https://pypi.python.org/pypi/data-lineage)
[![image](https://img.shields.io/pypi/l/data-lineage.svg)](https://pypi.org/project/data-lineage/)
[![image](https://img.shields.io/pypi/pyversions/data-lineage.svg)](https://pypi.org/project/data-lineage/)

# Data Lineage for Databases and Data Lakes

data-lineage is an open source application to query and visualize data lineage in databases, 
data warehouses and data lakes in AWS and GCP.

data-lineage's goal is to be _fast_, _simple setup_ and _allow analysis_ of the lineage. To achieve these goals, data lineage has the following features :

1. **Generate data lineage from query history.** Most databases maintain query history for a few days. Therefore the setup costs of an infrastructure to capture and store metadata is minimal. 
2. **Use networkx graph library to create a DAG of the lineage.** Networkx graphs provide programmatic access to data lineage providing rich opportunities to analyze data lineage.
3. **Integrate with Jupyter Notebooks.** Jupyter Notebooks provide an excellent IDE to generate, manipulate and analyze data lineage graphs. 
4. **Use Plotly to visualize the graph with rich annotations.** Plotly provides a number of features to provide rich graphs with tool tips, color coding and weights based on different attributes of the graph.

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
* AWS Redshift
* Snowflake

### Coming Soon

* MySQL
* SparkSQL
* Presto

## Documentation

For advanced usage, please refer to [data-lineage documentation](https://tokern.io/docs/data-lineage/index.html)
## Survey

Please take this [survey](https://forms.gle/p2oEQBJnpEguhrp3A) if you are a user or considering using data-lineage. Responses will help us prioritize features better. 

## Developer Setup
```shell script
# Install dependencies
pipenv install --dev

# Setup pre-commit and pre-push hooks
pipenv run pre-commit install -t pre-commit
pipenv run pre-commit install -t pre-push
```

