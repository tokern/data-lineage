import ast
import re
import sys
from os import getenv, path

from setuptools import find_packages, setup
from setuptools.command.install import install

_version_re = re.compile(r"__version__\s*=\s*(.*)")

with open("data_lineage/__init__.py", "rb") as f:
    __version__ = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )


class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""

    description = "verify that the git tag matches our version"

    def run(self):
        tag = getenv("CIRCLE_TAG")

        if tag != ("v%s" % __version__):
            info = "Git tag: {0} does not match the version of this app: {1}".format(
                tag, __version__
            )
            sys.exit(info)


here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="data-lineage",
    version=__version__,
    packages=find_packages(exclude=["docs", "test*"]),
    url="https://tokern.io/lineage",
    license="MIT",
    author="Tokern",
    author_email="info@tokern.io",
    description="Open Source Data Lineage Tool For AWS and GCP",
    long_description=long_description,
    long_description_content_type="text/markdown",
    download_url="https://github.com/tokern/data-lineage/tarball/" + __version__,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Database",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords="data-lineage databases postgres snowflake graphs plotly",
    install_requires=[
        "brotli==1.0.9",
        "click==7.1.2",
        "dash==1.17.0",
        "dash-core-components==1.13.0",
        "dash-html-components==1.1.1",
        "dash-renderer==1.8.3",
        "dash-table==4.11.0",
        "decorator==4.4.2",
        "flask==1.1.2",
        "flask-compress==1.8.0",
        "future==0.18.2",
        "inflection==0.5.1",
        "itsdangerous==1.1.0",
        "jinja2==2.11.2",
        "markupsafe==1.1.1",
        "networkx==2.5",
        "pglast==1.14",
        "plotly==4.12.0",
        "pyyaml==5.3.1",
        "retrying==1.3.3",
        "six==1.15.0",
        "werkzeug==1.0.1",
    ],
    extra_requires=[],
    dependency_links=[],
    cmdclass={"verify": VerifyVersionCommand,},
    entry_points={"console_scripts": ["data_lineage = data_lineage.__main__:main"]},
    include_package_data=True,
)
