import ast
import re
import sys
from os import getenv, path

from setuptools import setup
from setuptools.command.install import install

_version_re = re.compile(r'__version__\s*=\s*(.*)')

with open('data_lineage/__init__.py', 'rb') as f:
    __version__ = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))


class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""
    description = 'verify that the git tag matches our version'

    def run(self):
        tag = getenv('CIRCLE_TAG')

        if tag != ("v%s" % __version__):
            info = "Git tag: {0} does not match the version of this app: {1}".format(
                tag, __version__
            )
            sys.exit(info)


here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='data-lineage',
    version=__version__,
    packages=['test', 'data_lineage'],
    url='https://tokern.io/lineage',
    license='MIT',
    author='Tokern',
    author_email='info@tokern.io',
    description='Open Source Data Lineage Tool For AWS and GCP',
    cmdclass={
        'verify': VerifyVersionCommand,
    }
)
