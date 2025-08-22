"""
setup.py configuration script describing how to build and package this project.

This file is primarily used by the setuptools library and typically should not
be executed directly. See README.md for how to deploy, test, and run
the tip_challenge project.
"""

from setuptools import setup, find_packages

import sys

sys.path.append("./src")

import datetime
import tip_challenge

local_version = datetime.datetime.utcnow().strftime("%Y%m%d.%H%M%S")

setup(
    name="tip_challenge",
    # We use timestamp as Local version identifier (https://peps.python.org/pep-0440/#local-version-identifiers.)
    # to ensure that changes to wheel package are picked up when used on all-purpose clusters
    version=tip_challenge.__version__ + "+" + local_version,
    url="https://databricks.com",
    author="swapnilmaddula@gmx.co.uk",
    description="wheel file based on tip_challenge/src",
    packages=find_packages(where="./src"),
    package_dir={"": "src"},
    entry_points={
        "packages": [
            "main=tip_challenge.main:main",
        ],
    },
    install_requires=[
        # Dependencies in case the output wheel file is used as a library dependency.
        # For defining dependencies, when this package is used in Databricks, see:
        # https://docs.databricks.com/dev-tools/bundles/library-dependencies.html
        "setuptools"
    ],
)
