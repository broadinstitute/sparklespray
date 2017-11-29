#!/usr/bin/env python

# need python3 or gcloud stores bad values into datastore
import sys
assert sys.version_info >= (3,5), "This package requires at least python 3.5"
import ast
import re
from setuptools import setup, find_packages

_version_re = re.compile(r'__version__\s*=\s*(.*)')

with open('kubeque/__init__.py', 'rt') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read()).group(1)))

setup(name='kubeque',
      version=version,
      description='batch job submission front to kubernettes',
      author='Philip Montgomery',
      author_email='pmontgom@broadinstitute.org',
      include_package_data=True,
      install_requires=[
          ],
      packages=find_packages(),
      entry_points={'console_scripts': [
        "kubeque = kubeque.main:main",
        "sparkles = kubeque.main:main",
        ]}
     )

