#!/usr/bin/env python

# need python3 or gcloud stores bad values into datastore
import sys
assert sys.version_info >= (3,5), "This package requires at least python 3.5"
import ast
import re
from setuptools import setup, find_packages

_version_re = re.compile(r'__version__\s*=\s*(.*)')

with open('sparklespray/__init__.py', 'rt') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read()).group(1)))

setup(name='sparklespray',
      version=version,
      description='batch job submission front to kubernettes',
      author='Philip Montgomery',
      author_email='pmontgom@broadinstitute.org',
      include_package_data=True,
      install_requires=[
#      'google-api-python-client==1.6.4',
      'google-cloud==0.31.0',
      'attrs'
          ],
      packages=find_packages(),
      entry_points={'console_scripts': [
        "kubeque = sparklespray.main:main",
        "sparkles = sparklespray.main:main",
        ]}
     )

