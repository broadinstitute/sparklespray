#!/usr/bin/env python

# need python3 or gcloud stores bad values into datastore
import sys
assert sys.version_info >= (3, 5), "This package requires at least python 3.5"
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
          'termcolor==1.1.0',
          'attrs==17.2.0',
          'googleapis-common-protos==1.5.3',
          'google-cloud-datastore==1.7.3',
          'google-cloud-storage==1.13.2',
          'grpcio-tools==1.13.0',
          'pydantic==0.11.2',
          'google-api-python-client==1.7.4',
          'pyOpenSSL==18.0.0'
      ],
      packages=find_packages(),
      entry_points={'console_scripts': [
          "kubeque = sparklespray.main:sparkles_main",
          "sparkles = sparklespray.main:sparkles_main",
      ]}
      )
