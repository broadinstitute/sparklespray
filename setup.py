#!/usr/bin/env python

# need python3 or gcloud stores bad values into datastore
import sys
assert sys.version_info >= (3,)
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
      install_requires=[
        'requests==2.17.3',
        'requests-oauthlib==0.8.0',
        'protobuf==3.3.0',
        'oauth2client==3.0.0',
        'oauthlib==2.0.2',
        'google-auth==1.0.1',
        'google-auth-httplib2==0.0.2',
        'google-cloud==0.25.0',
        'google-cloud-bigquery==0.24.0',
        'google-cloud-bigtable==0.24.0',
        'google-cloud-core==0.24.1',
        'google-cloud-datastore==1.0.0',
        'google-cloud-dns==0.24.0',
        'google-cloud-error-reporting==0.24.2',
        'google-cloud-language==0.24.1',
        'google-cloud-logging==1.0.0',
        'google-cloud-monitoring==0.24.0',
        'google-cloud-pubsub==0.25.0',
        'google-cloud-resource-manager==0.24.0',
        'google-cloud-runtimeconfig==0.24.0',
        'google-cloud-spanner==0.24.2',
        'google-cloud-speech==0.24.0',
        'google-cloud-storage==1.1.1',
        'google-cloud-translate==0.24.0',
        'google-cloud-vision==0.24.0',
        'google-gax==0.15.13',
        'google-resumable-media==0.0.2',
        'googleapis-common-protos==1.5.2',
        'grpcio==1.4.0rc1',
        'httplib2==0.10.3',
        'pyasn1==0.3.4'
          ],
      packages=find_packages(),
      entry_points={'console_scripts': [
        "kubeque = kubeque.main:main",
        ]}
     )

