#!/usr/bin/env python

# need python3 or gcloud stores bad values into datastore
import sys
assert sys.version_info >= (3,)

from setuptools import setup, find_packages

setup(name='kubeque',
      version='0.1',
      description='batch job submission front to kubernettes',
      author='Philip Montgomery',
      author_email='pmontgom@broadinstitute.org',
      install_requires=['gcloud', 'pykube', 'attrs'],
      packages=find_packages(),
      entry_points={'console_scripts': [
        "kubeque = kubeque.main:main",
        "kubeque-consume = kubeque.consume:main",
        "kubeque-reaper = kubeque.reaper:main"
        ]}
     )
