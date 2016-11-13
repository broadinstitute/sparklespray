#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='kubeque',
      version='0.1',
      description='batch job submission front to kubernettes',
      author='Philip Montgomery',
      author_email='pmontgom@broadinstitute.org',
      install_requires=['gcloud'],
      packages=find_packages(),
      entry_points={'console_scripts': [
        "kubeque = kubeque.main:main"
        ]}
     )
