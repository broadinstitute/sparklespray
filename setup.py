#!/usr/bin/env python

# need python3 or gcloud stores bad values into datastore
import sys
assert sys.version_info >= (3,)

from setuptools import setup, find_packages

setup(name='kubeque',
      version='0.2.1',
      description='batch job submission front to kubernettes',
      author='Philip Montgomery',
      author_email='pmontgom@broadinstitute.org',
      install_requires=['google-cloud-storage',
          'google-cloud-datastore',
          'google-cloud-pubsub',
          'pykube', 'attrs',
          "oauth2client==3.0.0" # if not specified pip will install v4+ which doesn't seem to be compatible with google-cloud
          ],
      packages=find_packages(),
      entry_points={'console_scripts': [
        "kubeque = kubeque.main:main",
        "kubeque-consume = kubeque.consume:main",
        "kubeque-reaper = kubeque.reaper:main"
        ]}
     )
