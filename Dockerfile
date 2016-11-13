FROM ubuntu

RUN apt-get update
RUN apt-get install -y python-pip && pip install gcloud attr
RUN mkdir /install
ADD dist/kubeque-0.1.tar.gz /install/
RUN cd /install/kubeque-0.1 && python setup.py install
