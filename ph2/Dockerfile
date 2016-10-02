FROM ubuntu

RUN apt-get update
RUN apt-get install -y python-pip && pip install boto
RUN mkdir /install
ADD dist/ph2-0.1.tar.gz /install/
RUN cd /install/ph2-0.1 && python setup.py install
