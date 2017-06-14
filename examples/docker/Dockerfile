FROM ubuntu

RUN apt-get update
RUN apt-get install -y python3-pip && pip3 install google-cloud attrs
RUN mkdir /install
ADD kubeque-0.2.tar.gz /install/
RUN cd /install/kubeque-0.2 && python3 setup.py install
