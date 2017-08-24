FROM ubuntu

RUN echo 5 && apt-get update
RUN apt-get install -y python3-pip && pip3 install attrs
RUN mkdir /install
ADD kubeque-0.2.2.tar.gz /install/
RUN cd /install/kubeque-0.2.2 && python3 setup.py install
