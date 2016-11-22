FROM ubuntu

RUN apt-get update
RUN apt-get install -y python3-pip && pip3 install gcloud attrs
RUN mkdir /install
ADD kubeque-0.1.tar.gz /install/
RUN cd /install/kubeque-0.1 && python3 setup.py install
