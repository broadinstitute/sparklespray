FROM ubuntu

RUN apt-get update
RUN apt-get install -y python-pip && pip install boto
RUN mkdir /install
ADD setup.py /install/
ADD ph2/ /install/ph2/
RUN cd /install && python /install/setup.py install
