# A shippable 'virtual sensor' for the iQAS platform
# VERSION: 1.0

FROM ubuntu:16.04
MAINTAINER Antoine Auger <antoine.auger@isae.fr>

# To pass in arguments rather than here
ENV http_proxy http://proxy.isae.fr:3128
ENV https_proxy http://proxy.isae.fr:3128
ENV no_proxy isae.fr

# create user
RUN groupadd web
RUN useradd -d /home/bottle -m bottle

# make sure sources are up to date
RUN echo "deb http://archive.ubuntu.com/ubuntu/ xenial main restricted universe multiverse" > /etc/apt/sources.list

RUN apt-get update && apt-get install -y \ 
	net-tools \
	python3-pip
RUN apt-get upgrade -y

# install python 3 and pip3
RUN pip3 install --upgrade pip
RUN pip3 install bottle httplib2
RUN pip3 install kafka-python

# copy directories
COPY . /home/bottle/virtualSensor
ARG obsFile=data/raw_observations.txt
COPY $obsFile /home/bottle/virtualSensor/data/raw_observations.txt

# in case you'd prefer to use links, expose the port
WORKDIR /home/bottle/virtualSensor/src
EXPOSE 8080
ENTRYPOINT ["/usr/bin/python3", "/home/bottle/virtualSensor/src/main.py", "sensor01", "KAFKA", "test_python"]
USER bottle
