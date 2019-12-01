# specify image type https://hub.docker.com/
FROM ubuntu:latest

# set working directory
WORKDIR /usr/src/app

# install system requirements
RUN apt-get update 
RUN apt-get install -y python3.7
RUN apt-get install -y python3-pip
RUN apt-get install -y curl
RUN curl -O http://apache.mirrors.pair.com/kafka/2.3.0/kafka_2.12-2.3.0.tgz

# pass over python dependencies and install
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# extract kafka directory from tgz
RUN tar -xzf kafka_2.12-2.3.0.tgz

# docker build --no-cache -t kafkabaseimage:1.0 .