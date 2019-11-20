# KAFKA INTERMEDIATE NODE
# TODO build base image with python/pip install and copied kafka shit

# specify image type https://hub.docker.com/
FROM ubuntu:latest

# set working directory
WORKDIR /usr/src/app

# install system requirements
RUN apt-get update 
RUN apt-get install -y python3.7
RUN apt-get install -y python3-pip 

# pass over python dependencies and install
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# either copy over kafka library 
COPY kafka_2.12-2.3.0.tgz .
RUN tar -xzf kafka_2.12-2.3.0.tgz

# TODO dynamically build docker images from here and below
# pass the correct pickle file
ARG node_name
COPY ./sysfiles/${node_name}.dill .

# copy over the code contents to run the node
COPY kafka-intermediatenode.py .
COPY Makefile . 

ARG topic_subscriptions
# TODO remove this later
ARG broker_port_start
ARG num_brokers

# run the program
ENTRYPOINT [ "python3", "kafka-intermediatenode.py" ]
CMD ["--name", node_name, "--topic_subscriptions", topic_subscriptions,\
    "--broker_port_start", broker_port_start, "--num_brokers", num_brokers]

# build docker image
# docker build --build-arg node_name=testnode --build-arg topic_subscriptions=hello1,hello2,hello3 --build-arg broker_port_start=9092 --build-arg num_brokers=1 -t kafkaintermediatenode:1.0 .