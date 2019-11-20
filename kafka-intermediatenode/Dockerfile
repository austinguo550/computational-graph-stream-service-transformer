# KAFKA INTERMEDIATE NODE

FROM kafkabaseimage:1.0

# set working directory
WORKDIR /usr/src/app

# TODO dynamically build docker images from here and below
# pass the correct pickle file
ARG name
COPY ./sysfiles/${name} .

# copy over the code contents to run the node
COPY kafka-intermediatenode.py .

ARG topic_subscriptions
# TODO remove this later
ARG broker_port_start
ARG num_brokers

# run the program
ENTRYPOINT [ "python3", "kafka-intermediatenode.py" ]
CMD ["--name", name, "--topic_subscriptions", topic_subscriptions,\
    "--broker_port_start", broker_port_start, "--num_brokers", num_brokers]

# build docker image
# docker build --no-cache --build-arg name=inter_node1 --build-arg topic_subscriptions=hello1,hello2,hello3 --build-arg broker_port_start=9092 --build-arg num_brokers=1 -t kafkaintermediatenode:1.0 .