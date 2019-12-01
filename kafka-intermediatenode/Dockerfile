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

# run the program
ENTRYPOINT [ "python3", "kafka-intermediatenode.py" ]

# build docker image
# docker build --no-cache --build-arg name=inter_node1 -t kafka-intermediatenode_inter_node1:1.0 .

# run docker container
# docker container run kafkaintermediatenode_inter_node1:1.0 --name intern_node1 --topic_subscriptions hello1,hello2,hello3 --broker_port_start 9092 --num_brokers 1