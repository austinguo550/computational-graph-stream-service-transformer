# KAFKA DATASOURCE NODE

FROM kafkabaseimage:1.0

ARG name

# set working directory
WORKDIR /usr/src/app

# TODO dynamically build docker images from here and below
# pass the correct pickle file
COPY ./sysfiles/${name} .
COPY ./log1.txt .
COPY ./log2.txt .
COPY ./log3.txt .

# copy over the code contents to run the node
COPY kafka-datasourcenode.py .

# run the program
ENTRYPOINT [ "python3", "kafka-datasourcenode.py" ]

# build docker image
# docker build --no-cache --build-arg name=start_node1 -t kafka-datasourcenode_start_node1:1.0 .

# run docker container
# docker container run kafka-datasourcenode_start_node1:1.0 --name start_node1 --input_file log1.txt --broker_port_start 9092 --num_brokers 1