# KAFKA DATASOURCE NODE

FROM kafkabaseimage:1.0

# set working directory
WORKDIR /usr/src/app

# TODO dynamically build docker images from here and below
# pass the correct pickle file
ARG name
COPY ./sysfiles/${name} .

# copy over the code contents to run the node
COPY kafka-datasourcenode.py .

ARG data_source
# TODO remove this later
ARG broker_port_start
ARG num_brokers

# run the program
ENTRYPOINT [ "python3", "kafka-datasourcenode.py" ]
CMD ["--name", name, "--input_file", data_source,\
    "--broker_port_start", broker_port_start, "--num_brokers", num_brokers]

# build docker image
# docker build --no-cache --build-arg name=start_node1 --build-arg data_source="log1.txt" --build-arg broker_port_start=9092 --build-arg num_brokers=1 -t kafkadatasourcenode:1.0 .