# KAFKA TERMINAL NODE

FROM kafkabaseimage:1.0

# set working directory
WORKDIR /usr/src/app

# TODO dynamically build docker images from here and below
# pass the correct pickle file
ARG name
COPY ./sysfiles/${name} .

# copy over the code contents to run the node
COPY kafka-terminalnode.py .

# run the program
ENTRYPOINT [ "python3", "kafka-terminalnode.py" ]

# build docker image
# docker build --no-cache --build-arg name=end_node1 -t kafka-terminalnode_end_node1:1.0 .

# run docker container
# docker container run kafka-terminalnode_end_node1:1.0 --name end_node1 --topic_subscriptions hello1,hello2,hello3 --broker_port_start 9092 --num_brokers 1 --output_file "comp_output-docker.txt" 