from typing import Type, Any, Optional, List, Tuple, Callable
from collections import defaultdict
import subprocess
import os
import fileinput
import re

CURRENT_WORKING_DIRECTORY = os.getcwd()
KAFKA_FOLDERNAME = "kafka_2.11-2.3.1"
KAFKA_DIRECTORY = CURRENT_WORKING_DIRECTORY + "/" + KAFKA_FOLDERNAME

class ComputationalGraphNode:
    def __init__(self, name: str, processing_function: Callable = None):
        self.name = name
        self.processing_function = processing_function
    
    def get_name(self):
        return self.name
    
    def get_processing_function(self):
        return self.processing_function


class DataSourceNode(ComputationalGraphNode):
    def __init__(self, name: str, processing_function: Callable, data_source: str):
        ComputationalGraphNode.__init__(self, name=name, processing_function=processing_function)
        self.data_source = data_source
    
    def get_data_source(self):
        return self.data_source

class IntermediateNode(ComputationalGraphNode):
    def __init__(self, name: str, processing_function: Callable):
        ComputationalGraphNode.__init__(self, name=name, processing_function=processing_function)

class TerminalNode(ComputationalGraphNode):
    def __init__(self, name: str, processing_function, output_file_name: str):
        ComputationalGraphNode.__init__(self, name=name, processing_function=processing_function)
        self.output_file_name = output_file_name
    
    def get_output_file_name(self):
        return self.output_file_name


class ComputationalGraph:
    def __init__(self, nodes: List[Type[ComputationalGraphNode]]):
        self.nodes = nodes
        self.stream_writers = set()
        self.stream_consumer_subscription = defaultdict(set)
    
    def connect(self, from_node: Type[ComputationalGraphNode], to_node: Type[ComputationalGraphNode]):
        self.stream_writers.add(from_node)
        self.stream_consumer_subscription[to_node].add(from_node)

    def get_stream_writers(self):
        return self.stream_writers
    
    def get_consumer_subscriptions(self, node):
        return self.stream_consumer_subscription[node]


    def generate_kafka_env(self, num_brokers=1, num_topic_partitions=1, num_partition_replicas=1) -> List[Any]:

        # Start zookeeper
        subprocess.Popen(["bin/zookeeper-server-start.sh", "config/zookeeper.properties"], cwd=KAFKA_DIRECTORY)

        # Start brokers
        lines = None
        with open(KAFKA_DIRECTORY + "/config/" + 'server.properties', "r") as f:
            lines = f.readlines()

        start_port = 9092
        for i in range(0, num_brokers):
            new_broker_config_filename = "server-{}.properties".format(i)
            regex_broker_id = re.compile('broker.id')
            regex_ip_and_port = re.compile('listeners=PLAINTEXT')

            with open(KAFKA_DIRECTORY + "/config/" + new_broker_config_filename, "w") as f:
                for line in lines:
                    broker_id_match = regex_broker_id.match(line)
                    ip_and_port_match = regex_ip_and_port.match(line)
                    if broker_id_match:
                        f.write('broker.id={}\n'.format(i))
                    elif ip_and_port_match:
                        f.write('listeners=PLAINTEXT://:{}\n'.format(start_port + i))
                    else:
                        f.write(line)
        
            subprocess.Popen(["bin/kafka-server-start.sh", "config/" + new_broker_config_filename], cwd=KAFKA_DIRECTORY)

        kafka_topics = list(self.stream_writers)


        # create lookup table for incoming edges since kafka nodes need to know
        # what to subscribe to
        # for producer_node_index, consumer_node_index in self.edges:
        #     if consumer_node_index not in subscription_lookup:
        #         subscription_lookup[consumer_node_index] = [] 

        #     subscription_lookup[consumer_node_index].append(producer_node_index)
        #     producers_index_list.add(producer_node_index)

        # # create kafka streams for producers
        # for producer_index in producers_index_list:
        #     new_stream_name = self.nodes[producer_index].get_name()
        #     new_stream = "TODO" + new_stream_name
        #     print("TODO Created new stream: ", new_stream_name)
        #     kafka_streams[producer_index] = new_stream

        # # create kafka nodes where we can pass in list of streams which it can subscribe to
        # for i in range(len(self.nodes)):
        #     current_node = self.nodes[i]

        #     current_node_subscribed_streams = []
        #     if i in subscription_lookup: 
        #         current_node_subscribed_streams = list(map(
        #             lambda producer_index: kafka_streams[producer_index], 
        #             subscription_lookup[i]
        #         ))
            
        #     new_kafka_node = KafkaNode(
        #         current_node.get_name(),
        #         current_node_subscribed_streams,
        #         current_node.get_processing_function()
        #     )

        #     result_kafka_nodes.append(new_kafka_node)
            
        # # return list of created nodes
        # return result_kafka_nodes
    
class KafkaNode:
    # TODO on creating the node, subscribe to the streams (incoming connections)
    def __init__(self, name: str, incoming_connections: List[Any], processing_function: Callable[[Any], Any]):
        self.name = name
        self.incoming_connections = incoming_connections
        self.processing_function = processing_function
        print("TODO:", name, "node subscribed to all subscriptions", incoming_connections)
    

    def get_name(self):
        return self.name
    

    def get_incoming_connections(self):
        return self.incoming_connections


    def test_processing_function(self, incoming_data: Any):
        return self.processing_function(incoming_data)