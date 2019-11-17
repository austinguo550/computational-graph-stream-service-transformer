from typing import Type, Any, Optional, List, Tuple, Callable
from collections import defaultdict
import subprocess
import os
import fileinput
import re
import dill
import signal
import time
import psutil

CURRENT_WORKING_DIRECTORY = os.getcwd()
KAFKA_FOLDERNAME = "kafka_2.12-2.3.0"
KAFKA_DIRECTORY = CURRENT_WORKING_DIRECTORY + "/" + KAFKA_FOLDERNAME
START_PORT = 9092

class GracefulKiller:
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.live_processes = []
    
    def add_process(self, process):
        self.live_processes.append(process)

    def exit_gracefully(self, signum, frame):
        subprocess.call(["make", "shutdown"], cwd=CURRENT_WORKING_DIRECTORY)
        subprocess.call(["make", "clean"], cwd=CURRENT_WORKING_DIRECTORY)
        for process in self.live_processes:
            pid = process.pid
            if psutil.pid_exists(pid):
                os.kill(pid, signal.SIGINT)
        exit()

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
    def __init__(self, name: str, processing_function: Callable, output_file_name: str):
        ComputationalGraphNode.__init__(self, name=name, processing_function=processing_function)
        self.output_file_name = output_file_name
    
    def get_output_file_name(self):
        return self.output_file_name

class ComputationalGraph:
    def __init__(self, nodes: List[Type[ComputationalGraphNode]]):
        # Check to see if node names given are all unique
        node_names = {node.get_name() for node in nodes}
        if len(node_names) != len(nodes):
            print("ERROR - All node names must be unique")
            exit()

        self.nodes = nodes
        self.stream_writer_subscribers = defaultdict(set)
        self.stream_consumer_subscription = defaultdict(set)
        self.graceful_killer = GracefulKiller()
    
    def connect(self, edges: List[Tuple[Type[ComputationalGraphNode], Type[ComputationalGraphNode]]]):
        for from_node, to_node in edges:
            self.stream_writer_subscribers[from_node].add(to_node)
            self.stream_consumer_subscription[to_node].add(from_node)

    # Returns a list of consumer nodes that the writer node of interest is writing to
    def get_writer_subscribers(self, writer: Type[ComputationalGraphNode]):
        return self.stream_writer_subscribers[writer]
    
    # Returns a list of writer nodes that the consumer node of interest is subscribed to
    def get_consumer_subscriptions(self, consumer: Type[ComputationalGraphNode]):
        return self.stream_consumer_subscription[consumer]

    def generate_kafka_env(self, num_brokers=1, num_topic_partitions=1, num_partition_replicas=1) -> List[Any]:

        # Start zookeeper
        subprocess.Popen(["bin/zookeeper-server-start.sh", "config/zookeeper.properties"], cwd=KAFKA_DIRECTORY)

        # Start brokers
        default_server_properties = None
        with open(KAFKA_DIRECTORY + "/config/" + "server.properties", "r") as f:
            default_server_properties = f.readlines()

        for i in range(0, num_brokers):
            new_broker_config_filename = "server-{}.properties".format(i)
            regex_broker_id = re.compile("broker.id")
            regex_ip_and_port = re.compile("listeners=PLAINTEXT")
            regex_log_dir = re.compile("log.dirs")

            with open(KAFKA_DIRECTORY + "/config/" + new_broker_config_filename, "w") as f:
                for line in default_server_properties:
                    broker_id_match = regex_broker_id.match(line)
                    ip_and_port_match = regex_ip_and_port.match(line)
                    log_dir_match = regex_log_dir.match(line)
                    if broker_id_match:
                        f.write("broker.id={}\n".format(i))
                    elif ip_and_port_match:
                        f.write("listeners=PLAINTEXT://:{}\n".format(START_PORT + i))
                    elif log_dir_match:
                        f.write("log.dirs=/tmp/kafka-logs-{}".format(i))
                    else:
                        f.write(line)
        
            subprocess.Popen(["bin/kafka-server-start.sh", "config/" + new_broker_config_filename], cwd=KAFKA_DIRECTORY)

        # Create Kafka topics
        topics = [node.get_name() for node in self.stream_writer_subscribers.keys()]
        for topic in topics:
            subprocess.call(
                [
                    "bin/kafka-topics.sh",
                    "--create",
                    "--bootstrap-server", 
                    "localhost:" + str(START_PORT),
                    "--replication-factor", str(num_partition_replicas),
                    "--partitions", str(num_topic_partitions),
                    "--topic", topic
                ],
                cwd=KAFKA_DIRECTORY
            )

        print("Initial setup done - Zookeeper, Broker(s), and Topic(s) created")

        # Create "sysfiles" directory to store pickled stuff
        sysfiles_dir_path = CURRENT_WORKING_DIRECTORY + "/sysfiles"
        if not os.path.isdir(sysfiles_dir_path):
            os.mkdir(sysfiles_dir_path)

        # Pickle all processing functions and start up node instances
        for node in self.nodes:
            node_name = node.get_name()
            processing_function = node.get_processing_function()
            if processing_function != None:
                with open("./sysfiles/{}.dill".format(node.get_name()), "wb") as dill_file:
                    dill.dump(processing_function, dill_file)
            
            if isinstance(node, DataSourceNode):
                print("Starting up DataSourceNode {}".format(node_name))
                data_source = node.get_data_source()
                subprocess.Popen(["python3", "kafka-datasourcenode.py", "--name", node_name, "--input_file", data_source,
                "--broker_port_start", str(START_PORT), "--num_brokers", str(num_brokers)], cwd=CURRENT_WORKING_DIRECTORY)
            
            if isinstance(node, IntermediateNode):
                print("Starting up IntermediateNode {}".format(node_name))
                subscription_str = ",".join([subscription.get_name() for subscription in self.get_consumer_subscriptions(node)])
                subprocess.Popen(["python3", "kafka-intermediatenode.py", "--name", node_name, "--topic_subscriptions", 
                subscription_str, "--broker_port_start", str(START_PORT), "--num_brokers", str(num_brokers)], 
                cwd=CURRENT_WORKING_DIRECTORY)

            if isinstance(node, TerminalNode):
                print("Starting up TerminalNode {}".format(node_name))
                output_file = node.get_output_file_name()
                subscription_str = ",".join([subscription.get_name() for subscription in self.get_consumer_subscriptions(node)])
                subprocess.Popen(["python3", "kafka-terminalnode.py", "--name", node_name, "--topic_subscriptions", 
                subscription_str, "--broker_port_start", str(START_PORT), "--num_brokers", str(num_brokers),
                "--output_file", output_file], cwd=CURRENT_WORKING_DIRECTORY)
        
        while(True):
            pass
            