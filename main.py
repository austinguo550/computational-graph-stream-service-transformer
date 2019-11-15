from typing import Type, Any, Optional, List, Tuple, Callable
from computational_graph_lib import DataSourceNode, IntermediateNode, TerminalNode, ComputationalGraph

# def start_kafka_zookeeper(zookeeper_properties_file_location: str):
#     subprocess.Popen(["bin/zookeeper-server-start.sh", zookeeper_properties_file_location], cwd=KAFKA_DIRECTORY)


# def start_kafka_server(server_properties_file_location: str):
#     subprocess.Popen(["bin/kafka-server-start.sh", server_properties_file_location], cwd=KAFKA_DIRECTORY)


# def create_kafka_topic(topic_name: str, port: int, replication_factor: int, partition_count: int):
#     subprocess.call(
#         [
#             "bin/kafka-topics.sh",
#             "--create",
#             "--bootstrap-server", 
#             "localhost:" + str(port),
#             "--replication-factor " + str(replication_factor),
#             "--partitions " + str(partition_count),
#             "--topic " + topic_name
#         ],
#         cwd=KAFKA_DIRECTORY
#     )

def data_source_proc_func(msg: str):
    return int(msg)

def add3(msg: int):
    return msg + 3

def subtract4(msg: int):
    return msg - 4

def add6(msg: int):
    return msg + 6

def multiply5(msg: int):
    return msg * 5

def subtract10(msg: int):
    return msg - 10

def add8(msg: int):
    return msg + 8

def add100(msg: int):
    return msg + 100

def create_comp_graph():
    # create test computational graph nodes
    start_node1 = DataSourceNode(name="start_node1", processing_function=data_source_proc_func, data_source='log1.txt')
    start_node2 = DataSourceNode(name="start_node2", processing_function=data_source_proc_func, data_source='log2.txt')
    start_node3 = DataSourceNode(name="start_node3", processing_function=data_source_proc_func, data_source='log3.txt')

    inter_node1 = IntermediateNode(name="inter_node1",processing_function=add3)
    inter_node2 = IntermediateNode(name="inter_node2",processing_function=subtract4)
    inter_node3 = IntermediateNode(name="inter_node3",processing_function=add6)
    inter_node4 = IntermediateNode(name="inter_node4",processing_function=multiply5)
    inter_node5 = IntermediateNode(name="inter_node5",processing_function=subtract10)
    inter_node6 = IntermediateNode(name="inter_node6",processing_function=add8)
    inter_node7 = IntermediateNode(name="inter_node7",processing_function=add100)

    end_node1 = TerminalNode(name="end_node1", processing_function=None, output_file_name="comp_output.txt")

    # Create computational graph
    node_list = [start_node1, start_node2, start_node3, inter_node1, inter_node2, inter_node3, inter_node4,
                    inter_node5, inter_node6, inter_node7, end_node1]

    cg = ComputationalGraph(node_list)

    # Connect the edges cg.connect(from_node, to_node)
    cg.connect(start_node1, inter_node1)
    cg.connect(start_node2, inter_node1)
    cg.connect(start_node3, inter_node2)
    cg.connect(inter_node1, inter_node3)
    cg.connect(inter_node1, inter_node4)
    cg.connect(inter_node2, inter_node4)
    cg.connect(inter_node3, inter_node5)
    cg.connect(inter_node4, inter_node6)
    cg.connect(inter_node5, inter_node7)
    cg.connect(inter_node6, inter_node7)
    cg.connect(inter_node7, end_node1)

    cg.generate_kafka_env()
    print("done")

    # generate new kafka nodes
    # kafka_nodes = cg.generate_kafka_env()


def main():
    # create_kafka_topic("test", 9092, 1, 1)
    create_comp_graph()

if __name__ == "__main__":
    main()