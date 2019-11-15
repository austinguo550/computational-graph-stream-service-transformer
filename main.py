from typing import Type, Any, Optional, List, Tuple, Callable
from computational_graph_lib import DataSourceNode, IntermediateNode, TerminalNode, ComputationalGraph

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

    inter_node1 = IntermediateNode(name="inter_node1", processing_function=add3)
    inter_node2 = IntermediateNode(name="inter_node2", processing_function=subtract4)
    inter_node3 = IntermediateNode(name="inter_node3", processing_function=add6)
    inter_node4 = IntermediateNode(name="inter_node4", processing_function=multiply5)
    inter_node5 = IntermediateNode(name="inter_node5", processing_function=subtract10)
    inter_node6 = IntermediateNode(name="inter_node6", processing_function=add8)
    inter_node7 = IntermediateNode(name="inter_node7", processing_function=add100)

    end_node1 = TerminalNode(name="end_node1", processing_function=None, output_file_name="comp_output.txt")

    # Create computational graph
    node_list = [start_node1, start_node2, start_node3, inter_node1, inter_node2, inter_node3, inter_node4,
                    inter_node5, inter_node6, inter_node7, end_node1]

    cg = ComputationalGraph(node_list)

    # Connect the edges cg.connect(from_node, to_node)
    edge_list = [(start_node1, inter_node1), (start_node2, inter_node1), (start_node3, inter_node2), (inter_node1, inter_node3),
                (inter_node1, inter_node4), (inter_node2, inter_node4), (inter_node3, inter_node5), (inter_node4, inter_node6),
                (inter_node5, inter_node7), (inter_node6, inter_node7), (inter_node7, end_node1)] 
                
    cg.connect(edge_list)

    cg.generate_kafka_env()

def main():
    create_comp_graph()

if __name__ == "__main__":
    main()