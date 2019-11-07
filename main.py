from typing import Type, Any, Optional, List, Tuple, Callable

    
def main():
    test()


def test():
    # create test computational graph nodes
    start_node = ComputationalGraphNode("start_node")
    node_1 = ComputationalGraphNode("node_1", filter_for_1)
    node_1a = ComputationalGraphNode("node_1a", print_stream)
    node_1b = ComputationalGraphNode("node_1b", count)
    node_2 = ComputationalGraphNode("node_2", filter_for_2)
    node_2a = ComputationalGraphNode("node_2a", count)
    node_3 = ComputationalGraphNode("node_3", count)
    node_overseer = ComputationalGraphNode("overseer", count)
    
    # computational graph components
    cg_node_list = [start_node, node_1, node_1a, node_1b, node_2, node_2a, node_3, node_overseer]
    cg_edge_list = [(0, 1), (1, 2), (1, 3), (0, 4), (4, 5), (0, 6), (1, 7), (4, 7), (6, 7), (7, 0)]

    # create computational graph
    cg = ComputationalGraph(cg_node_list, cg_edge_list)

    # generate new kafka nodes
    kafka_nodes = cg.generate_kafka_env()


def filter_for_1(input_data: str) -> Optional[str]:
    """Filters the input stream for 1s and passes on the message if so"""
    return input_data if "1" in input_data else None


def filter_for_2(input_data: str) -> Optional[str]:
    """Filters the input stream for 2s and passes on the message if so"""
    return input_data if "2" in input_data else None


def print_stream(input_data: str) -> Optional[str]:
    print(input_data)
    return None

def count(input_data: str) -> Optional[str]:
    print("TODO count")
    return None



class ComputationalGraphNode:
    def __init__(self, name: str, processing_function: Callable[[str], Optional[str]] = None):
        self.name = name
        self.processing_function = processing_function
    
    def get_name(self):
        return self.name
    
    def get_processing_function(self):
        return self.processing_function
    


class ComputationalGraph:
    def __init__(self, nodes: List[Type[ComputationalGraphNode]], edges: List[Tuple[int, int]]):
        self.nodes = nodes
        self.edges = edges
    
    def generate_kafka_env(self) -> List[Any]:
        result_kafka_nodes = []
        subscription_lookup = {}
        producers_index_list = set([])
        kafka_streams = {}

        # create lookup table for incoming edges since kafka nodes need to know
        # what to subscribe to
        for producer_node_index, consumer_node_index in self.edges:
            if consumer_node_index not in subscription_lookup:
                subscription_lookup[consumer_node_index] = [] 

            subscription_lookup[consumer_node_index].append(producer_node_index)
            producers_index_list.add(producer_node_index)

        # create kafka streams for producers
        for producer_index in producers_index_list:
            new_stream_name = self.nodes[producer_index].get_name()
            new_stream = "TODO" + new_stream_name
            print("TODO Created new stream: ", new_stream_name)
            kafka_streams[producer_index] = new_stream

        # create kafka nodes where we can pass in list of streams which it can subscribe to
        for i in range(len(self.nodes)):
            current_node = self.nodes[i]

            current_node_subscribed_streams = []
            if i in subscription_lookup: 
                current_node_subscribed_streams = list(map(
                    lambda producer_index: kafka_streams[producer_index], 
                    subscription_lookup[i]
                ))
            
            new_kafka_node = KafkaNode(
                current_node.get_name(),
                current_node_subscribed_streams,
                current_node.get_processing_function()
            )

            result_kafka_nodes.append(new_kafka_node)
            
        # return list of created nodes
        return result_kafka_nodes
            

        

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



if __name__ == "__main__":
    main()