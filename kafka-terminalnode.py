from confluent_kafka import Producer, Consumer
import argparse
import os
import pickle

def main():
    print("Creating Kafka Terminal Node")

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--name', type=str, required=True)
    parser.add_argument('--topic_subscriptions', type=str, required=True)
    parser.add_argument('--output_file', type=str, required=True)
    parser.add_argument('--broker_ip_start', type=int, default=9092, required=False)
    parser.add_argument('--num_brokers', type=int, default=1, required=False)
    parsed_args = parser.parse_args()

    # Get the node's name, the topic name the Terminal 
    # node will write to is the same as its name
    node_name = parsed_args.name

    # Get the topics that the Terminal node is subscribed to, remove all whitespaces
    topic_subscriptions = parsed_args.topic_subscriptions.split(',')

    # Get the processing function if the node has one
    processing_function = None 
    if os.path.exists(os.getcwd() + "/sysfiles/{}.pkl".format(node_name)):
        processing_function = pickle.load(open("./sysfiles/{}.dill".format(node_name), "rb"))

    # Get the input file the datasource node will read from
    output_file = parsed_args.output_file

    # If there are multiple brokers available, the 
    # Terminal node will connect to them all
    broker_ip_start = parsed_args.broker_ip_start
    num_brokers = parsed_args.num_brokers
    bootstrap_server_str = "localhost:{}".format(broker_ip_start)
    for i in range(1, num_brokers):
        bootstrap_server_str += ",localhost:{}".format(broker_ip_start + i)

    c = Consumer({
        'bootstrap.servers': bootstrap_server_str,
        'group.id': node_name,
        'default.topic.config': {'auto.offset.reset': 'latest'}
    })
    c.subscribe(topic_subscriptions)

    # Read from the subscribed topics, process, and write results out to output file
    with open(output_file, "a") as file:
        while True:
            # Consume an available message
            msg = c.poll(0.0)
            if msg is None:
                continue
            elif msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            else:
                msg_value = msg.value().decode('utf-8')
                print('Received message: {}'.format(msg_value))

                # Process the message and write result to outgoing topic/stream
                processed_msg = msg_value if processing_function == None else str(processing_function(msg_value))
                file.write("{}\n".format(processed_msg))
            
if __name__ == "__main__":
    main()