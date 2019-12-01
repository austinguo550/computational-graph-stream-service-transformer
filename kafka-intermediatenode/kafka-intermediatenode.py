from confluent_kafka import Producer, Consumer
import argparse
import os
import pickle
import dill

def main():
    print("Creating Kafka Intermediate Node")

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--name', type=str, required=True)
    parser.add_argument('--topic_subscriptions', type=str, required=True)
    parser.add_argument('--broker_port_start', type=int, default=9092, required=False)
    parser.add_argument('--num_brokers', type=int, default=1, required=False)
    parsed_args = parser.parse_args()

    # Get the node's name, the topic name the intermediate 
    # node will write to is the same as its name
    node_name = parsed_args.name
    outgoing_topic = node_name

    # Get the topics that the intermediate node is subscribed to, remove all whitespaces
    topic_subscriptions = parsed_args.topic_subscriptions.split(',')

    # Get the processing function if the node has one
    processing_function = None 
    if os.path.exists(os.getcwd() + "/sysfiles/{}.dill".format(node_name)):
        processing_function = dill.load(open("./sysfiles/{}.dill".format(node_name), "rb"))

    # If there are multiple brokers available, the 
    # intermediate node will connect to them all
    broker_port_start = parsed_args.broker_port_start
    num_brokers = parsed_args.num_brokers
    localhost = "docker.for.mac.localhost"
    bootstrap_server_str = "{}:{}".format(localhost, broker_port_start)
    for i in range(1, num_brokers):
        bootstrap_server_str += ",{}:{}".format(localhost, broker_port_start + i)
    
    p = Producer({'bootstrap.servers': bootstrap_server_str})
    c = Consumer({
        'bootstrap.servers': bootstrap_server_str,
        'group.id': node_name,
        'auto.offset.reset': 'earliest'
    })
    c.subscribe(topic_subscriptions)

    # Read from the subscribed topics, process, and write results out to topic
    while True:
        # Consume an available message
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("{} encountered consumer error: {}".format(node_name, msg.error()))
            continue

        msg_value = msg.value().decode('utf-8')
        print('{} received message: {}'.format(node_name, msg_value))

        # Process the message and write result to outgoing topic/stream
        processed_msg = msg_value if processing_function == None else str(processing_function(msg_value))
        print("{} processed message: {}".format(node_name, processed_msg))

        def delivery_report(err, msg):
            """ Called once for each message produced to indicate delivery result.
                Triggered by poll() or flush(). """
            if err is not None:
                print('{} failed to deliver message: {}'.format(node_name, err))
            else:
                print('{} delivered message to {} [{}]'.format(node_name, msg.topic(), msg.partition()))

        p.produce(outgoing_topic, processed_msg.encode('utf-8'), callback=delivery_report)
        p.flush()

if __name__ == "__main__":
    main()