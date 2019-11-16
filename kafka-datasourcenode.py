from confluent_kafka import Producer
import argparse
import os
import dill

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def main():
    print("Creating Kafka DataSource Node")

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--name', type=str, required=True)
    parser.add_argument('--input_file', type=str, required=True)
    parser.add_argument('--broker_ip_start', type=int, default=9092, required=False)
    parser.add_argument('--num_brokers', type=int, default=1, required=False)
    parsed_args = parser.parse_args()

    # Get the node's name, the topic name the datasource 
    # node will write to is the same as its name
    node_name = parsed_args.name
    outgoing_topic = node_name

    # Get the processing function if the node has one
    processing_function = None 
    if os.path.exists(os.getcwd() + "/sysfiles/{}.pkl".format(node_name)):
        processing_function = dill.load(open("./sysfiles/{}.dill".format(node_name), "rb"))
    
    # Get the input file the datasource node will read from
    input_file = parsed_args.input_file

    # If there are multiple brokers available, the 
    # datasource node will connect to them all
    broker_ip_start = parsed_args.broker_ip_start
    num_brokers = parsed_args.num_brokers
    bootstrap_server_str = "localhost:{}".format(broker_ip_start)
    for i in range(1, num_brokers):
        bootstrap_server_str += ",localhost:{}".format(broker_ip_start + i)
    p = Producer({'bootstrap.servers': bootstrap_server_str})

    # Read from the specified input file and write contents out to topic
    while True:
        with open(input_file) as f:
            for line in f:
                processed_line = line.rstrip() if processing_function == None else str(processing_function(line.rstrip()))
                p.produce(outgoing_topic, processed_line.encode('utf-8'), callback=delivery_report)
                p.flush()
        break

if __name__ == "__main__":
    main()