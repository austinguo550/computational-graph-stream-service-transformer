from confluent_kafka import Producer

def main():
    print("Creating producer")
    p = Producer({'bootstrap.servers': 'localhost:9092'})

    some_data_source = ['1','2','3','4']

    counter = 0

    while(True):
        data = some_data_source[counter % 4]
        p.poll(0)
        p.produce('test', data.encode('utf-8'), callback=delivery_report)
        counter += 1



def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))



if __name__ == "__main__":
    main()