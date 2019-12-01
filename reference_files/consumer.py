from confluent_kafka import Consumer, KafkaError, Producer

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

p = Producer({'bootstrap.servers': 'localhost:9092'})

def write_to_stream(data):
    p.poll(0)
    p.produce('output', data.encode('utf-8'))

c.subscribe(['test'])

received_messages_count = 0

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))
    received_messages_count += 1

    if received_messages_count % 100 == 0:
        write_to_stream("MESSAGE COUNT: " + str(received_messages_count))


c.close()