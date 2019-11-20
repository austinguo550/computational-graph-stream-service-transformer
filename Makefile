run:
	python3 main.py

test-producer:
	python3 producer.py

test-consumer:
	python3 consumer.py

check:
	mypy main.py

clean:
	-rm -rf /tmp/kafka-* /tmp/zookeeper &> /dev/null
	-rm ./kafka_2.12-2.3.0/logs/* &> /dev/null
	-rm ./kafka_2.12-2.3.0/config/server-*.properties &> /dev/null
	-rm ./sysfiles/* &> /dev/null

shutdown:
	-./kafka_2.12-2.3.0/bin/zookeeper-server-stop.sh
	-./kafka_2.12-2.3.0/bin/kafka-server-stop.sh 