run:
	python3 main.py

test-producer:
	python3 producer.py

test-consumer:
	python3 consumer.py

check:
	mypy main.py
