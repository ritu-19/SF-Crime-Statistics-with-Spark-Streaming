import pykafka


if __name__ == "__main__":

	client = pykafka.KafkaClient(hosts="127.0.0.1:9092")
	print("topics", client.topics)
	topic= client.topics[b'demo']
	consumer = topic.get_simple_consumer()
	for message in consumer:
		if message is not None:
			print (message.offset, message.value)
