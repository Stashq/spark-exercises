from kafka import KafkaConsumer


if __name__ == '__main__':
    consumer = KafkaConsumer(
        bootstrap_servers='localhost:9092', group_id='first-group')
    # print(consumer.topics())
    consumer.subscribe(topics=['timeseries-topic'])
    # print(consumer.subscription())
    for message in consumer:
        print(message.value)
