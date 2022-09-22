from kafka import KafkaProducer
# import numpy as np
import time
import json
# from datetime import datetime


if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    while True:
        val = 0.1  # np.random.randn()
        producer.send(
            'timeseries-topic', {
                # 'timestamp': datetime.utcnow().strftime('%Y-%M-%d %H:%M:%S'),
                'value': val})
        time.sleep(1)
