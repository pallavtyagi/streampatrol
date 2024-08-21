from kafka import KafkaProducer
import json


class KafkaConnector():
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def push_message(self, topic, message):
        self.producer.send(topic, value=message)
        self.producer.flush()

    def close(self):
        self.producer.close()
