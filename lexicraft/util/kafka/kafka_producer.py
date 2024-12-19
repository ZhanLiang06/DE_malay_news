from kafka import KafkaProducer
import json

class KafkaStreamProducer:
    def __init__(self, topic_name='beritaH', bootstrap_servers='localhost:9092'):
        self.topic_name = topic_name
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send_data(self, data, partition=0):
        try:
            self.producer.send(self.topic_name, value=data,partition=partition)
            self.producer.flush()  # Ensure data is sent
            print(f"Data sent to topic {self.topic_name}")
        except Exception as e:
            print(f"Failed to send data to Kafka: {e}")
