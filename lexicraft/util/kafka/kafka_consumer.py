from kafka import KafkaConsumer, TopicPartition
import json
import logging

def get_kafka_topic_latest_message(topics, bootstrap_servers, kafka_partition):
    topic = topics
    bootstrap_servers = bootstrap_servers
    partition = kafka_partition
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    topic_partition = TopicPartition(topic, partition)
    consumer.assign([topic_partition])
    consumer.seek_to_end(topic_partition)
    latest_offset = consumer.position(topic_partition)
    
    consumer.seek(topic_partition, latest_offset - 1)
    new_message = None
    for message in consumer:
        new_message = message
        break  
    consumer.close()
    
    if new_message == None:
        return None
    else:
        # if topic == "morphological_analysis":
        #     result = json.loads(new_message.value)
        #     df_received = pd.DataFrame(result)
        #     return df_received
        return new_message

# class KafkaStreamConsumer:
#     def __init__(self, topic_name='beritaH', bootstrap_servers='localhost:9092'):
#         self.topic_name = topic_name
#         self.consumer = KafkaConsumer(
#             topic_name,
#             bootstrap_servers=bootstrap_servers,
#             value_deserializer=lambda v: json.loads(v.decode('utf-8')),
#             auto_offset_reset='earliest',
#             enable_auto_commit=True,
#             consumer_timeout_ms=10000  # Stop polling after 10 seconds of inactivity
#         )
#         self.logger = logging.getLogger(__name__)
#         logging.basicConfig(level=logging.INFO)

#     def consume_data(self, max_records=None):
#         try:
#             self.logger.info(f"Consuming messages from Kafka topic: {self.topic_name}")
#             data_list = []
#             for idx, message in enumerate(self.consumer):
#                 data_list.append(message.value)
#                 self.logger.info(f"Consumed message: {message.value}")

#                 # Stop if max_records is reached
#                 if max_records and idx + 1 >= max_records:
#                     break

#             return data_list
#         except Exception as e:
#             self.logger.error(f"Error while consuming messages: {e}")
#             return []
