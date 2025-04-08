import pandas as pd
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import json


class KafkaAPI:
    def __init__(self, bootstrap_servers="localhost:9092", group_id="default_group"):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id

    def create_topic(self, topic_name: str, num_partitions=3, replication_factor=1):
        admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        try:
            futures = admin_client.create_topics([topic])
            for topic, future in futures.items():
                try:
                    future.result()
                    print(f"Топик '{topic}' успешно создан.")
                except KafkaException as e:
                    print(f"Ошибка при создании топика '{topic}': {e}")
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    def delete_topic(self, topic_name: str):
        admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        try:
            futures = admin_client.delete_topics([topic_name])
            for topic, future in futures.items():
                try:
                    future.result()
                    print(f"Топик '{topic}' успешно удален.")
                except KafkaException as e:
                    print(f"Ошибка при удалении топика '{topic}': {e}")
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    def write_to_topic(self, topic_name: str, dataframe: pd.DataFrame):
        producer = Producer({"bootstrap.servers": self.bootstrap_servers})

        def delivery_report(err, msg):
            if err is not None:
                print(f"Сообщение не доставлено: {err}")
            else:
                print(f"Сообщение доставлено в топик {msg.topic()} [{msg.partition()}]")

        for _, row in dataframe.iterrows():
            message = row.to_json()
            producer.produce(topic_name, value=message, callback=delivery_report)
            producer.poll(0)

        producer.flush()

    def read_from_topic(self, topic_name: str, max_messages=100) -> pd.DataFrame:
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([topic_name])

        messages = []
        try:
            while len(messages) < max_messages:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print("Конец раздела")
                    elif msg.error():
                        print(f"Ошибка: {msg.error()}")
                else:
                    messages.append(json.loads(msg.value().decode("utf-8")))

        except KeyboardInterrupt:
            print("Прервано пользователем")
        finally:
            consumer.close()

        return pd.DataFrame(messages)


if __name__ == "__main__":
    kafka_api = KafkaAPI()
    kafka_api.create_topic("test-topic")
    df = pd.DataFrame(
        {"id": [1, 2, 3], "message": ["Hello, Kafka!", "This is a test.", "End of the test."]}
    )
    kafka_api.write_to_topic("test-topic", df)
    result_df = kafka_api.read_from_topic("test-topic", max_messages=3)
    print(result_df)
    kafka_api.delete_topic("test-topic")