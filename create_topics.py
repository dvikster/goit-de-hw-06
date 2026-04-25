from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import kafka_config, TOPIC_BUILDING_SENSORS, TOPIC_ALERTS_OUT


admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

topics = [
    NewTopic(
        name=TOPIC_BUILDING_SENSORS,
        num_partitions=3,
        replication_factor=1,
    ),
    NewTopic(
        name=TOPIC_ALERTS_OUT,
        num_partitions=3,
        replication_factor=1,
    ),
]

try:
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print("Топіки створено успішно.")
except TopicAlreadyExistsError:
    print("Деякі топіки вже існують.")
except Exception as e:
    print(f"Помилка створення топіків: {e}")

try:
    topics_all = sorted(admin_client.list_topics())

    print("\nНаявні топіки:")
    for topic in topics_all:
        print(topic)

    print("\nМої топіки:")
    for topic in topics_all:
        if "danyl" in topic:
            print(topic)

finally:
    admin_client.close()