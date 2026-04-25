import json

from kafka import KafkaConsumer
from configs import kafka_config, TOPIC_ALERTS_OUT


def main():
    consumer = KafkaConsumer(
        TOPIC_ALERTS_OUT,
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="alerts_reader_group_danyl",
    )

    print(f"[reader] subscribed to: {TOPIC_ALERTS_OUT}")

    try:
        for msg in consumer:
            print(f"[{msg.topic}] key={msg.key} → {msg.value}")

    except KeyboardInterrupt:
        print("Зупинено користувачем.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()