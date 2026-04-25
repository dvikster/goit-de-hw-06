import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from configs import kafka_config, TOPIC_BUILDING_SENSORS


def current_time():
    return datetime.now(timezone.utc).isoformat()


def main():
    sensor_id = str(uuid.uuid4())[:8]

    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )

    print(f"[producer] sensor_id={sensor_id}")
    print(f"[producer] topic={TOPIC_BUILDING_SENSORS}")

    try:
        for i in range(40):
            payload = {
                "sensor_id": sensor_id,
                "timestamp": current_time(),
                "temperature": random.randint(20, 50),
                "humidity": random.randint(10, 90)
            }

            producer.send(
                TOPIC_BUILDING_SENSORS,
                key=sensor_id,
                value=payload,
            )

            producer.flush()

            print(f"sent [{i + 1:02d}]: {payload}")

            time.sleep(2)

    except KeyboardInterrupt:
        print("Зупинено користувачем.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()