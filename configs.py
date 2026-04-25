kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
}



TOPIC_BUILDING_SENSORS = "building_sensors_danyl"
TOPIC_TEMPERATURE_ALERTS = "temperature_alerts_danyl"
TOPIC_HUMIDITY_ALERTS = "humidity_alerts_danyl"
TOPIC_ALERTS_OUT = "alerts_out_danyl"


def spark_kafka_options():
    return {
        "kafka.bootstrap.servers": kafka_config["bootstrap_servers"][0],
        "kafka.security.protocol": kafka_config["security_protocol"],
        "kafka.sasl.mechanism": kafka_config["sasl_mechanism"],
        "kafka.sasl.jaas.config": (
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" '
            f'password="{kafka_config["password"]}";'
        ),
    }