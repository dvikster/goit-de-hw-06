import os

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages "
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 "
    "pyspark-shell"
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    from_unixtime,
    window,
    avg,
    when,
    struct,
    to_json,
    expr,
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from configs import TOPIC_BUILDING_SENSORS, TOPIC_ALERTS_OUT, spark_kafka_options


ALERTS_FILE = "alerts_conditions.csv"
CHECKPOINT_DIR = "./checkpoints/sensor_alerts"


spark = (
    SparkSession.builder
    .appName("SensorAlerts")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_options = spark_kafka_options()


sensor_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True),
])


raw_stream = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .option("subscribe", TOPIC_BUILDING_SENSORS)
    .option("startingOffsets", "earliest")
    .load()
)


sensor_stream = (
    raw_stream
    .select(from_json(col("value").cast("string"), sensor_schema).alias("data"))
    .select(
        col("data.sensor_id").alias("sensor_id"),

        when(
            col("data.timestamp").rlike("^[0-9]+(\\.[0-9]+)?(E[0-9]+)?$"),
            from_unixtime(col("data.timestamp").cast("double")).cast("timestamp")
        ).otherwise(
            to_timestamp(col("data.timestamp"))
        ).alias("event_time"),

        col("data.temperature").cast(DoubleType()).alias("temperature"),
        col("data.humidity").cast(DoubleType()).alias("humidity"),
    )
    .where(col("event_time").isNotNull())
)


averages = (
    sensor_stream
    .withWatermark("event_time", "10 seconds")
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds")
    )
    .agg(
        avg(col("temperature")).alias("t_avg"),
        avg(col("humidity")).alias("h_avg"),
    )
)


conditions_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ALERTS_FILE)
)


conditions = conditions_raw.select(
    col("id").cast("int").alias("rule_id"),

    when(col("humidity_min") == -999, None)
    .otherwise(col("humidity_min").cast(DoubleType()))
    .alias("h_min"),

    when(col("humidity_max") == -999, None)
    .otherwise(col("humidity_max").cast(DoubleType()))
    .alias("h_max"),

    when(col("temperature_min") == -999, None)
    .otherwise(col("temperature_min").cast(DoubleType()))
    .alias("t_min"),

    when(col("temperature_max") == -999, None)
    .otherwise(col("temperature_max").cast(DoubleType()))
    .alias("t_max"),

    col("code").cast("string").alias("code"),
    col("message").cast("string").alias("message"),
)


alert_filter = (
    ((col("h_min").isNull()) | (col("h_avg") >= col("h_min"))) &
    ((col("h_max").isNull()) | (col("h_avg") <= col("h_max"))) &
    ((col("t_min").isNull()) | (col("t_avg") >= col("t_min"))) &
    ((col("t_max").isNull()) | (col("t_avg") <= col("t_max")))
)


alerts = (
    averages
    .crossJoin(conditions)
    .where(alert_filter)
    .select(
        struct(
            col("window.start").alias("start"),
            col("window.end").alias("end"),
        ).alias("window"),
        col("t_avg"),
        col("h_avg"),
        col("code"),
        col("message"),
    )
)


alerts_to_kafka = alerts.select(
    col("code").alias("key"),
    to_json(
        struct(
            col("window"),
            col("t_avg"),
            col("h_avg"),
            col("code"),
            col("message"),
            expr("current_timestamp()").alias("timestamp"),
        )
    ).alias("value")
)


query = (
    alerts_to_kafka.writeStream
    .format("kafka")
    .options(**kafka_options)
    .option("topic", TOPIC_ALERTS_OUT)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .outputMode("append")
    .start()
)

query.awaitTermination()