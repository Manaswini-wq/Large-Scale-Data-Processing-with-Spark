import os
import yaml
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()


def load_config(path="config/config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def get_env(key):
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(f"Missing env variable: {key}")
    return val


def create_spark_session(config):
    return (
        SparkSession.builder
        .appName(config["spark"]["app_name"])
        .config(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1"
        )
        .config("spark.sql.shuffle.partitions", config["spark"]["shuffle_partitions"])
        .config(
            "spark.sql.autoBroadcastJoinThreshold",
            config["spark"]["broadcast_threshold"]
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
