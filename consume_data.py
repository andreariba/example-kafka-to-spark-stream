from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, col, regexp_replace, regexp_extract
from pyspark.sql.types import FloatType
from consumer_utils import get_latest_version_schema, json_schema_to_spark_structtype
import yaml


# function to create new SparkSession
def create_session():
    spk = SparkSession.builder \
    	.master("local") \
    	.appName("staging_engine") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0")\
    	.getOrCreate()
    return spk


@udf(returnType=FloatType())
def transform_signal_to_real_value(signal: float):
    return signal*20+10



if __name__=="__main__":
    with open('configuration.yaml','r') as fp:
        config = yaml.load(fp, Loader=yaml.CLoader)

    topic = config['KAFKA_TOPIC']

    kafka_options = {
        "kafka.bootstrap.servers": config['KAFKA_BOOTSTRAP_SERVERS'],
        "subscribe": topic
    }

    registry_schema = get_latest_version_schema(url=config['SCHEMA_REGISTRY_URL'], topic=topic)
    
    schema = json_schema_to_spark_structtype(registry_schema)

    spark = create_session()

    streaming_df = spark.readStream\
        .format("kafka") \
        .options(**kafka_options) \
        .option("startingOffsets", "latest") \
        .load()
    
    value_df = streaming_df \
        .select('Timestamp',regexp_extract(col("value").cast("string"), "\{.*\}", 0).alias("value")) \
        .select('Timestamp', from_json(col("value").cast("string"),schema).alias("value"))
    
    exploded_df = value_df \
        .selectExpr('Timestamp', 'value.sensor_id', 'value.value') \
        .withColumn('real_value', transform_signal_to_real_value('value'))
    
    query = exploded_df \
            .writeStream \
            .format("console") \
            .outputMode("append") \
            .start()
    
    query.awaitTermination()