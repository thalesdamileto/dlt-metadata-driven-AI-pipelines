# Import modules
from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json

# COMMAND ----------

# Initiating parameters from pipeline configuration
kafka_bootstrap_servers = spark.conf.get("kafka_bootstrap_servers")
kafka_topic = spark.conf.get("kafka_topic")
# Spark SQL DDL for the Kafka message value (e.g. "struct<id:string,name:string,...>")
json_schema = spark.conf.get("json_schema")
target_schema = spark.conf.get("target_schema")
target_table = spark.conf.get("target_table")
description = spark.conf.get("description")

# COMMAND ----------

# Table attributes
target_catalog = "bronze"
full_table_name = f"`{target_catalog}`.`{target_schema}`.`{target_table}`"

# COMMAND ----------

@dp.table(
    name=target_table,  # DLT já sabe o schema do pipeline
    comment=f"Bronze delta table: {full_table_name}. Description: {description}"
)
def dlt_kafka_table_raw():
    kafka_df = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", kafka_topic)
            .option(
                "startingOffsets",
                spark.conf.get("kafka_starting_offsets", "latest"),
            )
            .load()
    )
    with_meta = kafka_df.select(
        col("timestamp").alias("kafka_timestamp"),
        col("topic"),
        col("partition"),
        col("offset"),
        from_json(col("value").cast("string"), json_schema).alias("payload"),
    )
    return with_meta.select(
        "kafka_timestamp",
        "topic",
        "partition",
        "offset",
        col("payload.*"),
    )
