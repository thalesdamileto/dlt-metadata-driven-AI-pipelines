# Import modules
# Databricks Runtime: from_avro with subject + schemaRegistryAddress requires DBR with
# Confluent Schema Registry integration (see structured-streaming/avro-dataframe).
from pyspark import pipelines as dp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col

# COMMAND ----------

# Initiating parameters from pipeline configuration
kafka_bootstrap_servers = spark.conf.get("kafka_bootstrap_servers")
kafka_topic = spark.conf.get("kafka_topic")
schema_registry_url = spark.conf.get("schema_registry_url")
# Confluent convention: value subject is "<topic>-value" unless overridden
schema_registry_value_subject = spark.conf.get(
    "schema_registry_value_subject", f"{kafka_topic}-value"
)
target_schema = spark.conf.get("target_schema")
target_table = spark.conf.get("target_table")
description = spark.conf.get("description")

# COMMAND ----------

# Options for from_avro + Schema Registry (DBR 12.2+ basic auth example in docs)
schema_registry_options = {
    "mode": spark.conf.get("schema_registry_from_avro_mode", "FAILFAST"),
}
_avro_evo = spark.conf.get("avro_schema_evolution_mode")
if _avro_evo:
    schema_registry_options["avroSchemaEvolutionMode"] = _avro_evo

_basic_auth = spark.conf.get("schema_registry_basic_auth_user_info", "")
if _basic_auth:
    schema_registry_options[
        "confluent.schema.registry.basic.auth.credentials.source"
    ] = "USER_INFO"
    schema_registry_options[
        "confluent.schema.registry.basic.auth.user.info"
    ] = _basic_auth
else:
    _sr_key = spark.conf.get("schema_registry_api_key", "")
    _sr_secret = spark.conf.get("schema_registry_api_secret", "")
    if _sr_key and _sr_secret:
        schema_registry_options[
            "confluent.schema.registry.basic.auth.credentials.source"
        ] = "USER_INFO"
        schema_registry_options[
            "confluent.schema.registry.basic.auth.user.info"
        ] = f"{_sr_key}:{_sr_secret}"

# COMMAND ----------

# Table attributes
target_catalog = "bronze"
full_table_name = f"`{target_catalog}`.`{target_schema}`.`{target_table}`"

# COMMAND ----------

@dp.table(
    name=target_table,  # DLT já sabe o schema do pipeline
    comment=f"Bronze delta table: {full_table_name}. Description: {description}",
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
    # Deserialize Confluent Avro wire format using Schema Registry (value column)
    value_decoded = from_avro(
        col("value"),
        schema_registry_value_subject,
        schema_registry_url,
        schema_registry_options,
    )
    with_meta = kafka_df.select(
        col("timestamp").alias("kafka_timestamp"),
        col("topic"),
        col("partition"),
        col("offset"),
        value_decoded.alias("payload"),
    )
    return with_meta.select(
        "kafka_timestamp",
        "topic",
        "partition",
        "offset",
        col("payload.*"),
    )
