# Databricks notebook source
bootstarp_servers='pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092'
api_key='7MBPE7ACHN76UR7K'
api_secret='cfltfoCs6Czx2aQvpiRaSjbbveUmg9bTPv5zbjgVEZwQY4il6A0OCGSo4zLSia5Q'
topic='credit_card_transaction'

# COMMAND ----------

jaas_config = f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";"""

# COMMAND ----------

sample_batch=spark.read.format("kafka") \
  .option("kafka.bootstrap.servers", bootstarp_servers) \
  .option("subscribe", topic) \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.sasl.jaas.config",jaas_config)\
  .option("startingOffsets", "earliest")\
  .load()

# COMMAND ----------

sample_batch.count()
sample_batch.display()

# COMMAND ----------

from pyspark.sql.functions import col
parsed_batch=sample_batch.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)

# COMMAND ----------

parsed_batch.display()

# COMMAND ----------

parsed_batch.write.saveAsTable("finguard.bronze.transactions_batch_test")

# COMMAND ----------

streaming_df=spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", bootstarp_servers) \
  .option("subscribe", topic) \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.sasl.jaas.config",jaas_config)\
  .option("startingOffsets", "earliest")\
  .load()

# COMMAND ----------

from pyspark.sql.functions import col
parsed_streaming_df=streaming_df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)

# COMMAND ----------

streaming_query=(parsed_streaming_df.writeStream.format("delta")
.outputMode("Append")
.option(
    "checkpointLocation",
    "/Volumes/finguard/source/transactions/checkpoint/"
)
.trigger(availableNow=True)
.toTable("finguard.bronze.transactions_streaming_test"))

display(spark.sql("select * from finguard.bronze.transactions_streaming_test"))