from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

@dp.table(
    name="finguard.bronze.transaction"
    ,comment="Transactions raw stream data ingested by kafka"
)
def transactions_bronze() -> DataFrame:

    bootstrap_servers='pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092'
    api_key='7MBPE7ACHN76UR7K'
    api_secret='cfltfoCs6Czx2aQvpiRaSjbbveUmg9bTPv5zbjgVEZwQY4il6A0OCGSo4zLSia5Q'
    topic='credit_card_transaction'

    jaas_config = f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";"""

    streaming_df=spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", topic) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",jaas_config)\
    .option("startingOffsets", "earliest")\
    .load()

    parsed_streaming_df=streaming_df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)

    return parsed_streaming_df