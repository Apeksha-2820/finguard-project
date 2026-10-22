import dlt
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("card_number", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("payment_channel", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("is_international", BooleanType(), True),
    StructField("status", StringType(), True),
])

@dlt.table(
    name="finguard.silver.transaction"
    ,comment="Parsed and cleaned transactions data"
)


@dlt.expect_or_drop("valid_transaction_id","transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id","customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_card_number","card_number IS NOT NULL")
@dlt.expect_or_drop("valid_merchant_id","merchant_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount","amount>0")
def transactions_silver() -> DataFrame:
    bronze_df=spark.readStream.table("finguard.bronze.transaction")
    
    transformed_df=bronze_df.select(
        F.from_json(col("value"),schema).alias("data"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("ingestion_timestamp")
    ).select(
        F.col("data.*"),
        F.col("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("ingestion_timestamp")
    )

    return transformed_df




















