from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F


# ── Streaming Table: detect high-value transactions ──────────────────
@dp.table(
    name="finguard.gold.transactions",
    comment="Alert details while the transaction is performing with value higher than what is configured by customer"
)
def high_value_transaction_alert() -> DataFrame:
    transactions = spark.readStream.table("finguard.silver.transaction")
    customers = spark.read.table("finguard.silver.customers")

    joined_df = (
        transactions.join(customers, transactions.customer_id == customers.customer_id, "left")
                    .filter(F.col("amount") > F.col("transaction_limit"))
                    .select(
                        F.concat_ws("-", F.lit("ALERT"), F.col("transaction_id")).alias("alert_id"),
                        F.lit("High Value transaction").alias("alert_type"),
                        F.current_timestamp().alias("alert_timestamp"),
                        transactions.transaction_id,
                        transactions.customer_id,
                        customers.email.alias("customer_email"),
                        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("customer_name"),
                        transactions.transaction_timestamp.alias("transaction_timestamp"),
                        transactions.amount.alias("transaction_amount"),
                        customers.transaction_limit,
                        transactions.currency,
                        transactions.merchant_name,
                        transactions.merchant_category,
                    )
    )

    return joined_df
