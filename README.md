# FinGuard — Real-Time Financial Transaction Monitoring

FinGuard is a real-time financial data streaming project built using **Apache Kafka, Databricks, and PySpark**. It demonstrates how financial transaction events can be ingested, processed, and analyzed through a structured data pipeline, with automated email notifications for high-value transactions.

## Project Overview

Financial systems generate large volumes of transaction events that need to be processed efficiently. FinGuard demonstrates a streaming architecture that consumes transaction events from Kafka, processes them in Databricks, organizes the data into layers, and supports high-value transaction alerts.

## Architecture

```text
Financial Transaction Events
            |
            v
      Apache Kafka
            |
            v
    Databricks Streaming
            |
            v
    Bronze Layer
  Raw transaction data
            |
            v
     Silver Layer
 Processed transaction data
            |
            v
      Gold Layer
  High-value transaction
   detection and alerts
            |
            v
    Email Notifications
```

## Technology Stack

- **Apache Kafka:** Streams transaction events between the producer and consumer.
- **Confluent Cloud:** Provides the managed Kafka environment.
- **Databricks:** Provides the environment for data ingestion and processing.
- **PySpark / Spark Structured Streaming:** Processes streaming data using Spark.
- **Python:** Implements pipeline logic and alerting workflows.
- **GitHub:** Stores the project source code and tracks changes.

## Project Structure

```text
finguard-project/
├── bronze/
│   ├── customer_bronze.py
│   └── transactions_bronze.py
├── silver/
│   └── transactions_silver.py
├── gold/
│   ├── high_value_transaction_email_alert.py
│   └── high_value_transactions_alert.py
├── 01_kafka_streaming_test.py
├── 03_Send_Email.py
└── README.md
```

### Pipeline Components

**1. Kafka Streaming Test**

`01_kafka_streaming_test.py`

Connects to the Kafka topic and demonstrates reading transaction events through Spark.

**2. Bronze Layer**

- `customer_bronze.py` — handles customer data ingestion.
- `transactions_bronze.py` — handles transaction data ingestion.

The Bronze layer represents the raw-data ingestion stage.

**3. Silver Layer**

`transactions_silver.py`

Contains transaction processing logic for the refined-data stage.

**4. Gold Layer**

- `high_value_transactions_alert.py` — contains logic related to high-value transaction alerts.
- `high_value_transaction_email_alert.py` — contains logic related to email alerts for high-value transactions.

The Gold layer represents the business-facing alerting stage.

**5. Email Notification**

`03_Send_Email.py`

Contains email notification functionality used in the project.

## Key Features

- Kafka-based transaction event ingestion.
- Streaming data processing with Spark.
- Layered Bronze, Silver, and Gold data organization.
- High-value transaction alerting logic.
- Email notification integration.
- Version control through GitHub.

## Setup and Configuration

This project is designed to run in a configured Databricks environment with access to a Kafka-compatible streaming source.

Before running the notebooks or scripts:

1. Configure a Kafka topic and connection in Confluent Cloud.
2. Configure the required Databricks environment and permissions.
3. Store Kafka API keys, secrets, and email credentials securely using Databricks secrets or an approved secret-management solution.
4. Update the configuration to reference the appropriate secret scope, keys, topic, and connection settings.
5. Run the ingestion components, followed by the processing and alerting components as appropriate for the implementation.

**Security:** Never commit API keys, passwords, access tokens, or other credentials to GitHub. Rotate any credentials that have previously been exposed.

## Learning Objectives

This project demonstrates practical experience with:

- Real-time data ingestion and streaming concepts.
- Apache Kafka and Confluent Cloud integration.
- Spark Structured Streaming and PySpark.
- Layered data pipeline design.
- Transaction monitoring and alerting workflows.
- Managing project source code using GitHub.

## Future Enhancements

Potential improvements include:

- Adding data-quality checks and schema validation.
- Implementing robust error handling and retry mechanisms.
- Adding monitoring, logging, and pipeline metrics.
- Building dashboards for transaction trends and alert statistics.
- Introducing automated tests and CI/CD deployment workflows.

## Repository

GitHub: https://github.com/Apeksha-2820/finguard-project

---

*FinGuard is a learning and portfolio project demonstrating real-time financial data streaming and transaction monitoring concepts.*
