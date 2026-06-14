#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
)


KAFKA_BOOTSTRAP_SERVERS = "rc1b-pp8eafcpuf057hcb.mdb.yandexcloud.net:9091"
KAFKA_TOPIC = "loan-events-topic"
KAFKA_USERNAME = "user1"
KAFKA_PASSWORD = "password1"

OUTPUT_PATH = "s3a://dataproc-bucket-kate-2026/task3/output/loan_events_flatten_stream"
CHECKPOINT_PATH = "s3a://dataproc-bucket-kate-2026/task3/checkpoints/loan_events_flatten_stream"


def main():
    spark = (
        SparkSession.builder
        .appName("kafka-read-stream-flatten-loan-events")
        .getOrCreate()
    )

    schema = StructType([
        StructField("application_id", StringType(), True),

        StructField("customer", StructType([
            StructField("customer_id", StringType(), True),
            StructField("region", StringType(), True),
        ]), True),

        StructField("loan", StructType([
            StructField("amount", IntegerType(), True),
            StructField("term_months", IntegerType(), True),
        ]), True),

        StructField("scoring", StructType([
            StructField("score", IntegerType(), True),
            StructField("risk_level", StringType(), True),
        ]), True),

        StructField("documents", ArrayType(
            StructType([
                StructField("type", StringType(), True),
                StructField("status", StringType(), True),
            ])
        ), True),

        StructField("decision_status", StringType(), True),
        StructField("submitted_at", StringType(), True),
    ])

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option(
            "kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            f"username={KAFKA_USERNAME} "
            f"password={KAFKA_PASSWORD};"
        )
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
    )

    flat_df = (
        parsed_df
        .withColumn("document", explode(col("documents")))
        .select(
            col("application_id"),
            col("customer.customer_id").alias("customer_id"),
            col("customer.region").alias("region"),
            col("loan.amount").alias("amount"),
            col("loan.term_months").alias("term_months"),
            col("scoring.score").alias("score"),
            col("scoring.risk_level").alias("risk_level"),
            col("document.type").alias("document_type"),
            col("document.status").alias("document_status"),
            col("decision_status"),
            col("submitted_at"),
        )
    )

    query = (
        flat_df.writeStream
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(once=True)
        .start()
    )

    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()