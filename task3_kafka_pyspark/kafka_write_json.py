#!/usr/bin/env python3

import traceback

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    lit,
    struct,
    array,
    to_json,
    when,
)


KAFKA_BOOTSTRAP_SERVERS = "rc1b-pp8eafcpuf057hcb.mdb.yandexcloud.net:9091"
KAFKA_TOPIC = "loan-events-topic"
KAFKA_USERNAME = "user1"
KAFKA_PASSWORD = "password1"


def main():
    spark = SparkSession.builder.appName("kafka-write-loan-events").getOrCreate()

    # 100 000 JSON-сообщений дают объём больше 20 МБ
    rows_count = 100_000

    df = spark.range(0, rows_count).select(
        concat(lit("loan_"), col("id")).alias("application_id"),

        struct(
            concat(lit("cust_"), (col("id") % 10000)).alias("customer_id"),
            when((col("id") % 5) == 0, lit("DE-HE"))
            .when((col("id") % 5) == 1, lit("DE-BE"))
            .when((col("id") % 5) == 2, lit("DE-BY"))
            .when((col("id") % 5) == 3, lit("DE-HH"))
            .otherwise(lit("DE-NW"))
            .alias("region"),
            ).alias("customer"),

        struct(
            (lit(5000) + (col("id") % 50000)).cast("int").alias("amount"),
            (lit(6) + (col("id") % 60)).cast("int").alias("term_months"),
        ).alias("loan"),

        struct(
            (lit(300) + (col("id") % 600)).cast("int").alias("score"),
            when((col("id") % 3) == 0, lit("low"))
            .when((col("id") % 3) == 1, lit("medium"))
            .otherwise(lit("high"))
            .alias("risk_level"),
            ).alias("scoring"),

        array(
            struct(
                lit("passport").alias("type"),
                when((col("id") % 2) == 0, lit("verified"))
                .otherwise(lit("pending"))
                .alias("status"),
                )
        ).alias("documents"),

        when((col("id") % 4) == 0, lit("approved"))
        .when((col("id") % 4) == 1, lit("rejected"))
        .when((col("id") % 4) == 2, lit("manual_review"))
        .otherwise(lit("pending"))
        .alias("decision_status"),

        concat(
            lit("2026-05-"),
            ((col("id") % 28) + 1).cast("string"),
            lit("T10:15:11Z"),
        ).alias("submitted_at"),
        )

    json_df = df.select(
        to_json(
            struct(
                col("application_id"),
                col("customer"),
                col("loan"),
                col("scoring"),
                col("documents"),
                col("decision_status"),
                col("submitted_at"),
            )
        ).alias("value")
    )

    json_df.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_TOPIC) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option(
        "kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f"username={KAFKA_USERNAME} "
        f"password={KAFKA_PASSWORD};"
    ) \
        .save()

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        raise