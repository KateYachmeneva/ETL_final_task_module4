from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, round


INPUT_PATH = "s3a://dataproc-bucket-kate-2026/task2/input/credit_applications.csv"
OUTPUT_BASE_PATH = "s3a://dataproc-bucket-kate-2026/task2/output"


def main():
    spark = (
        SparkSession.builder
        .appName("process-credit-applications")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(INPUT_PATH)
    )

    clean_df = (
        df
        .filter(col("application_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("requested_amount") > 0)
        .filter((col("credit_score") >= 300) & (col("credit_score") <= 900))
    )

    applications_by_region = (
        clean_df
        .groupBy("region_code")
        .agg(
            count("*").alias("applications_count"),
            spark_sum("requested_amount").alias("total_requested_amount"),
            spark_sum("approved_amount").alias("total_approved_amount"),
            round(avg("credit_score"), 2).alias("avg_credit_score"),
            round(avg("processing_time_sec"), 2).alias("avg_processing_time_sec"),
        )
    )

    applications_by_risk = (
        clean_df
        .groupBy("risk_level")
        .agg(
            count("*").alias("applications_count"),
            round(avg("credit_score"), 2).alias("avg_credit_score"),
            spark_sum("approved_amount").alias("total_approved_amount"),
        )
    )

    decisions_by_product = (
        clean_df
        .groupBy("product_type", "decision_status")
        .agg(
            count("*").alias("applications_count"),
            spark_sum("requested_amount").alias("total_requested_amount"),
            spark_sum("approved_amount").alias("total_approved_amount"),
        )
    )

    applications_by_channel = (
        clean_df
        .groupBy("channel")
        .agg(
            count("*").alias("applications_count"),
            round(avg("processing_time_sec"), 2).alias("avg_processing_time_sec"),
        )
    )

    applications_by_region.write.mode("overwrite").parquet(
        f"{OUTPUT_BASE_PATH}/applications_by_region"
    )

    applications_by_risk.write.mode("overwrite").parquet(
        f"{OUTPUT_BASE_PATH}/applications_by_risk"
    )

    decisions_by_product.write.mode("overwrite").parquet(
        f"{OUTPUT_BASE_PATH}/decisions_by_product"
    )

    applications_by_channel.write.mode("overwrite").parquet(
        f"{OUTPUT_BASE_PATH}/applications_by_channel"
    )

    spark.stop()


if __name__ == "__main__":
    main()