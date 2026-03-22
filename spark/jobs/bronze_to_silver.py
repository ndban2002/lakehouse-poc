import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, regexp_replace, sum as sum_, to_timestamp, trim, upper


def build_spark_session() -> SparkSession:
    iceberg_rest_uri = os.getenv("ICEBERG_REST_URI", "http://localhost:8181")

    return (
        SparkSession.builder.appName("bronze_to_silver")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.type", "rest")
        .config("spark.sql.catalog.rest.uri", iceberg_rest_uri)
        .config("spark.sql.catalog.rest.warehouse", "s3://warehouse")
        .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.rest.s3.endpoint", os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
        .config("spark.sql.catalog.rest.s3.path-style-access", "true")
        .config("spark.sql.catalog.rest.s3.access-key-id", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.sql.catalog.rest.s3.secret-access-key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.sql.catalog.rest.s3.region", os.getenv("AWS_REGION", "us-east-1"))
        .config("spark.sql.defaultCatalog", "rest")
        .getOrCreate()
    )


def run(run_date: str) -> None:
    spark = build_spark_session()

    raw_path = f"s3a://lakehouse/raw/banking_transactions/extract_date={run_date}/transactions_raw.parquet"
    tx_raw_df = spark.read.parquet(raw_path)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.lakehouse")

    tx_raw_df.writeTo("rest.lakehouse.transactions_raw").using("iceberg").createOrReplace()

    tx_normalized_df = (
        tx_raw_df.withColumn("txn_id_normalized", trim(col("txn_id")))
        .withColumn("customer_id_normalized", trim(col("customer_id")))
        .withColumn("txn_type_normalized", upper(trim(col("txn_type_raw"))))
        .withColumn("status_normalized", upper(trim(col("status_raw"))))
        .withColumn("currency_normalized", upper(trim(col("currency_raw"))))
        .withColumn("txn_ts", to_timestamp(col("txn_ts_raw")))
        .withColumn("amount_numeric", regexp_replace(trim(col("amount_raw")), ",", "").cast("double"))
    )

    daily_df = (
        tx_normalized_df.filter(col("status_normalized") == "SUCCESS")
        .filter(col("txn_ts").isNotNull())
        .filter(col("amount_numeric").isNotNull())
        .withColumn("txn_day", date_trunc("day", col("txn_ts")))
        .groupBy("txn_day", "customer_id_normalized")
        .agg(sum_("amount_numeric").alias("net_amount"))
    )

    daily_df.writeTo("rest.lakehouse.daily_customer_net_amount").using("iceberg").createOrReplace()
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True, help="Partition date in format YYYY-MM-DD")
    args = parser.parse_args()
    run(args.run_date)
