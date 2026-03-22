import argparse
import io
import os
from datetime import datetime

import boto3
import pandas as pd
import psycopg2


def extract_transactions_raw() -> pd.DataFrame:
    conn = psycopg2.connect(
        host=os.getenv("SOURCE_DB_HOST", "localhost"),
        port=int(os.getenv("SOURCE_DB_PORT", "5432")),
        dbname=os.getenv("SOURCE_DB_NAME", "source_db"),
        user=os.getenv("SOURCE_DB_USER", "source_user"),
        password=os.getenv("SOURCE_DB_PASSWORD", "source_pass"),
    )
    query = """
        SELECT
            raw_id,
            txn_id,
            customer_id,
            account_no,
            txn_ts_raw,
            amount_raw,
            currency_raw,
            txn_type_raw,
            channel_raw,
            merchant_raw,
            city_raw,
            country_raw,
            status_raw,
            reference_raw,
            ingestion_note,
            created_at
        FROM banking.transactions_raw
        ORDER BY raw_id
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def upload_to_minio(df: pd.DataFrame, run_date: str) -> str:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    object_key = f"raw/banking_transactions/extract_date={run_date}/transactions_raw.parquet"
    s3_client.put_object(Bucket="lakehouse", Key=object_key, Body=parquet_buffer.read())
    return f"s3://lakehouse/{object_key}"


def run(run_date: str | None = None) -> str:
    effective_date = run_date or datetime.utcnow().strftime("%Y-%m-%d")
    df = extract_transactions_raw()
    destination = upload_to_minio(df, effective_date)
    print(f"Uploaded {len(df)} rows to {destination}")
    return destination


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=None, help="Partition date in format YYYY-MM-DD")
    args = parser.parse_args()
    run(args.run_date)
