select
    cast(raw_id as bigint) as raw_id,
    trim(txn_id) as txn_id,
    trim(customer_id) as customer_id,
    trim(account_no) as account_no,
    txn_ts_raw,
    amount_raw,
    upper(trim(currency_raw)) as currency,
    upper(trim(txn_type_raw)) as txn_type,
    upper(trim(status_raw)) as status,
    channel_raw,
    merchant_raw,
    city_raw,
    country_raw,
    reference_raw,
    ingestion_note,
    created_at
from iceberg.lakehouse.transactions_raw
