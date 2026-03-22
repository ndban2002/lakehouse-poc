select
    txn_day,
    customer_id_normalized as customer_id,
    net_amount
from iceberg.lakehouse.daily_customer_net_amount
