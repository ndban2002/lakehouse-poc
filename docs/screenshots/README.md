# Pipeline run evidence

Add screenshots (or a short recording) after executing the PoC.

Suggested captures:

1. `docker compose ps` showing all services healthy.
2. Airflow DAG graph and successful run state (`lakehouse_banking_transactions_to_iceberg`).
3. Trino query result (`SELECT * FROM iceberg.lakehouse.daily_customer_net_amount`).
4. dbt run/test output.

Example file names:

- `01-compose-services.png`
- `02-airflow-success.png`
- `03-trino-query.png`
- `04-dbt-run.png`
