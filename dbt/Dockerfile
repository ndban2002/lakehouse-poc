FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-core==1.8.8 dbt-trino==1.8.2

WORKDIR /usr/app
CMD ["tail", "-f", "/dev/null"]
