FROM apache/airflow:2.10.3-python3.12

# Only what the DAGs import on top of the Airflow image, pinned against Airflow's constraints.
RUN pip install --no-cache-dir \
      "clickhouse-connect==0.8.11" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.12.txt"
