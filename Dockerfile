FROM apache/airflow:2.9.1

USER root
RUN pip install --no-cache-dir pandas
USER airflow