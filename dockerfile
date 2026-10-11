FROM apache/airflow:2.8.1-python3.10

# The base image already contains Airflow itself and psycopg2.
# Only add what the DAG tasks import. pip must run as the "airflow" user
# (the base image's default), not root.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt