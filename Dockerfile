FROM apache/airflow:2.8.1

# Switch to root to install system packages
USER root

# Install PostgreSQL client
RUN apt-get update && apt-get install -y postgresql-client

# Switch to airflow user to install Python packages properly
USER airflow

# Install kafka-python (NO --user flag here!)
RUN pip install --no-cache-dir kafka-python
