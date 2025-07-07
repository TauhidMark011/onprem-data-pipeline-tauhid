FROM apache/airflow:2.8.1

# Switch to root to install system packages
USER root
# Install PostgreSQL client
RUN apt-get update && apt-get install -y postgresql-client
# Install Java and download Spark
RUN apt-get update && apt-get install -y curl openjdk-17-jdk \
  && curl -L "https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz" \
  | tar -xz -C /opt \
  && mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Switch to airflow user to install Python packages properly
USER airflow

# Install kafka-python (NO --user flag here!)
RUN pip install --no-cache-dir kafka-python
