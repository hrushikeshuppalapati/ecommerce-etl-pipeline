# Start from the official Airflow image
FROM apache/airflow:2.7.1

# Switch to the root user to get permissions
USER root

# Install the Spark provider package
RUN pip install apache-airflow-providers-spark

# Switch back to the default airflow user
USER airflow