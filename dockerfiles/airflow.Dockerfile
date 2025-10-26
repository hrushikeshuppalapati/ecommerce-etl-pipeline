# Start from the official Airflow image
# The default user is 'airflow', so we don't need any USER commands
FROM apache/airflow:2.7.1

# Install the Spark package to the 'airflow' user's local directory
# This is the officially supported way.
RUN pip install --user apache-airflow-providers-spark