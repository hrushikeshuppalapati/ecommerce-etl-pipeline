FROM apache/airflow:2.7.1

RUN pip install --user --upgrade pip

RUN pip install --user apache-airflow-providers-spark