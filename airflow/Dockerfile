# This Docker Container extends the official Apache Airflow Container to install the required depencies for my DAGs

FROM apache/airflow:3.0.1
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
