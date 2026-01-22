FROM apache/airflow:2.7.1-python3.10

USER root

# Install OpenJDK-11 (Required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64


USER airflow

# Install PyPi dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
