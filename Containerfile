FROM docker.io/library/python:3.11-slim

WORKDIR /opt/airflow

# Install Java and system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    default-jdk \
    procps \
    postgresql-client \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Verify Java
RUN java -version && echo "JAVA_HOME: $JAVA_HOME"

# Copy requirements
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Download Spark S3 JARs
RUN mkdir -p /opt/spark-jars && \
    cd /opt/spark-jars && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar && \
    wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs
ENV PYTHONPATH=/opt/airflow
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080

EXPOSE 8080 4040 8888 9000 9001 5432

CMD ["airflow", "webserver"]

