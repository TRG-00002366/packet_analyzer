FROM apache/spark:3.5.0
USER root


# Install Python, pip, and netcat (nc)
RUN apt-get update && \
    apt-get install -y python3 python3-pip netcat && \
    rm -rf /var/lib/apt/lists/* || true

# Install Python dependencies for Kafka, Airflow, PySpark, Faker, and lz4
RUN pip3 install --no-cache-dir apache-airflow kafka-python pyspark faker lz4

WORKDIR /app

# Copy project files
COPY spark/ ./spark/
COPY data/ ./data/
COPY kafka/ ./kafka/
COPY airflow/ ./airflow/
COPY utils/ ./utils/

# Set environment variables for Airflow and Python path
ENV AIRFLOW_HOME=/app/airflow
ENV PYTHONPATH="/app:/app/utils"

CMD ["/bin/bash"]
