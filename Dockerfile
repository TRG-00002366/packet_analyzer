FROM apache/spark:3.5.0
USER root


# Install Python, pip, and netcat (nc)
RUN apt-get update && \
    apt-get install -y python3 python3-pip netcat && \
    rm -rf /var/lib/apt/lists/* || true

# Install Python dependencies for the Spark-side services
RUN pip3 install --no-cache-dir kafka-python pyspark faker lz4

WORKDIR /app

# Copy project files
COPY spark/ ./spark/
COPY data/ ./data/
COPY kafka/ ./kafka/
COPY utils/ ./utils/

# Set Python path for project modules
ENV PYTHONPATH="/app:/app/utils"

CMD ["/bin/bash"]
