# Use the official Apache Spark Python image
FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

# Install system dependencies (e.g. build-essential if needed for some libraries)
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install required Python packages
# We install zeroetl from PyPI which includes pyspark, pandas, pyarrow, pyiceberg, and python-dotenv
# We also install kafka-python for our producer script
RUN pip3 install --no-cache-dir \
    zeroetl==0.1.0 \
    kafka-python-ng \
    pandas>=2.0.0 \
    pyarrow>=15.0.0 \
    pyiceberg[pyarrow]>=0.5.0 \
    python-dotenv>=1.0.0

WORKDIR /app

# Ensure correct permissions
RUN chmod -R 777 /app

USER spark
