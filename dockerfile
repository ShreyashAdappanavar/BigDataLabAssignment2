FROM apache/airflow:latest

USER root

# Install the required system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    unzip \
    wget \
    ffmpeg \
    libgeos-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch to the airflow user before using pip to install packages
USER airflow

# Install the required Python packages as the airflow user
RUN pip install --no-cache-dir \
    apache-beam \
    geopandas \
    geodatasets \
    requests \
    beautifulsoup4