FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
# docker SDK: the DAGs talk to the other containers through the Docker socket
RUN pip install --no-cache-dir docker==7.1.0
