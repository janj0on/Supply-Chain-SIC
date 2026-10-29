FROM flink:1.20.5

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Jars for Flink 1.20:
#   - Iceberg runtime for Flink (tables + the Hadoop catalog that stores them on HDFS)
#   - Hadoop 3 client (hadoop-client-api + hadoop-client-runtime). The version matches the HDFS
#     image in the compose file (apache/hadoop:3.4.1), so Flink talks to the NameNode with a
#     client of the same major version.
#   - Kafka connector
# No S3 / AWS bundle any more: the data lives on HDFS.
ARG ICEBERG_VERSION=1.9.2
ARG HADOOP_VERSION=3.4.1
RUN set -eux; cd /opt/flink/lib; \
    curl -fsSLO "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar"; \
    curl -fsSLO "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar"; \
    curl -fsSLO "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar"

# Folders for checkpoints and savepoints (shared by the JobManager and the TaskManager)
RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints \
    && chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints /opt/flink/lib

USER flink
