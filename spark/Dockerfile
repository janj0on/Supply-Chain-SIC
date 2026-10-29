FROM apache/spark:3.5.6

USER root

RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar \
    -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar \
    && curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.4.1/hadoop-client-api-3.4.1.jar \
    -o /opt/spark/jars/hadoop-client-api-3.4.1.jar \
    && curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.4.1/hadoop-client-runtime-3.4.1.jar \
    -o /opt/spark/jars/hadoop-client-runtime-3.4.1.jar

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
