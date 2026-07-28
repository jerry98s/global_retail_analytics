# Local Flink image used by infra/docker/compose/docker-compose.yml.
# Pinned to flink:1.17.1-scala_2.12 to match EMR 6.15.0's Flink runtime
# in production so jobs smoke-tested locally match what runs on EMR.
#
# Connector JAR versions (Iceberg, Kafka, Hadoop-AWS, AWS SDK) are pinned
# via ENVs below. The single source of truth is
# infra/docker/flink/versions.env — keep the ENVs in sync with that file.
# tests/unit/test_flink_connector_versions.py fails CI on drift between
# this Dockerfile, install_flink_connectors.sh, and versions.env.
FROM flink:1.17.1-scala_2.12

USER root

# System deps + Python (PyFlink 1.17 requires Python 3.7-3.10; the slim image
# has 3.9 in apt).
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl python3 python3-pip ca-certificates \
 && rm -rf /var/lib/apt/lists/*

ENV ICEBERG_VERSION=1.4.3 \
    KAFKA_CONNECTOR_VERSION=1.17.1 \
    HADOOP_AWS_VERSION=3.3.6 \
    AWS_SDK_VERSION=1.12.262

# Iceberg + Kafka SQL connectors and Hadoop client JARs.
#
# `hadoop-client-api` and `hadoop-client-runtime` are the modern shaded
# bundles - together they provide org.apache.hadoop.conf.Configuration and
# every other Hadoop dep Iceberg's FlinkCatalogFactory expects on the
# classpath. EMR ships Hadoop natively so we only need this for local.
RUN set -eux; \
    cd /opt/flink/lib && \
    curl -sSL --retry 5 -o "iceberg-flink-runtime-1.17-${ICEBERG_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.17/${ICEBERG_VERSION}/iceberg-flink-runtime-1.17-${ICEBERG_VERSION}.jar" && \
    curl -sSL --retry 5 -o "flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" && \
    curl -sSL --retry 5 -o "hadoop-client-api-${HADOOP_AWS_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_AWS_VERSION}/hadoop-client-api-${HADOOP_AWS_VERSION}.jar" && \
    curl -sSL --retry 5 -o "hadoop-client-runtime-${HADOOP_AWS_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_AWS_VERSION}/hadoop-client-runtime-${HADOOP_AWS_VERSION}.jar" && \
    curl -sSL --retry 5 -o "commons-logging-1.2.jar" \
        "https://repo1.maven.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar" && \
    curl -sSL --retry 5 -o "hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    curl -sSL --retry 5 -o "aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# Flink's checkpoint storage requires its own s3 filesystem PLUGIN (must live
# under /opt/flink/plugins/<name>/, not /opt/flink/lib/). The base image ships
# flink-s3-fs-hadoop in /opt/flink/opt/; install it as a plugin so jobs can
# checkpoint to s3a:// (Iceberg already uses hadoop-aws from lib/).
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && cp /opt/flink/opt/flink-s3-fs-hadoop-1.17.1.jar /opt/flink/plugins/s3-fs-hadoop/

RUN pip3 install --no-cache-dir apache-flink==1.17.1 pyyaml structlog

# PyFlink CLI spawns `python`; Debian only provides `python3` by default.
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Local Iceberg warehouse (file://) lives here so jobs can write without S3.
RUN mkdir -p /tmp/iceberg && chown -R flink:flink /tmp/iceberg

USER flink
WORKDIR /opt/flink
