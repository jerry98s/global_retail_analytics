#!/usr/bin/env bash
# EMR bootstrap action: install Flink Iceberg + Kafka connector JARs and
# Python deps into the Flink lib/python paths so PyFlink jobs can sink to
# Iceberg on S3 and source from MSK.
#
# EMR 6.15.0 ships Flink 1.17.1 + Iceberg requires the Flink 1.17 runtime.
# Versions are pinned to keep job binaries deterministic.
#
# Connector JAR versions (Iceberg, Kafka, Hadoop-AWS, AWS SDK) are pinned
# via shell variables below. The single source of truth is
# infra/docker/flink/versions.env — keep these variables in sync with that
# file. tests/unit/test_flink_connector_versions.py fails CI on drift
# between this script, the local Flink Dockerfile, and versions.env.
set -euo pipefail

ARTIFACTS_BUCKET="${1:-}"
FLINK_LIB="/usr/lib/flink/lib"
ICEBERG_VERSION="1.4.3"
KAFKA_CONNECTOR_VERSION="1.17.1"
HADOOP_AWS_VERSION="3.3.6"
AWS_SDK_VERSION="1.12.262"

sudo mkdir -p "${FLINK_LIB}"

download() {
  local url="$1"
  local dest="$2"
  echo "Downloading ${url}"
  sudo curl -sSL --fail --retry 5 -o "${dest}" "${url}"
}

download \
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.17/${ICEBERG_VERSION}/iceberg-flink-runtime-1.17-${ICEBERG_VERSION}.jar" \
  "${FLINK_LIB}/iceberg-flink-runtime-1.17-${ICEBERG_VERSION}.jar"

download \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
  "${FLINK_LIB}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"

download \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
  "${FLINK_LIB}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"

download \
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
  "${FLINK_LIB}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# MSK IAM auth handler (needed when MSK uses SASL_SSL + IAM).
download \
  "https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.9/aws-msk-iam-auth-1.1.9-all.jar" \
  "${FLINK_LIB}/aws-msk-iam-auth-1.1.9-all.jar"

sudo chown -R hadoop:hadoop "${FLINK_LIB}"

# Python deps used by the PyFlink jobs themselves.
sudo python3 -m pip install --no-cache-dir pyyaml structlog

# Optional: pull repo-level config so jobs can read /opt/flink-config/*.yaml
if [[ -n "${ARTIFACTS_BUCKET}" ]]; then
  sudo mkdir -p /opt/flink-config
  aws s3 sync "s3://${ARTIFACTS_BUCKET}/streaming/config/" /opt/flink-config/ || true
fi

echo "Flink connectors installed."
