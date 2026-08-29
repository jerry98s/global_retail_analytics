# Local Spark image used by infra/docker/compose/docker-compose.yml
# (profile `spark`). Pinned to Spark 3.4.1 to match EMR 6.15.0 so the
# GraphFrames identity job smoke-tested locally matches the EMR step.
#
# Iceberg Spark runtime 1.4.3 matches Flink (infra/docker/flink/versions.env).
# GraphFrames 0.8.3 is the Spark 3.4 / Scala 2.12 artifact used on EMR.
#
# JARs are baked in (no `--packages` at submit time) so `-Task spark` does not
# need a host Spark/JDK install or Maven on the laptop.

FROM apache/spark:3.4.1-python3

USER root

ENV ICEBERG_VERSION=1.4.3 \
    GRAPHFRAMES_VERSION=0.8.3-spark3.4-s_2.12 \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH=/opt/graphframes-python

# GraphFrames is published to spark-packages, not Maven Central.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl python3 python3-pip ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    ln -sf /usr/bin/python3 /usr/bin/python; \
    pip3 install --no-cache-dir 'numpy>=1.19,<1.25' 'pandas>=1.3,<2.1'; \
    cd /opt/spark/jars; \
    curl -fSL --retry 5 -o "iceberg-spark-runtime-3.4_2.12-${ICEBERG_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.4_2.12-${ICEBERG_VERSION}.jar"; \
    curl -fSL --retry 5 -o "graphframes-${GRAPHFRAMES_VERSION}.jar" \
        "https://repos.spark-packages.org/graphframes/graphframes/${GRAPHFRAMES_VERSION}/graphframes-${GRAPHFRAMES_VERSION}.jar"

COPY extract_graphframes_py.py /tmp/extract_graphframes_py.py
RUN python3 /tmp/extract_graphframes_py.py

WORKDIR /opt/spark-identity/identity_resolution
