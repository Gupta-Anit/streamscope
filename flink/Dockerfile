FROM flink:1.18.1-scala_2.12-java11

# ─── Iceberg-Flink runtime ─────────────────────────────────────────────
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.5.2/iceberg-flink-runtime-1.18-1.5.2.jar

# ─── Iceberg AWS bundle ────────────────────────────────────────────────
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar

# ─── Flink-shaded Hadoop uber ──────────────────────────────────────────
# Provides org.apache.hadoop.conf.Configuration (transitive dep of iceberg-flink-runtime).
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# ─── Flink Kafka SQL connector ─────────────────────────────────────────
# Required for the 'kafka' table connector. Versioned independently of Flink core.
# Format: flink-sql-connector-kafka-<connector_version>-<flink_minor>.jar
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.18/flink-sql-connector-kafka-3.2.0-1.18.jar

# ─── Flink S3 filesystem plugin (Flink's own checkpoint storage) ───────
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-1.18.1.jar /opt/flink/plugins/s3-fs-hadoop/

# ─── Bake our config into the image ────────────────────────────────────
COPY conf/flink-conf.yaml /opt/flink/conf/flink-conf.yaml
