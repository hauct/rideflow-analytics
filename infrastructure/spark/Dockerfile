FROM apache/spark:3.5.1

USER root

# ── System deps ───────────────────────────────────────────────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl procps netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# ── Download JARs ─────────────────────────────────────────────────────────────
# 5 JARs cần thiết cho project này:
#   hadoop-aws + aws-java-sdk-bundle  → S3A filesystem (MinIO)
#   postgresql                         → JDBC connector
#   delta-spark_2.12 + delta-storage  → Delta Lake
RUN cd /opt/spark/jars && \
    curl -fsSL -o hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fsSL -o postgresql-42.7.1.jar \
        https://jdbc.postgresql.org/download/postgresql-42.7.1.jar && \
    curl -fsSL -o delta-spark_2.12-3.0.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar && \
    curl -fsSL -o delta-storage-3.0.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar && \
    ls -lh hadoop-aws-3.3.4.jar aws-java-sdk-bundle-1.12.262.jar \
           postgresql-42.7.1.jar delta-spark_2.12-3.0.0.jar delta-storage-3.0.0.jar

# ── Python deps + Directories (gộp 1 layer) ──────────────────────────────────
RUN pip3 install --no-cache-dir delta-spark==3.0.0 && \
    mkdir -p /opt/spark-apps /opt/spark-data/raw /opt/spark-events /opt/spark/conf && \
    chown -R spark:spark /opt/spark-apps /opt/spark-data /opt/spark-events /opt/spark/conf && \
    chmod -R 755 /opt/spark-apps /opt/spark-data /opt/spark-events /opt/spark/conf

# ── spark-defaults.conf ──────────────────────────────────────────────────────
RUN printf '%s\n' \
  'spark.hadoop.fs.s3a.impl                   org.apache.hadoop.fs.s3a.S3AFileSystem' \
  'spark.hadoop.fs.s3a.path.style.access      true' \
  'spark.hadoop.fs.s3a.connection.ssl.enabled false' \
  'spark.sql.extensions                       io.delta.sql.DeltaSparkSessionExtension' \
  'spark.sql.catalog.spark_catalog            org.apache.spark.sql.delta.catalog.DeltaCatalog' \
  'spark.eventLog.enabled                     true' \
  'spark.eventLog.dir                         /opt/spark-events' \
  'spark.executor.cores                       1' \
  'spark.executor.memory                      512m' \
  'spark.driver.memory                        512m' \
  > /opt/spark/conf/spark-defaults.conf && \
    chown spark:spark /opt/spark/conf/spark-defaults.conf

USER spark
WORKDIR /opt/spark

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080 || exit 1
