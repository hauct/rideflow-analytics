# ==============================================================================
# Jupyter Lab with PySpark - Optimized build
# ==============================================================================
FROM apache/spark:3.5.1

USER root

# ── System deps (chỉ những gì thực sự cần) ───────────────────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-pip \
        python3-dev \
        build-essential \
        curl && \
    rm -rf /var/lib/apt/lists/*

# ── Python packages ──────────────────────────────────────────────────────────
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt && \
    # Xóa build tools sau khi compile xong → giảm ~150MB
    apt-get update && \
    apt-get purge -y --auto-remove build-essential python3-dev && \
    rm -rf /var/lib/apt/lists/* /root/.cache/pip

# ── Download JARs (Delta Lake, PostgreSQL, AWS) ──────────────────────────────
RUN cd ${SPARK_HOME}/jars && \
    curl -fsSL -o hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fsSL -o postgresql-42.7.1.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar && \
    curl -fsSL -o delta-spark_2.12-3.0.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar && \
    curl -fsSL -o delta-storage-3.0.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar

# ── Directories (gộp 1 layer) ────────────────────────────────────────────────
RUN mkdir -p /home/spark/.local/share/jupyter/runtime \
             /opt/spark-notebooks /opt/spark-data /opt/spark-apps \
             /opt/spark-scripts /opt/spark-events /opt/shared && \
    chown -R spark:spark /home/spark /opt/spark-notebooks /opt/spark-data \
             /opt/spark-apps /opt/spark-scripts /opt/spark-events /opt/shared

USER spark
ENV SHELL=/bin/bash

WORKDIR /opt/spark-notebooks
EXPOSE 8888 4040

CMD ["python3", "-m", "jupyterlab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token=dataengineer"]