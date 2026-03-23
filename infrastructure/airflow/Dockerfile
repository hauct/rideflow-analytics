# ==============================================================================
# Multi-stage build: compile wheels ở stage 1, chỉ copy kết quả sang stage 2
# ==============================================================================

# ── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM apache/airflow:slim-2.11.2-python3.11 AS builder

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc g++ libpq-dev && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM apache/airflow:slim-2.11.2-python3.11

USER root

# docker-cli: cần để BashOperator chạy "docker exec de-spark-master spark-submit"
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg \
      | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
      https://download.docker.com/linux/debian \
      $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
      > /etc/apt/sources.list.d/docker.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy pre-built Python packages from builder stage
COPY --from=builder /home/airflow/.local /home/airflow/.local
ENV PATH="/home/airflow/.local/bin:${PATH}"

WORKDIR /opt/airflow
