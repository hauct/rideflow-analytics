#!/bin/bash
set -e

echo "=========================================="
echo "🚀 RideFlow Airflow Initialization"
echo "=========================================="

# Chờ Postgres
echo "⏳ Waiting for Postgres..."
sleep 10

# Migrate DB
echo "📦 Running airflow db migrate..."
airflow db migrate

# Tạo user
echo "👤 Creating admin user..."
airflow users create \
  --username "${_AIRFLOW_WWW_USER_USERNAME}" \
  --password "${_AIRFLOW_WWW_USER_PASSWORD}" \
  --firstname RideFlow \
  --lastname Admin \
  --role Admin \
  --email admin@rideflow.local 2>/dev/null || echo "ℹ️  User already exists"

echo ""
echo "🔧 Setting Airflow Variables..."

# Variables
airflow variables set MINIO_ENDPOINT "http://minio:9000" 2>/dev/null || true
airflow variables set MINIO_ACCESS_KEY "${MINIO_ROOT_USER}" 2>/dev/null || true
airflow variables set MINIO_SECRET_KEY "${MINIO_ROOT_PASSWORD}" 2>/dev/null || true
airflow variables set SIMULATOR_PATH "/opt/airflow/data/generators/simulator.py" 2>/dev/null || true

echo ""
echo "🔌 Creating Airflow Connections..."

# Xóa connections cũ
airflow connections delete rideflow_postgres 2>/dev/null || true
airflow connections delete spark_default 2>/dev/null || true

# Tạo Postgres Connection
echo "  Creating rideflow_postgres..."
airflow connections add rideflow_postgres \
  --conn-type postgres \
  --conn-host postgres \
  --conn-login "${POSTGRES_USER}" \
  --conn-password "${POSTGRES_PASSWORD}" \
  --conn-schema "${POSTGRES_DB}" \
  --conn-port 5432

echo "✅ Created: rideflow_postgres"

# Tạo Spark Connection
echo "  Creating spark_default..."
airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark://spark-master \
  --conn-port 7077

echo "✅ Created: spark_default"

echo ""
echo "=========================================="
echo "✅ Airflow initialization complete!"
echo "=========================================="