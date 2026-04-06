#!/bin/bash
# Cleanup script for stale Delta table metastore entries in the gold schema
# This script drops all gold tables from the Hive metastore so dbt can recreate them cleanly

echo "Dropping stale gold tables from metastore..."

docker exec de-jupyter-spark spark-sql \
  --master spark://spark-master:7077 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  -e "
DROP TABLE IF EXISTS gold.dim_zone;
DROP TABLE IF EXISTS gold.dim_driver;
DROP TABLE IF EXISTS gold.dim_rider;
DROP TABLE IF EXISTS gold.fact_trips;
DROP TABLE IF EXISTS gold.demand_heatmap;
DROP TABLE IF EXISTS gold.dim_time;
DROP TABLE IF EXISTS gold.mart_cancellation_analysis;
DROP TABLE IF EXISTS gold.mart_daily_revenue;
DROP TABLE IF EXISTS gold.mart_driver_performance;
DROP TABLE IF EXISTS gold.mart_rating_distribution;
DROP TABLE IF EXISTS gold.mart_revenue_by_payment_method;
"

echo "Stale tables dropped. You can now run: dbt run --full-refresh"
