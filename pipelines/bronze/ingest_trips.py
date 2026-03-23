"""
ingest_trips.py
---------------
PySpark script chạy trên Spark cluster (submit từ Airflow).
Đọc JSONL từ local filesystem → validate → ghi Delta Bronze trên MinIO.

Usage (từ Airflow SparkSubmitOperator):
    spark-submit \
      --master spark://spark-master:7077 \
      /opt/spark-apps/bronze/ingest_trips.py \
      --target-date "2026-03-21"
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# ─── Schema Definition ────────────────────────────────────────────────────────

TRIPS_SCHEMA = StructType([
    StructField("trip_id",       StringType(),    False),
    StructField("driver_id",     StringType(),    True),   # NULL nếu no_driver
    StructField("rider_id",      StringType(),    False),
    StructField("request_time",  TimestampType(), False),
    StructField("pickup_time",   TimestampType(), True),
    StructField("dropoff_time",  TimestampType(), True),
    StructField("status",        StringType(),    False),
    StructField("city",          StringType(),    False),
    StructField("pickup_zone",   StringType(),    False),
    StructField("pickup_lat",    DoubleType(),    False),
    StructField("pickup_lng",    DoubleType(),    False),
    StructField("dropoff_zone",  StringType(),    False),
    StructField("dropoff_lat",   DoubleType(),    False),
    StructField("dropoff_lng",   DoubleType(),    False),
    StructField("distance_km",   DoubleType(),    False),
    StructField("duration_min",  IntegerType(),   True),
    StructField("fare_vnd",      IntegerType(),   False),
])

# ─── Config ───────────────────────────────────────────────────────────────────

RAW_ROOT      = "/opt/spark-data/raw"
MINIO_ENDPOINT = "http://minio:9000"
BRONZE_BUCKET = "s3a://rideflow/bronze"

# ─── Functions ────────────────────────────────────────────────────────────────

def partition_path(entity: str, target_date: str) -> Path:
    """Hive-style partition path."""
    return (
        Path(RAW_ROOT) / entity
        / f"date={target_date}"
        / "data.jsonl"
    )


def validate_and_clean(df):
    """
    Validate data quality:
    - Remove duplicates
    - Filter invalid status
    - Check required fields
    """
    from pyspark.sql.functions import col
    
    # Remove duplicates
    df = df.dropDuplicates(["trip_id"])
    
    # Filter valid status
    df = df.filter(col("status").isin(["completed", "cancelled", "no_driver"]))
    
    # Filter valid cities
    df = df.filter(col("city").isin(["HCMC", "HANOI"]))
    
    # Remove rows with NULL trip_id or rider_id
    df = df.filter(col("trip_id").isNotNull() & col("rider_id").isNotNull())
    
    return df


def ingest_trips(target_date: str, minio_key: str, minio_secret: str):
    """Main ingestion logic."""
    
    jsonl_path = partition_path("trips", target_date)
    
    print(f"[ingest_trips] Target Date: {target_date}")
    print(f"[ingest_trips] JSONL path: {jsonl_path}")
    
    if not jsonl_path.exists():
        raise FileNotFoundError(f"JSONL not found: {jsonl_path}")
    
    # ── Create Spark Session ──────────────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName(f"rideflow_bronze_trips_{target_date.replace('-', '')}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO / S3A config
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", minio_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    # ── Read JSONL ─────────────────────────────────────────────────────────────
    print(f"[ingest_trips] Reading JSONL...")
    
    df = spark.read.json(str(jsonl_path), schema=TRIPS_SCHEMA)
    
    initial_count = df.count()
    print(f"[ingest_trips] Initial records: {initial_count}")
    
    # ── Validate & Clean ───────────────────────────────────────────────────────
    df = validate_and_clean(df)
    
    valid_count = df.count()
    print(f"[ingest_trips] Valid records: {valid_count}")
    print(f"[ingest_trips] Dropped: {initial_count - valid_count}")
    
    if valid_count == 0:
        print("[ingest_trips] No valid records — skipping write")
        spark.stop()
        return
    
    # ── Add audit columns ──────────────────────────────────────────────────────
    df = (df
          .withColumn("_ingested_at",  current_timestamp())
          .withColumn("_batch_id",     lit(target_date))
          .withColumn("_source",       lit("simulator"))
          .withColumn("ingest_date",   lit(target_date))
    )
    
    # ── Write to Delta Bronze ──────────────────────────────────────────────────
    bronze_path = f"{BRONZE_BUCKET}/trips"
    
    print(f"[ingest_trips] Writing to Delta: {bronze_path}")
    print(f"[ingest_trips] Partition: date={target_date}")
    
    (df.write
       .format("delta")
       .mode("append")
       .partitionBy("ingest_date")
       .save(bronze_path)
    )
    
    print(f"[ingest_trips] ✅ Written {valid_count} rows")
    
    spark.stop()


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-date", required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--minio-key", default="minioadmin")
    parser.add_argument("--minio-secret", default="minioadmin123")
    args = parser.parse_args()
    
    try:
        ingest_trips(args.target_date, args.minio_key, args.minio_secret)
    except Exception as e:
        print(f"[ingest_trips] ❌ ERROR: {e}", file=sys.stderr)
        sys.exit(1)