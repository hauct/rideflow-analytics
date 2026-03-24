import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# ─── Schema Definition ────────────────────────────────────────────────────────

PAYMENTS_SCHEMA = StructType([
    StructField("payment_id",         StringType(),    True),
    StructField("trip_id",            StringType(),    True),
    StructField("rider_id",           StringType(),    True),
    StructField("payment_time",       TimestampType(), True),
    StructField("payment_method",     StringType(),    True),
    StructField("fare_vnd",           IntegerType(),   True),
    StructField("promo_code",         StringType(),    True),
    StructField("discount_vnd",       IntegerType(),   True),
    StructField("final_amount_vnd",   IntegerType(),   True),
    StructField("payment_status",     StringType(),    True),
    StructField("platform_fee_vnd",   IntegerType(),   True),
    StructField("driver_earning_vnd", IntegerType(),   True),
])

# ─── Config ───────────────────────────────────────────────────────────────────

RAW_ROOT       = "/opt/spark-data/raw"
MINIO_ENDPOINT = "http://minio:9000"
BRONZE_BUCKET  = "s3a://rideflow/bronze"

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
    Bronze Layer: Keep as-is. Data quality checks are moved to Silver Layer.
    """
    return df

def ingest_payments(target_date: str, minio_key: str, minio_secret: str):
    """Main ingestion logic."""
    jsonl_path = partition_path("payments", target_date)
    
    print(f"[ingest_payments] Target Date: {target_date}")
    print(f"[ingest_payments] JSONL path: {jsonl_path}")
    
    if not jsonl_path.exists():
        raise FileNotFoundError(f"JSONL not found: {jsonl_path}")
    
    spark = (
        SparkSession.builder
        .appName(f"rideflow_bronze_payments_{target_date.replace('-', '')}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", minio_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"[ingest_payments] Reading JSONL...")
    df = spark.read.json(str(jsonl_path), schema=PAYMENTS_SCHEMA)
    
    initial_count = df.count()
    print(f"[ingest_payments] Initial records: {initial_count}")
    
    df = validate_and_clean(df)
    valid_count = df.count()
    print(f"[ingest_payments] Valid records: {valid_count}")
    print(f"[ingest_payments] Dropped: {initial_count - valid_count}")
    
    if valid_count == 0:
        print("[ingest_payments] No valid records — skipping write")
        spark.stop()
        return
    
    df = (df
          .withColumn("_ingested_at",  current_timestamp())
          .withColumn("_batch_id",     lit(target_date))
          .withColumn("_source",       lit("simulator"))
          .withColumn("ingest_date",   lit(target_date))
    )
    
    bronze_path = f"{BRONZE_BUCKET}/payments"
    
    print(f"[ingest_payments] Writing to Delta: {bronze_path}")
    print(f"[ingest_payments] Partition: date={target_date}")
    
    (df.write
       .format("delta")
       .mode("append")
       .partitionBy("ingest_date")
       .save(bronze_path)
    )
    
    print(f"[ingest_payments] ✅ Written {valid_count} rows")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-date", required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--minio-key", default="minioadmin")
    parser.add_argument("--minio-secret", default="minioadmin123")
    args = parser.parse_args()
    
    try:
        ingest_payments(args.target_date, args.minio_key, args.minio_secret)
    except Exception as e:
        print(f"[ingest_payments] ❌ ERROR: {e}", file=sys.stderr)
        sys.exit(1)
