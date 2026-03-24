import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# ─── Schema Definition ────────────────────────────────────────────────────────

RATINGS_SCHEMA = StructType([
    StructField("rating_id",  StringType(),    True),
    StructField("trip_id",    StringType(),    True),
    StructField("rater_id",   StringType(),    True),
    StructField("ratee_id",   StringType(),    True),
    StructField("rater_type", StringType(),    True),
    StructField("ratee_type", StringType(),    True),
    StructField("stars",      IntegerType(),   True),
    StructField("tags",       StringType(),    True),
    StructField("rated_at",   TimestampType(), True),
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

def ingest_ratings(target_date: str, minio_key: str, minio_secret: str):
    """Main ingestion logic."""
    jsonl_path = partition_path("ratings", target_date)
    
    print(f"[ingest_ratings] Target Date: {target_date}")
    print(f"[ingest_ratings] JSONL path: {jsonl_path}")
    
    if not jsonl_path.exists():
        raise FileNotFoundError(f"JSONL not found: {jsonl_path}")
    
    spark = (
        SparkSession.builder
        .appName(f"rideflow_bronze_ratings_{target_date.replace('-', '')}")
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
    
    print(f"[ingest_ratings] Reading JSONL...")
    df = spark.read.json(str(jsonl_path), schema=RATINGS_SCHEMA)
    
    initial_count = df.count()
    print(f"[ingest_ratings] Initial records: {initial_count}")
    
    df = validate_and_clean(df)
    valid_count = df.count()
    print(f"[ingest_ratings] Valid records: {valid_count}")
    print(f"[ingest_ratings] Dropped: {initial_count - valid_count}")
    
    if valid_count == 0:
        print("[ingest_ratings] No valid records — skipping write")
        spark.stop()
        return
    
    df = (df
          .withColumn("_ingested_at",  current_timestamp())
          .withColumn("_batch_id",     lit(target_date))
          .withColumn("_source",       lit("simulator"))
          .withColumn("ingest_date",   lit(target_date))
    )
    
    bronze_path = f"{BRONZE_BUCKET}/ratings"
    
    print(f"[ingest_ratings] Writing to Delta: {bronze_path}")
    print(f"[ingest_ratings] Partition: date={target_date}")
    
    (df.write
       .format("delta")
       .mode("append")
       .partitionBy("ingest_date")
       .save(bronze_path)
    )
    
    print(f"[ingest_ratings] ✅ Written {valid_count} rows")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-date", required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--minio-key", default="minioadmin")
    parser.add_argument("--minio-secret", default="minioadmin123")
    args = parser.parse_args()
    
    try:
        ingest_ratings(args.target_date, args.minio_key, args.minio_secret)
    except Exception as e:
        print(f"[ingest_ratings] ❌ ERROR: {e}", file=sys.stderr)
        sys.exit(1)
