import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

MINIO_ENDPOINT = "http://minio:9000"
BRONZE_BUCKET  = "s3a://rideflow/bronze"
SILVER_BUCKET  = "s3a://rideflow/silver"
QUARANTINE_BUCKET = "s3a://rideflow/silver/quarantine"

def cleanse_ratings(target_date: str, minio_key: str, minio_secret: str):
    spark = (
        SparkSession.builder
        .appName(f"rideflow_silver_ratings_{target_date.replace('-', '')}")
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

    print(f"[cleanse_ratings] Target Date: {target_date}")
    bronze_path = f"{BRONZE_BUCKET}/ratings"

    try:
        df = spark.read.format("delta").load(bronze_path).filter(col("ingest_date") == target_date)
    except Exception as e:
        print(f"[cleanse_ratings] No Bronze data found for {target_date}. Exiting.")
        return

    # 1. Deduplicate based on rating_id
    df = df.dropDuplicates(["rating_id"])
    initial_count = df.count()
    if initial_count == 0:
        print("[cleanse_ratings] No records found.")
        return

    # 2. Define Data Quality Rules
    dq_cond = (
        col("rating_id").isNotNull() & 
        col("trip_id").isNotNull() &
        col("stars").between(1, 5) &
        col("rater_type").isin(["rider", "driver"])
    )

    valid_df = df.filter(dq_cond)
    invalid_df = df.filter(~dq_cond | dq_cond.isNull())

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    print(f"[cleanse_ratings] Evaluated {initial_count} records")
    print(f"[cleanse_ratings] Valid: {valid_count} | Invalid (Quarantine): {invalid_count}")

    # 3. Add Silver Meta
    valid_df = valid_df.withColumn("_silver_at", current_timestamp())
    invalid_df = invalid_df.withColumn("_silver_at", current_timestamp()).withColumn("quarantine_reason", lit("Failed Data Quality Checks at Silver Layer"))

    # 4. Write out
    if valid_count > 0:
        silver_path = f"{SILVER_BUCKET}/ratings"
        valid_df.write.format("delta").mode("append").partitionBy("ingest_date").save(silver_path)
    
    if invalid_count > 0:
        quarantine_path = f"{QUARANTINE_BUCKET}/ratings"
        invalid_df.write.format("delta").mode("append").partitionBy("ingest_date").save(quarantine_path)
    
    print(f"[cleanse_ratings] ✅ Completed processing logic.")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-date", required=True)
    parser.add_argument("--minio-key", default="minioadmin")
    parser.add_argument("--minio-secret", default="minioadmin123")
    args = parser.parse_args()
    try:
        cleanse_ratings(args.target_date, args.minio_key, args.minio_secret)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
