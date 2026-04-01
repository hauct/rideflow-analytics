"""
export_gold_postgres.py
-----------------------
Read Gold layer tables from MinIO (Delta Lake) and write them to PostgreSQL.
Since DBT incremental updates MinIO tables, we overwrite the Postgres tables 
to keep them perfectly in sync for the presentation/BI layer.
"""

import sys
import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser(description="Export Gold Layer from MinIO to Postgres")
    parser.add_argument("--postgresql-url", type=str, required=True, help="JDBC URL for PostgreSQL")
    parser.add_argument("--postgresql-user", type=str, required=True, help="PostgreSQL user")
    parser.add_argument("--postgresql-password", type=str, required=True, help="PostgreSQL password")
    args = parser.parse_args()

    # Khởi tạo SparkSession
    spark = SparkSession.builder \
        .appName("ExportGoldToPostgres") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    print("[export_gold_postgres] Cấu hình PostgreSQL:", args.postgresql_url)
    
    tables_to_export = [
        "fact_trips",
        "mart_daily_revenue",
        "mart_driver_performance"
    ]
    
    for table in tables_to_export:
        s3_path = f"s3a://rideflow/gold/{table}"
        try:
            print(f"[export_gold_postgres] Đọc bảng {table} từ {s3_path}...")
            df = spark.read.format("delta").load(s3_path)
            
            # Ghi vào PostgreSQL (Overwrite = đồng bộ hoàn toàn với MinIO)
            print(f"[export_gold_postgres] Ghi {df.count()} dòng vào database PostgreSQL, bảng: {table}...")
            df.write \
                .format("jdbc") \
                .option("url", args.postgresql_url) \
                .option("driver", "org.postgresql.Driver") \
                .option("dbtable", table) \
                .option("user", args.postgresql_user) \
                .option("password", args.postgresql_password) \
                .mode("overwrite") \
                .save()
            print(f"[export_gold_postgres] ✅ Export bảng {table} thành công.")
        except Exception as e:
            print(f"[export_gold_postgres] ⚠️ Lỗi khi export bảng {table}: {e}")

    spark.stop()

if __name__ == "__main__":
    main()
