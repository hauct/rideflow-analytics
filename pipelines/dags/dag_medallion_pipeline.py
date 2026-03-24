"""
dag_medallion_pipeline.py
-----------------------
Pipeline: Simulator → Bronze Delta Lake (Raw) → Silver Delta Lake (Cleansed)

Lý do dùng BashOperator + docker exec thay SparkSubmitOperator:
  - SparkSubmitOperator cần spark-submit binary trong Airflow container
  - Airflow container không có Java/Spark → JAVA_HOME not set → fail
  - docker exec de-spark-master spark-submit → chạy trực tiếp trong Spark container
  - Cần /var/run/docker.sock mount trong docker-compose (đã có sẵn)
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# ─── Config ───────────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":             "rideflow",
    "depends_on_past":   False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}

SIMULATOR_PATH = Variable.get(
    "SIMULATOR_PATH",
    default_var="/opt/airflow/data/generators/simulator.py"
)
MINIO_KEY      = Variable.get("MINIO_ACCESS_KEY",  default_var="minioadmin")
MINIO_SECRET   = Variable.get("MINIO_SECRET_KEY",  default_var="minioadmin123")
MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT",    default_var="http://minio:9000")

# Path của python scripts bên trong Spark container
# (mount từ ./pipelines → /opt/spark-apps)
INGEST_TRIPS_SCRIPT    = "/opt/spark-apps/bronze/ingest_trips.py"
INGEST_PAYMENTS_SCRIPT = "/opt/spark-apps/bronze/ingest_payments.py"
INGEST_RATINGS_SCRIPT  = "/opt/spark-apps/bronze/ingest_ratings.py"

CLEANSE_TRIPS_SCRIPT    = "/opt/spark-apps/silver/cleanse_trips.py"
CLEANSE_PAYMENTS_SCRIPT = "/opt/spark-apps/silver/cleanse_payments.py"
CLEANSE_RATINGS_SCRIPT  = "/opt/spark-apps/silver/cleanse_ratings.py"

SPARK_MASTER           = "spark://spark-master:7077"


# ─── Helper: import simulator ─────────────────────────────────────────────────

def _get_simulator():
    """Load simulator module từ SIMULATOR_PATH dùng importlib."""
    import importlib.util
    spec   = importlib.util.spec_from_file_location("simulator", SIMULATOR_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ─── Tasks ────────────────────────────────────────────────────────────────────

def check_simulator_state(**context):
    sim = _get_simulator()
    if Path(sim.STATE_FILE).exists():
        print(f"[check_state] ✅ State found: {sim.STATE_FILE}")
        return "run_simulator"
    print(f"[check_state] ⚠️  No state — need init")
    return "init_simulator"


def init_simulator(**context):
    sim   = _get_simulator()
    state = sim.init_state(n_drivers=500, n_riders=2000)
    sim.save_state(state)
    print(f"[init_simulator] ✅ Pool created: "
          f"{len(state['drivers'])} drivers, {len(state['riders'])} riders")


def run_simulator(**context):
    """Gọi run_daily_batch() trực tiếp — đọc target_date từ state và tiến thêm 1 ngày."""
    sim          = _get_simulator()

    result = sim.run_daily_batch()
    target_date = result["target_date"]

    print(f"[run_simulator] Target Date: {target_date}")
    print(f"[run_simulator] ✅ "
          f"{result['trips']} trips | "
          f"{result['payments']} payments | "
          f"{result['ratings']} ratings")

    context["ti"].xcom_push(key="target_date", value=target_date)


def log_summary(**context):
    import psycopg2, os
    target_date = context["ti"].xcom_pull(
        key="target_date", task_ids="run_simulator"
    )
    print(f"[log_summary] target_date={target_date}")
    try:
        conn = psycopg2.connect(
            host    =os.getenv("POSTGRES_HOST",     "postgres"),
            port    =os.getenv("POSTGRES_PORT",     "5432"),
            dbname  =os.getenv("POSTGRES_DB",       "rideflow"),
            user    =os.getenv("POSTGRES_USER",     "dataengineer"),
            password=os.getenv("POSTGRES_PASSWORD", "dataengineer123"),
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id           SERIAL PRIMARY KEY,
                run_at       TIMESTAMP DEFAULT NOW(),
                target_date  VARCHAR(30),
                dag_id       VARCHAR(100),
                status       VARCHAR(20)
            )
        """)
        cur.execute(
            "INSERT INTO pipeline_runs (target_date, dag_id, status) VALUES (%s, %s, %s)",
            (target_date, "rideflow_medallion_pipeline", "success")
        )
        conn.commit()
        cur.close(); conn.close()
        print("[log_summary] ✅ Logged")
    except Exception as e:
        print(f"[log_summary] ⚠️  {e}")


# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="rideflow_medallion_pipeline",
    description="Simulator → Bronze Delta Lake (MinIO) → Silver Delta Lake",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 20),
    schedule_interval="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["rideflow", "bronze", "silver", "spark"],
) as dag:

    t_check = BranchPythonOperator(
        task_id="check_simulator_state",
        python_callable=check_simulator_state,
    )

    t_init = PythonOperator(
        task_id="init_simulator",
        python_callable=init_simulator,
    )

    t_simulate = PythonOperator(
        task_id="run_simulator",
        python_callable=run_simulator,
        trigger_rule="none_failed",
    )

    # ── Spark Submit Template ────────────────────────────────────────────
    SPARK_SUBMIT_CMD = """
TARGET_DATE="{{ ti.xcom_pull(key='target_date', task_ids='run_simulator') }}"

docker exec de-spark-master \
  /opt/spark/bin/spark-submit \
    --master {{ params.spark_master }} \
    --deploy-mode client \
    --driver-memory 512m \
    --executor-memory 512m \
    --executor-cores 1 \
    --conf spark.hadoop.fs.s3a.endpoint={{ params.minio_endpoint }} \
    --conf spark.hadoop.fs.s3a.access.key={{ params.minio_key }} \
    --conf spark.hadoop.fs.s3a.secret.key={{ params.minio_secret }} \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    {{ params.script }} \
    --target-date "$TARGET_DATE" \
    --minio-key {{ params.minio_key }} \
    --minio-secret {{ params.minio_secret }}
"""

    def create_spark_task(task_id: str, script_path: str):
        return BashOperator(
            task_id=task_id,
            bash_command=SPARK_SUBMIT_CMD,
            params={
                "spark_master":   SPARK_MASTER,
                "minio_endpoint": MINIO_ENDPOINT,
                "minio_key":      MINIO_KEY,
                "minio_secret":   MINIO_SECRET,
                "script":  script_path,
            },
            trigger_rule="none_failed",
        )

    # Bronze Layer Tasks
    t_ingest_trips = create_spark_task("ingest_trips_bronze", INGEST_TRIPS_SCRIPT)
    t_ingest_payments = create_spark_task("ingest_payments_bronze", INGEST_PAYMENTS_SCRIPT)
    t_ingest_ratings = create_spark_task("ingest_ratings_bronze", INGEST_RATINGS_SCRIPT)

    # Silver Layer Tasks
    t_cleanse_trips = create_spark_task("cleanse_trips_silver", CLEANSE_TRIPS_SCRIPT)
    t_cleanse_payments = create_spark_task("cleanse_payments_silver", CLEANSE_PAYMENTS_SCRIPT)
    t_cleanse_ratings = create_spark_task("cleanse_ratings_silver", CLEANSE_RATINGS_SCRIPT)

    t_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
        trigger_rule="all_done",
    )

    # ── Dependencies ────────────────────────────────────────────────────────
    t_check >> [t_init, t_simulate]
    t_init  >> t_simulate

    # Simulate -> Bronze
    t_simulate >> [t_ingest_trips, t_ingest_payments, t_ingest_ratings]
    
    # Bronze -> Silver (Từng domain sẽ chảy data độc lập, Trip Ingest xong sẽ kéo Trip Cleanse ngay)
    t_ingest_trips >> t_cleanse_trips
    t_ingest_payments >> t_cleanse_payments
    t_ingest_ratings >> t_cleanse_ratings

    # Tất cả Silver hoàn tất -> Ghi Log Summary
    [t_cleanse_trips, t_cleanse_payments, t_cleanse_ratings] >> t_summary
