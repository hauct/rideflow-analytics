"""
dag_bronze_ingestion.py
-----------------------
Pipeline: Simulator → Bronze Delta Lake (MinIO)

Lý do dùng BashOperator + docker exec thay SparkSubmitOperator:
  - SparkSubmitOperator cần spark-submit binary trong Airflow container
  - Airflow container không có Java/Spark → JAVA_HOME not set → fail
  - docker exec de-spark-master spark-submit → chạy trực tiếp trong Spark container
  - Cần /var/run/docker.sock mount trong docker-compose (đã có sẵn)

Fix trong version này:
  - Dùng BashOperator + docker exec (pattern đúng với setup này)
  - Fix window argument: dùng quotes đúng để "2026-03-22 09:15" không bị tách
  - Import simulator trực tiếp (không dùng subprocess --sim-time)
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

# Path của ingest_trips.py bên trong Spark container
# (mount từ ./pipelines → /opt/spark-apps)
INGEST_SCRIPT  = "/opt/spark-apps/bronze/ingest_trips.py"
SPARK_MASTER   = "spark://spark-master:7077"


# ─── Helper: import simulator ─────────────────────────────────────────────────

def _get_simulator():
    """Load simulator module từ SIMULATOR_PATH dùng importlib."""
    import importlib.util
    spec   = importlib.util.spec_from_file_location("simulator", SIMULATOR_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _window_from_context(context) -> datetime:
    """Round data_interval_start về mốc 15 phút."""
    ts     = context["data_interval_start"]
    minute = (ts.minute // 15) * 15
    return ts.replace(minute=minute, second=0, microsecond=0)


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
    """Gọi run_single_window() trực tiếp — không subprocess, không argument."""
    sim          = _get_simulator()
    window_start = _window_from_context(context)
    window_str   = window_start.strftime("%Y-%m-%d %H:%M")

    print(f"[run_simulator] Window: {window_str}")
    result = sim.run_single_window(window_start=window_start)
    print(f"[run_simulator] ✅ "
          f"{result['trips']} trips | "
          f"{result['payments']} payments | "
          f"{result['ratings']} ratings")

    context["ti"].xcom_push(key="window_start", value=window_str)


def log_summary(**context):
    import psycopg2, os
    window_str = context["ti"].xcom_pull(
        key="window_start", task_ids="run_simulator"
    )
    print(f"[log_summary] window={window_str}")
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
                window_start VARCHAR(30),
                dag_id       VARCHAR(100),
                status       VARCHAR(20)
            )
        """)
        cur.execute(
            "INSERT INTO pipeline_runs (window_start, dag_id, status) VALUES (%s, %s, %s)",
            (window_str, "rideflow_bronze_ingestion", "success")
        )
        conn.commit()
        cur.close(); conn.close()
        print("[log_summary] ✅ Logged")
    except Exception as e:
        print(f"[log_summary] ⚠️  {e}")


# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="rideflow_bronze_ingestion",
    description="Simulator → JSONL → Bronze Delta Lake (MinIO)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 20),
    schedule_interval="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["rideflow", "bronze", "spark"],
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

    # ── BashOperator + docker exec ────────────────────────────────────────────
    # Chạy spark-submit bên trong Spark container (có đủ Java + JARs).
    # FIX: window value có space → phải wrap bằng single quotes trong bash
    #      "2026-03-22 09:15" nếu không quote sẽ bị tách thành 2 args
    t_ingest_trips = BashOperator(
        task_id="ingest_trips_spark",
        bash_command="""
WINDOW="{{ ti.xcom_pull(key='window_start', task_ids='run_simulator') }}"

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
    {{ params.ingest_script }} \
    --window "$WINDOW" \
    --minio-key {{ params.minio_key }} \
    --minio-secret {{ params.minio_secret }}
""",
        params={
            "spark_master":   SPARK_MASTER,
            "minio_endpoint": MINIO_ENDPOINT,
            "minio_key":      MINIO_KEY,
            "minio_secret":   MINIO_SECRET,
            "ingest_script":  INGEST_SCRIPT,
        },
        trigger_rule="none_failed",
    )

    t_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
        trigger_rule="all_done",
    )

    t_check >> [t_init, t_simulate]
    t_init  >> t_simulate
    t_simulate >> t_ingest_trips >> t_summary
