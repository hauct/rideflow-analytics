# ==============================================================================
# RIDEFLOW ANALYTICS — Makefile
# ==============================================================================

.PHONY: help init dirs build build-spark build-airflow build-jupyter build-nc \
        up up-core up-spark up-airflow up-all up-streaming up-tools up-monitor \
        down restart restart-airflow restart-spark restart-jupyter \
        status ps logs logs-airflow logs-spark logs-jupyter \
        airflow-url metabase-url jupyter-url spark-url minio-url \
        shell-airflow shell-jupyter shell-spark shell-postgres \
        sim-run sim-backfill sim-reset sim-status \
        dag-trigger dag-list dag-unpause dag-pause \
        dbt-run dbt-test dbt-docs dbt-debug \
        db-connect db-pipeline-runs db-backup db-reset \
        clean-bronze clean-data clean-infra clean-images clean-all prune \
        spark-submit pyspark-shell check-env version

_R := $(shell printf '\033[31m')
_G := $(shell printf '\033[32m')
_Y := $(shell printf '\033[33m')
_B := $(shell printf '\033[34m')
_C := $(shell printf '\033[36m')
_0 := $(shell printf '\033[0m')

-include .env
export

SPARK_CTR   := de-spark-master
AIRFLOW_SCH := de-airflow-scheduler
POSTGRES    := de-postgres
JUPYTER     := de-jupyter-spark

# ==============================================================================
# HELP
# ==============================================================================
help:
	@printf '\n$(_C)╔══════════════════════════════════════════════════╗$(_0)\n'
	@printf '$(_C)║         RIDEFLOW ANALYTICS — Commands             ║$(_0)\n'
	@printf '$(_C)╚══════════════════════════════════════════════════╝$(_0)\n\n'
	@printf '$(_Y)🚀 First time setup:$(_0)\n'
	@printf '  $(_G)make init$(_0)          → tạo dirs + .env\n'
	@printf '  $(_G)make build$(_0)         → build Spark + Airflow + Jupyter images\n'
	@printf '  $(_G)make up$(_0)            → start toàn bộ stack\n\n'
	@printf '$(_Y)📦 Build images:$(_0)\n'
	@printf '  $(_G)make build$(_0)         → build tất cả (no-cache)\n'
	@printf '  $(_G)make build-spark$(_0)   → chỉ Spark\n'
	@printf '  $(_G)make build-airflow$(_0) → chỉ Airflow\n'
	@printf '  $(_G)make build-jupyter$(_0) → chỉ Jupyter\n'
	@printf '  $(_G)make build-nc$(_0)      → build nhanh (có cache)\n\n'
	@printf '$(_Y)▶️  Start:$(_0)\n'
	@printf '  $(_G)make up$(_0)            → tất cả services\n'
	@printf '  $(_G)make up-core$(_0)       → nhẹ: Postgres + MinIO\n'
	@printf '  $(_G)make up-spark$(_0)      → thêm Spark + Jupyter\n'
	@printf '  $(_G)make up-airflow$(_0)    → thêm Airflow\n'
	@printf '  $(_G)make up-streaming$(_0)  → thêm Kafka\n'
	@printf '  $(_G)make up-tools$(_0)      → PgAdmin\n'
	@printf '  $(_G)make up-monitor$(_0)    → Spark History\n\n'
	@printf '$(_Y)⏹️  Stop / Restart:$(_0)\n'
	@printf '  $(_G)make down$(_0)  $(_G)make restart$(_0)  $(_G)make restart-airflow$(_0)\n\n'
	@printf '$(_Y)🔍 Monitor:$(_0)\n'
	@printf '  $(_G)make status$(_0)        → containers + URLs\n'
	@printf '  $(_G)make logs-airflow$(_0)  → scheduler logs\n'
	@printf '  $(_G)make logs-spark$(_0)\n\n'
	@printf '$(_Y)🚗 Simulator:$(_0)\n'
	@printf '  $(_G)make sim-run$(_0)              → 1 window\n'
	@printf '  $(_G)make sim-backfill N=96$(_0)    → 1 ngày data\n'
	@printf '  $(_G)make sim-status$(_0)\n\n'
	@printf '$(_Y)🔁 Airflow DAGs:$(_0)\n'
	@printf '  $(_G)make dag-list$(_0)\n'
	@printf '  $(_G)make dag-trigger DAG=rideflow_bronze_ingestion$(_0)\n'
	@printf '  $(_G)make dag-unpause DAG=rideflow_bronze_ingestion$(_0)\n\n'
	@printf '$(_Y)📦 dbt:$(_0)\n'
	@printf '  $(_G)make dbt-run  dbt-test  dbt-docs$(_0)\n\n'
	@printf '$(_Y)🗄️  Database:$(_0)\n'
	@printf '  $(_G)make db-connect$(_0)         → psql\n'
	@printf '  $(_G)make db-pipeline-runs$(_0)   → monitoring\n'
	@printf '  $(_G)make db-backup$(_0)\n\n'
	@printf '$(_Y)🧹 Clean:$(_0)\n'
	@printf '  $(_G)make clean-bronze$(_0)              → JSONL files\n'
	@printf '  $(_G)make clean-data LAYER=bronze$(_0)   → Delta layer\n'
	@printf '  $(_G)make clean-infra$(_0)               → containers+volumes\n'
	@printf '  $(_G)make clean-all$(_0)                 → everything\n\n'

# ==============================================================================
# INIT
# ==============================================================================
init: dirs
	@[ -f .env ] || (cp .env.example .env && printf '$(_Y)⚠️  .env created — please review$(_0)\n')
	@printf '$(_G)✅ Ready. Next: make build → make up$(_0)\n'

dirs:
	@printf '$(_B)📁 Creating directories...$(_0)\n'
	@mkdir -p infrastructure/{postgres/init,jupyter,spark,airflow}
	@mkdir -p data/{generators,raw/trips,raw/payments,raw/ratings}
	@mkdir -p pipelines/{dags,bronze,silver,streaming}
	@mkdir -p dbt/models/{staging,intermediate,marts} dbt/tests
	@mkdir -p ai/{forecasting,anomaly} dashboards docs notebooks tests shared scripts
	@printf '$(_G)✅ Done$(_0)\n'

# ==============================================================================
# BUILD  ← phải chạy trước make up lần đầu
# ==============================================================================
build:
	@printf '$(_B)🔨 Building all images (no-cache)...$(_0)\n'
	docker compose build --no-cache spark-master airflow-init jupyter-spark
	@printf '$(_G)✅ Build complete$(_0)\n'

build-spark:
	docker compose build --no-cache spark-master
	@printf '$(_G)✅ Spark done$(_0)\n'

build-airflow:
	docker compose build --no-cache airflow-init
	@printf '$(_G)✅ Airflow done$(_0)\n'

build-jupyter:
	docker compose build --no-cache jupyter-spark
	@printf '$(_G)✅ Jupyter done$(_0)\n'

build-nc:
	docker compose build spark-master airflow-init jupyter-spark

# ==============================================================================
# STARTUP
# ==============================================================================
up:
	@printf '$(_B)▶️  Starting RideFlow...$(_0)\n'
	docker compose up -d
	@sleep 3
	@$(MAKE) --no-print-directory status

up-core:
	docker compose up -d postgres minio minio-client

up-spark:
	docker compose up -d spark-master spark-worker-1 spark-worker-2 jupyter-spark

up-airflow:
	@printf '$(_B)▶️  Starting Airflow...$(_0)\n'
	docker compose up -d airflow-init
	@printf '  Waiting for init (~20s)...\n'
	@sleep 20
	docker compose up -d airflow-webserver airflow-scheduler
	@printf '$(_G)✅ http://localhost:%s  (%s/%s)$(_0)\n' "${AIRFLOW_PORT}" "${AIRFLOW_USER}" "${AIRFLOW_PASSWORD}"

up-streaming:
	docker compose --profile streaming up -d

up-tools:
	docker compose --profile tools up -d

up-monitor:
	docker compose --profile monitoring up -d

up-all:
	docker compose --profile streaming --profile tools --profile monitoring up -d
	@$(MAKE) --no-print-directory status

# ==============================================================================
# SHUTDOWN
# ==============================================================================
down:
	docker compose --profile streaming --profile tools --profile monitoring down
	@printf '$(_G)✅ Stopped$(_0)\n'

restart: down up

restart-airflow:
	docker compose restart airflow-webserver airflow-scheduler

restart-spark:
	docker compose restart spark-master spark-worker-1 spark-worker-2 spark-history

restart-jupyter:
	docker compose restart jupyter-spark

# ==============================================================================
# MONITORING
# ==============================================================================
status:
	@printf '\n$(_C)╔═══════════════════════════════════════╗$(_0)\n'
	@printf '$(_C)║         RIDEFLOW — STATUS              ║$(_0)\n'
	@printf '$(_C)╚═══════════════════════════════════════╝$(_0)\n'
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || docker compose ps
	@printf '\n$(_Y)🌐 URLs:$(_0)\n'
	@printf '  Airflow  → http://localhost:%s  (%s/%s)\n' "${AIRFLOW_PORT}"        "${AIRFLOW_USER}"    "${AIRFLOW_PASSWORD}"
	@printf '  Metabase → http://localhost:%s\n'          "${METABASE_PORT}"
	@printf '  Jupyter  → http://localhost:%s  (token: %s)\n' "${JUPYTER_PORT}"   "${JUPYTER_TOKEN}"
	@printf '  Spark    → http://localhost:%s\n'          "${SPARK_MASTER_UI_PORT}"
	@printf '  MinIO    → http://localhost:%s  (%s/%s)\n\n' "${MINIO_UI_PORT}"     "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

ps:
	docker compose ps

logs:
	docker compose logs -f

logs-airflow:
	docker compose logs -f airflow-scheduler

logs-webserver:
	docker compose logs -f airflow-webserver

logs-spark:
	docker compose logs -f spark-master spark-worker-1 spark-worker-2

logs-jupyter:
	docker compose logs -f jupyter-spark

# ==============================================================================
# URLS
# ==============================================================================
airflow-url:
	@printf 'http://localhost:%s  (%s/%s)\n' "${AIRFLOW_PORT}" "${AIRFLOW_USER}" "${AIRFLOW_PASSWORD}"
metabase-url:
	@printf 'http://localhost:%s\n' "${METABASE_PORT}"
jupyter-url:
	@printf 'http://localhost:%s?token=%s\n' "${JUPYTER_PORT}" "${JUPYTER_TOKEN}"
spark-url:
	@printf 'http://localhost:%s\n' "${SPARK_MASTER_UI_PORT}"
minio-url:
	@printf 'http://localhost:%s  (%s/%s)\n' "${MINIO_UI_PORT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

# ==============================================================================
# SHELL
# ==============================================================================
shell-airflow:
	docker exec -it $(AIRFLOW_SCH) bash
shell-jupyter:
	docker exec -it $(JUPYTER) bash
shell-spark:
	docker exec -it $(SPARK_CTR) bash
shell-postgres:
	docker exec -it $(POSTGRES) psql -U ${POSTGRES_USER} -d ${POSTGRES_DB}

# ==============================================================================
# SIMULATOR
# ==============================================================================
sim-run:
	@printf '$(_B)🚗 1 window...$(_0)\n'
	python data/generators/simulator.py

sim-backfill:
	@printf '$(_B)🚗 Backfilling ${N:-32} windows...$(_0)\n'
	python data/generators/simulator.py --backfill --windows ${N:-32}

sim-reset:
	@rm -f data/simulator_state.json
	@printf '$(_G)✅ State cleared$(_0)\n'

sim-status:
	@if [ -f data/simulator_state.json ]; then \
		python3 -c "\
import json; s=json.load(open('data/simulator_state.json')); \
print(f'  Trips total: {s[\"total_trips\"]:,}'); \
print(f'  Drivers    : {len(s[\"drivers\"])}'); \
print(f'  Riders     : {len(s[\"riders\"])}'); \
print(f'  Last run   : {s.get(\"last_run_at\",\"never\")}'); \
"; \
	else \
		printf '$(_Y)No state. Run: make sim-run$(_0)\n'; \
	fi

# ==============================================================================
# AIRFLOW DAGs
# ==============================================================================
dag-list:
	docker exec $(AIRFLOW_SCH) airflow dags list

dag-trigger:
	@[ -n "$(DAG)" ] || (printf '$(_R)❌ Usage: make dag-trigger DAG=<id>$(_0)\n' && exit 1)
	docker exec $(AIRFLOW_SCH) airflow dags trigger $(DAG)
	@printf '$(_G)✅ Triggered: %s$(_0)\n' "$(DAG)"

dag-unpause:
	@[ -n "$(DAG)" ] || (printf '$(_R)❌ Usage: make dag-unpause DAG=<id>$(_0)\n' && exit 1)
	docker exec $(AIRFLOW_SCH) airflow dags unpause $(DAG)

dag-pause:
	@[ -n "$(DAG)" ] || (printf '$(_R)❌ Usage: make dag-pause DAG=<id>$(_0)\n' && exit 1)
	docker exec $(AIRFLOW_SCH) airflow dags pause $(DAG)

# ==============================================================================
# DBT
# ==============================================================================
dbt-run:
	docker exec $(JUPYTER) bash -c "cd /opt/dbt && dbt run"
dbt-test:
	docker exec $(JUPYTER) bash -c "cd /opt/dbt && dbt test"
dbt-docs:
	docker exec $(JUPYTER) bash -c "cd /opt/dbt && dbt docs generate && dbt docs serve --port 8081 --no-browser &"
	@printf '$(_G)✅ http://localhost:8081$(_0)\n'
dbt-debug:
	docker exec $(JUPYTER) bash -c "cd /opt/dbt && dbt debug"

# ==============================================================================
# DATABASE
# ==============================================================================
db-connect:
	docker exec -it $(POSTGRES) psql -U ${POSTGRES_USER} -d ${POSTGRES_DB}

db-pipeline-runs:
	@docker exec $(POSTGRES) psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} \
		-c "SELECT window_start, dag_id, status, run_at FROM pipeline_runs ORDER BY run_at DESC LIMIT 20;" \
		2>/dev/null || printf '$(_Y)pipeline_runs not created yet$(_0)\n'

db-backup:
	@mkdir -p backups
	@TS=$$(date +%Y%m%d_%H%M%S); \
	docker exec $(POSTGRES) pg_dump -U ${POSTGRES_USER} ${POSTGRES_DB} > backups/rideflow_$$TS.sql; \
	printf '$(_G)✅ backups/rideflow_%s.sql$(_0)\n' "$$TS"

db-reset:
	@printf '$(_R)⚠️  Delete ALL data in %s?$(_0)\n' "${POSTGRES_DB}"
	@read -p "yes/no: " c && [ "$$c" = "yes" ] || exit 1
	docker exec $(POSTGRES) psql -U ${POSTGRES_USER} \
		-c "DROP DATABASE IF EXISTS ${POSTGRES_DB};" \
		-c "CREATE DATABASE ${POSTGRES_DB};"
	@printf '$(_G)✅ Done$(_0)\n'

# ==============================================================================
# CLEAN
# ==============================================================================
clean-bronze:
	@printf '$(_Y)⚠️  Delete raw JSONL files?$(_0)\n'
	@read -p "yes/no: " c && [ "$$c" = "yes" ] || exit 1
	@rm -rf data/raw/trips/ data/raw/payments/ data/raw/ratings/
	@mkdir -p data/raw/trips data/raw/payments data/raw/ratings
	@printf '$(_G)✅ Done$(_0)\n'

clean-data:
	@[ -n "$(LAYER)" ] || (printf '$(_R)❌ Usage: make clean-data LAYER=bronze|silver|gold|all$(_0)\n' && exit 1)
	@printf '$(_Y)⚠️  Delete MinIO rideflow/%s/?$(_0)\n' "$(LAYER)"
	@read -p "yes/no: " c && [ "$$c" = "yes" ] || exit 1
	@TARGETS="$(LAYER)"; [ "$(LAYER)" = "all" ] && TARGETS="bronze silver gold"; \
	for l in $$TARGETS; do \
		docker run --rm --network de_network minio/mc:latest /bin/sh -c \
			"mc alias set m http://de-minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} --api S3v4 >/dev/null \
			 && mc rm --recursive --force m/rideflow/$$l/ 2>/dev/null; echo $$l cleared"; \
	done
	@printf '$(_G)✅ Cleaned$(_0)\n'

clean-infra:
	@printf '$(_R)⚠️  Delete containers + volumes?$(_0)\n'
	@read -p "yes/no: " c && [ "$$c" = "yes" ] || exit 1
	docker compose --profile streaming --profile tools --profile monitoring down -v --remove-orphans
	@printf '$(_G)✅ Done$(_0)\n'

clean-images:
	docker rmi de-spark:3.5.1 de-airflow:2.11.2 de-jupyter-pyspark:latest 2>/dev/null || true
	@printf '$(_G)✅ Images removed$(_0)\n'

clean-all: clean-infra clean-images
	docker system prune -f
	@printf '$(_G)✅ Full cleanup$(_0)\n'

prune:
	docker system prune -f

# ==============================================================================
# SPARK
# ==============================================================================
spark-submit:
	@[ -n "$(APP)" ] || (printf '$(_R)❌ Usage: make spark-submit APP=pipelines/bronze/myjob.py$(_0)\n' && exit 1)
	docker exec $(SPARK_CTR) /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 /opt/spark-apps/$(APP)

pyspark-shell:
	docker exec -it $(SPARK_CTR) /opt/spark/bin/pyspark \
		--master spark://spark-master:7077

# ==============================================================================
# UTILS
# ==============================================================================
check-env:
	@[ -f .env ] && printf '$(_G)✅ .env found$(_0)\n' || printf '$(_R)❌ .env missing — run: make init$(_0)\n'
	@printf 'POSTGRES_DB=%s  AIRFLOW_PORT=%s  MINIO_ROOT_USER=%s\n' \
		"${POSTGRES_DB}" "${AIRFLOW_PORT}" "${MINIO_ROOT_USER}"

version:
	@docker --version && docker compose version
