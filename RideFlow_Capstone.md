# 🚗 RideFlow Analytics Platform

> Grab-style Ride-hailing & Delivery — End-to-End Data Platform  
> **16 weekends · ~4-5h/weekend · Middle-level · Portfolio-ready**

---

## 1. Business Context

Một startup ride-hailing khu vực (TP.HCM + Hà Nội) với hàng chục nghìn chuyến/ngày nhưng thiếu visibility về operations: dashboard lag 24h, cancellation cao không rõ nguyên nhân, driver/rider chỉ là số ID.

**RideFlow giải quyết:**

- Demand heatmap theo giờ + zone → dispatch thông minh
- Funnel đầy đủ: request → match → pickup → complete → rate
- Driver scorecard → incentive program data-driven
- Near real-time dashboard, cập nhật mỗi 15 phút

**Data Sources:**

| Source         | Mô tả                 | Volume/day   | Ingest            |
| -------------- | --------------------- | ------------ | ----------------- |
| 🚗 Trip events | Lifecycle 1 chuyến đi | ~50k trips   | Simulator → JSONL |
| 📱 App events  | Click, cancel, search | ~500k events | Kafka stream      |
| 💳 Payments    | Fare, promo, refund   | ~55k rows    | Từ trip events    |
| ⭐ Ratings     | Rider ↔ Driver        | ~75k rows    | Từ trip events    |
| 📍 GPS pings   | Driver location/30s   | ~200k rows   | Kafka stream      |

---

## 2. Data Architecture

### Incremental Data Flow

Mỗi 15 phút, simulator sinh một "window" data mới dạng JSONL — giống hệ thống thật đang ghi log liên tục. Airflow DAG detect file mới → ingest vào Bronze Delta → Silver → Gold.

**Simulator hoạt động có state:**

- Khởi tạo pool 500 drivers + 2000 riders, lưu vào `state.json`
- Các lần sau reuse pool cũ → rider/driver ID overlap realistic
- Thêm nhỏ giọt 0-3 drivers mới, 5-20 riders mới mỗi batch
- `trip_counter` tăng liên tục, không bao giờ trùng
- Demand tự nhiên theo giờ: rush hour 7-9h và 17-19h

**Timeline mỗi 15 phút (DAG: `bronze_trips_ingest`):**

| T+    | Task               | Chi tiết                                                                |
| ----- | ------------------ | ----------------------------------------------------------------------- |
| 0:00  | `run_simulator`    | Sinh JSONL cho window 15 phút                                           |
| 0:30  | `validate_schema`  | Kiểm tra file, JSON hợp lệ, volume 1-500 trips, completion rate > 50%   |
| 1:00  | `ingest_to_bronze` | PySpark → cast types → append Delta Bronze (partition by `_date/_hour`) |
| 4:00  | `log_metrics`      | Ghi vào Postgres `pipeline_runs` (`trigger_rule=all_done`)              |
| 6:00  | Silver DAG         | PySpark clean, dedup, enrich → append Silver Delta                      |
| 10:00 | dbt incremental    | Process rows mới → refresh Gold tables                                  |
| 12:00 | Metabase query     | Dashboard fresh trong ~12 phút so với event thực tế                     |

### Medallion Architecture

**🥉 Bronze — Raw Landing**

- JSONL → Delta Lake, append-only, không transform
- Partition: `_date / _hour` (Hive-style), retention 60 ngày
- Great Expectations: schema + volume + freshness checks
- Time travel: query data 3h trước để debug

**🥈 Silver — Clean & Conform**

- PySpark: dedup `trip_id`, cast types, mask PII (phone), validate lat/lng bounds (HCMC/HN)
- dbt staging: `stg_trips`, `stg_drivers`, `stg_riders`
- dbt tests: `not_null`, `unique`, `accepted_values`, `relationships`

**🥇 Gold — Business Ready**

- `fact_trips`: 1 row/trip (Kimball star schema)
- Dims: `dim_driver`, `dim_rider`, `dim_zone`, `dim_time`
- Marts: `daily_trip_summary`, `driver_weekly_scorecard`, `demand_heatmap`
- dbt docs: auto-gen catalog + lineage graph

### Metadata & Documentation

RideFlow implements comprehensive metadata management to ensure data discoverability, understanding, and governance:

#### **Table & Column Documentation**

Every dbt model includes:

- **Description**: Business purpose and usage guidelines
- **Column Descriptions**: Meaning, units, acceptable values, and sources
- **Data Types**: Logical types with physical implementations
- **Constraints**: Not null, unique, foreign key relationships where applicable

Example from `fact_trips.sql`:

```sql
{{ config(
    materialized='incremental',
    file_format='delta',
    location_root='s3a://rideflow/gold',
    unique_key='trip_id',
    incremental_strategy='merge'
) }}

-- Fact table containing one record per completed trip
-- Contains all metrics and dimensions needed for business analysis
-- Updated incrementally as new trip data arrives
```

#### **Data Dictionary**

Automatically generated via `dbt docs generate`:

- **Catalog**: Searchable interface showing all tables, columns, and descriptions
- **Lineage Graph**: Visual representation of data flow from source to consumption
- **Freshness Indicators**: Last updated timestamps for monitoring

#### **Log Types & Sources**

1. **Simulation Logs** (`data/raw/*`):
   - JSONL files with trip, payment, and rating events
   - Partitioned by date/hour for efficient querying
   - Includes metadata: schema version, ingestion timestamp, batch ID

2. **Pipeline Logs** (Airflow task logs):
   - Execution timestamps, durations, and resource usage
   - Error messages and stack traces for debugging
   - Performance metrics: records processed, throughput rates

3. **Quality Logs** (Great Expectations):
   - Expectation suite results: passed/failed checks
   - Detailed error rows when validation fails
   - Trend analysis of data quality over time

4. **Monitoring Logs** (Postgres `pipeline_runs` table):
   - Start/end times for each pipeline component
   - Record counts at each stage (Bronze/Silver/Gold)
   - Success/failure status with error details

5. **Audit Logs** (dbt run results):
   - Model execution status and timing
   - Test results: passed/failed/skipped
   - Resource utilization: bytes processed, computation time

### AI Add-on (Phase 4)

AI layer chỉ READ từ Gold — pipeline core hoàn toàn độc lập. Nếu AI có bug, dashboard vẫn chạy bình thường.

- **Demand Forecasting (Prophet):** Input từ `demand_heatmap` 7 ngày, output forecast zone × hour (next 24h), retrain mỗi Chủ nhật 2AM, MAPE < 20%
- **Anomaly Detection (Z-score):** Monitor `cancellation_rate` + `driver_utilization`, threshold |z| > 2.5 → alert email qua Metabase

---

## 3. Tech Stack

| Công nghệ              | Dùng cho                       | Có sẵn?       |
| ---------------------- | ------------------------------ | ------------- |
| Apache Spark 3.5.1     | ETL, PySpark transformations   | ✅            |
| Delta Lake 3.0         | ACID storage, time travel      | ✅            |
| Apache Kafka           | GPS stream, app events         | ✅            |
| MinIO                  | Object storage (S3-compatible) | ✅            |
| PostgreSQL 15          | Metadata, alerts, monitoring   | ✅            |
| Great Expectations     | Data quality gates             | ✅            |
| **Apache Airflow 2.8** | Pipeline orchestration         | ❌ Mới (W1-2) |
| **dbt Core 1.7**       | SQL transform, tests, docs     | ❌ Mới (W5-6) |
| Metabase               | BI dashboard                   | ❌ Mới (W11)  |
| Prophet                | Demand forecasting             | ❌ Mới (W13)  |

> Chỉ cần học thêm **2 thứ chính**: Airflow (~1-2 weekends) và dbt (~2 weekends).

---

## 4. 16-Weekend Sprint Plan

### Phase 1 · W1-4 — Foundation & Incremental Simulator

| Week | Focus           | Done khi                                           |
| ---- | --------------- | -------------------------------------------------- |
| W1   | Airflow basics  | Airflow UI lên, DAG Hello World chạy ✅            |
| W2   | Simulator setup | 32 JSONL files, `state.json` có 500d/2000r         |
| W3   | Bronze DAG      | DAG chạy end-to-end, Delta partition trên MinIO    |
| W4   | Hardening       | Retry, idempotency, `pipeline_runs` table verified |

### Phase 2 · W5-8 — Silver Layer & dbt Foundation

| Week | Focus            | Done khi                                                     |
| ---- | ---------------- | ------------------------------------------------------------ |
| W5   | dbt basics       | `stg_trips` pass, `dbt run + test` lần đầu                   |
| W6   | PySpark Silver   | Silver trips table, quality 99%+                             |
| W7   | dbt intermediate | `int_trip_enriched`, `int_driver_daily` + relationship tests |
| W8   | Tests + docs     | 20+ dbt tests, lineage graph visible                         |

### Phase 3 · W9-12 — Gold Layer & Dashboard

| Week | Focus                  | Done khi                                                |
| ---- | ---------------------- | ------------------------------------------------------- |
| W9   | Dimensional model      | Star schema 5 tables, ERD trong `/docs`                 |
| W10  | KPI models             | 3 Gold marts, query < 3s trên 200k rows                 |
| W11  | Metabase + Dashboard 1 | Dashboard Executive live: GMV, trips, cancellation rate |
| W12  | Dashboard 2 + 3        | Driver Scorecard + Demand Heatmap với drill-through     |

### Phase 4 · W13-16 — AI Layer + Polish + Showcase

| Week | Focus             | Done khi                                                    |
| ---- | ----------------- | ----------------------------------------------------------- |
| W13  | Demand forecast   | Forecast chart live, MAPE < 20%                             |
| W14  | Anomaly detection | Alert email demo được, 2 alert rules                        |
| W15  | GitHub + README   | Repo public, README điểm 9/10                               |
| W16  | Final demo        | Screencast 3 phút + CV bullets + 3 interview talking points |

---

## 5. Showcase & Interview Prep

### CV Bullet Points

- Built incremental data pipeline processing 50,000+ daily ride-hailing trips via stateful Python simulator (JSONL partitioned by date/hour) + Airflow 4-task DAG, delivering Bronze Delta Lake updates every 15 minutes
- Designed Medallion Architecture (Bronze/Silver/Gold) on MinIO with dbt-managed transformations and 25+ automated schema tests — established single source of truth for 8 operational KPIs including GMV, driver utilization, and cancellation rate
- Implemented demand forecasting using Prophet time-series model (MAPE < 20%) with Airflow weekly retraining pipeline; integrated anomaly detection (Z-score) with automated Metabase email alerts for ops team
- Developed 3 interactive Metabase dashboards (Executive Overview, Driver Scorecard, Demand Heatmap) with city/date drill-through capability, sourced from optimized Delta Lake Gold layer
- Maintained end-to-end data quality framework: Great Expectations at Bronze ingestion, dbt schema tests at Silver/Gold, and `pipeline_runs` monitoring table tracking freshness SLAs

### Interview Q&A

**"Walk me through your pipeline"**
Mở đầu bằng business problem → simulator sinh JSONL mỗi 15 phút (stateful) → Airflow 4-task DAG → Bronze Delta → PySpark Silver (incremental) → dbt Gold → Metabase. Nhấn vào: tại sao JSONL thay CSV, tại sao partition by hour.

**"Tại sao JSONL thay vì CSV?"**
CSV không append-friendly. JSONL: mỗi dòng là 1 JSON object độc lập, append bằng cách ghi thêm dòng. FileSensor detect file mới không cần scan toàn bộ thư mục. Schema embedded → dễ validate từng record.

**"Incremental pipeline hoạt động thế nào?"**
3 tầng: Bronze (append Delta partition mới) → Silver (PySpark chỉ đọc partition mới nhất) → dbt Gold (filter `WHERE request_time > max(request_time) IN target`). Mỗi layer chỉ process data mới.

**"Tại sao Delta Lake thay vì Parquet thuần?"**
ACID transactions (tránh dirty reads khi Airflow + Spark write đồng thời), time travel (query data 3h trước khi debug), schema evolution với `mergeSchema=true`.

**"dbt là gì, tại sao không dùng Spark thôi?"**
Spark giỏi heavy lifting. dbt giỏi business logic bằng SQL + built-in testing + auto-generated documentation + lineage graph — thứ Spark không có.

**"Project nhỏ so với production thật"**
Đúng — đây là local dev với synthetic data. Nhưng pattern giống production: stateful incremental ingestion, partitioned storage, layered architecture. Scale path rõ ràng: MinIO → S3, local Airflow → MWAA, local Spark → EMR.

---

## 6. Metadata & Logging Details

### Table Documentation Standards

All dbt models in RideFlow follow these documentation standards:

#### **Model Header**

```sql
{{ config(...) }}

-- [Business purpose description]
-- [Key characteristics: granularity, update frequency, retention]
-- [Primary use cases: dashboards, ML features, operational monitoring]
-- [Important limitations or assumptions]
```

#### **Column Documentation**

Each SELECT column includes inline comments:

```sql
SELECT
    trip_id,                    -- Unique identifier for the trip
    request_time,              -- When customer requested the ride (UTC)
    -- ... other columns
```

#### **Example: dim_driver.sql**

```sql
{{ config(
    materialized='incremental',
    file_format='delta',
    location_root='s3a://rideflow/gold',
    unique_key='driver_id',
    incremental_strategy='merge'
) }}

-- Slowly changing dimension (Type 1) for driver attributes
-- Contains current state of driver profile information
-- Updated when driver data changes in source systems

WITH driver_data AS (
    SELECT
        driver_id,
        city,
        vehicle_type,
        tier,
        avg_rating,
        is_active,
        online_hours,
        ingest_date
    FROM {{ ref('stg_trips') }}
    {% if is_incremental() %}
    WHERE ingest_date >= (
        SELECT COALESCE(MAX(ingest_date), '1970-01-01')
        FROM {{ this }}
    )
    {% endif %}
),
driver_latest AS (
    SELECT
        driver_id,
        city,
        vehicle_type,
        tier,
        avg_rating,
        is_active,
        online_hours,
        MAX(ingest_date) as last_updated
    FROM driver_data
    GROUP BY driver_id, city, vehicle_type, tier, avg_rating, is_active, online_hours
)
SELECT
    driver_id,                    -- Unique driver identifier
    city,                         -- Operating city: HCMC or HANOI
    vehicle_type,                 -- Type of vehicle: bike, car_4, car_7, electric
    tier,                         -- Service level: bronze, silver, gold, platinum
    avg_rating,                   -- Average rating from completed trips (1-5 scale)
    is_active,                    -- Whether driver is currently active on platform
    online_hours,                 -- Array of 24 bits indicating hourly availability
    last_updated                  -- When this record was last refreshed
FROM driver_latest
```

### Log Formats & Examples

#### **Simulation JSONL Log Example**

```json
{
  "_meta": {
    "schema_version": "1.0",
    "ingested_at": "2026-03-29T10:30:00.000Z",
    "window_start": "2026-03-29T10:15:00.000Z",
    "batch_id": "20260329_1015"
  },
  "trip_id": "TRP00000123",
  "driver_id": "DRV00123",
  "rider_id": "RDR004567",
  "request_time": "2026-03-29T10:16:30.000Z",
  "pickup_time": "2026-03-29T10:20:00.000Z",
  "dropoff_time": "2026-03-29T10:35:00.000Z",
  "status": "completed",
  "city": "HCMC",
  "pickup_zone": "Quận 1",
  "pickup_lat": 10.7769,
  "pickup_lng": 106.7009,
  "dropoff_zone": "Bình Thạnh",
  "dropoff_lat": 10.8121,
  "dropoff_lng": 106.7094,
  "distance_km": 8.5,
  "duration_min": 19,
  "fare_vnd": 85000
}
```

#### **Airflow Task Log Example**

```
[2026-03-29 10:02:15,123] {{taskinstance.py:663}} INFO - Executing <Task(PythonIngestToBronze): ingest_to_bronze> on 2026-03-29T10:00:00+00:00
[2026-03-29 10:02:15,125] {{base_hook.py:88}} INFO - Using connection to: minio
[2026-03-29 10:02:16,500] {{ingest_to_bronze.py:45}} INFO - Processing 1245 trip records for date=2026-03-29 hour=10
[2026-03-29 10:02:18,200] {{ingest_to_bronze.py:67}} INFO - Successfully written to s3a://rideflow/bronze/trips/date=2026-03-29/hour=10/
[2026-03-29 10:02:18,201] {{taskinstance.py:730}} INFO - Marking task as SUCCESS. dag_id=rideflow_pipeline, task_id=ingest_to_bronze, execution_date=2026-03-29T10:00:00+00:00, start_date=2026-03-29T10:02:15,123, end_date=2026-03-29T10:02:18,201
```

#### **Great Expectations Validation Log Example**

```
2026-03-29 10:05:00,123 INFO  great_expectations.expectations.expect_column_values_to_not_be_null
    Column: trip_id
    Unexpected count: 0
    Unexpected percent: 0.0%
    Partial unexpected count: 0
    Element count: 1245
    Missing count: 0
    Missing percent: 0.0%
    Unexpected percentage: 0.0
    Unexpected list: []
    Partial unexpected list: []
    Success: True

2026-03-29 10:05:00,456 INFO  great_expectations.expectations.expect_column_values_to_be_between
    Column: fare_vnd
    Unexpected count: 12
    Unexpected percent: 0.96%
    Partial unexpected count: 12
    Element count: 1245
    Missing count: 0
    Missing percent: 0.0%
    Unexpected percentage: 0.96
    Unexpected list: [-5000, -3000, -1500, ...]
    Success: False
```

#### **Postgres pipeline_runs Table Schema**

```sql
CREATE TABLE pipeline_runs (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    task_name VARCHAR(100) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) CHECK (status IN ('running', 'success', 'failed')),
    records_processed INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Example row:
-- id: 12345
-- pipeline_name: "rideflow_pipeline"
-- task_name: "validate_schema"
-- execution_date: "2026-03-29 10:00:00"
-- start_time: "2026-03-29 10:00:30"
-- end_time: "2026-03-29 10:00:45"
-- status: "success"
-- records_processed: 1
-- error_message: NULL
```

### Data Dictionary Samples

#### **fact_trips Table**

| Column Name          | Data Type    | Description                         | Example Values                  |
| -------------------- | ------------ | ----------------------------------- | ------------------------------- |
| trip_id              | VARCHAR(20)  | Unique identifier for each trip     | TRP00000123                     |
| driver_id            | VARCHAR(10)  | Foreign key to dim_driver           | DRV00123                        |
| rider_id             | VARCHAR(12)  | Foreign key to dim_rider            | RDR004567                       |
| trip_status          | VARCHAR(15)  | Current state of trip               | completed, cancelled, no_driver |
| city                 | VARCHAR(10)  | City where trip occurred            | HCMC, HANOI                     |
| pickup_zone          | VARCHAR(50)  | Neighborhood where trip started     | Quận 1, Bình Thạnh              |
| request_time         | TIMESTAMP    | When customer requested ride        | 2026-03-29 10:16:30             |
| pickup_time          | TIMESTAMP    | When driver arrived at pickup       | 2026-03-29 10:20:00             |
| dropoff_time         | TIMESTAMP    | When trip ended at destination      | 2026-03-29 10:35:00             |
| distance_km          | DECIMAL(6,2) | Trip distance in kilometers         | 8.50                            |
| duration_min         | INTEGER      | Trip duration in minutes            | 19                              |
| fare_vnd             | INTEGER      | Base fare before discounts (VND)    | 85000                           |
| gmv_vnd              | INTEGER      | Gross merchandise value (VND)       | 85000                           |
| platform_revenue_vnd | INTEGER      | Platform commission (VND)           | 17000                           |
| driver_earning_vnd   | INTEGER      | Driver payout (VND)                 | 68000                           |
| discount_vnd         | INTEGER      | Promotional discounts applied (VND) | 0                               |
| request_date         | DATE         | Date portion of request_time        | 2026-03-29                      |
| request_hour         | INTEGER      | Hour of request (0-23)              | 10                              |
| ingest_date          | DATE         | When data entered Bronze layer      | 2026-03-29                      |

#### **dim_driver Table**

| Column Name  | Data Type    | Description                                  | Example Values                                    |
| ------------ | ------------ | -------------------------------------------- | ------------------------------------------------- |
| driver_id    | VARCHAR(10)  | Unique driver identifier                     | DRV00123                                          |
| city         | VARCHAR(10)  | Operating city                               | HCMC, HANOI                                       |
| vehicle_type | VARCHAR(10)  | Type of vehicle                              | bike, car_4, car_7, electric                      |
| tier         | VARCHAR(10)  | Service quality level                        | bronze, silver, gold, platinum                    |
| avg_rating   | DECIMAL(3,2) | Average rating from trips                    | 4.85                                              |
| is_active    | BOOLEAN      | Whether driver is active                     | true, false                                       |
| online_hours | INTEGER[]    | 24-element array showing hourly availability | [0,0,0,0,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,0,0,0] |
| last_updated | TIMESTAMP    | When record was last refreshed               | 2026-03-29 10:30:00                               |

### Benefits of This Approach

1. **Self-Documenting Code**: Future developers can understand table purpose without external documentation
2. **Automated Documentation**: `dbt docs` generates up-to-date data dictionary and lineage
3. **Data Trust**: Clear definitions reduce misinterpretation and analysis errors
4. **Governance Compliance**: Meets requirements for data lineage and metadata management
5. **Onboarding Efficiency**: New team members can quickly understand available data assets
6. **Quality Assurance**: Explicit expectations make data validation more effective

### Implementation Notes

- All dbt models include descriptive headers and column comments
- The `dbt docs generate` command creates searchable documentation site
- Column-level testing ensures documented constraints are enforced
- Log structures are standardized across components for easier monitoring
- Metadata tables in PostgreSQL track pipeline performance and data freshness
