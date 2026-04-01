-- infrastructure/postgres/init/01-create-airflow-db.sql
-- Tạo database cho Airflow
CREATE DATABASE airflow;

-- Tạo database cho Metabase (nếu chưa có)
CREATE DATABASE metabase;

-- Tạo database mặc định cho user dataengineer
CREATE DATABASE dataengineer;

-- Grant quyền cho user dataengineer
GRANT ALL PRIVILEGES ON DATABASE airflow TO dataengineer;
GRANT ALL PRIVILEGES ON DATABASE metabase TO dataengineer;
GRANT ALL PRIVILEGES ON DATABASE dataengineer TO dataengineer;
GRANT ALL PRIVILEGES ON DATABASE rideflow TO dataengineer;