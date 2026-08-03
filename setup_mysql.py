"""
setup_mysql.py
--------------
One-time setup script to initialize the Central Metadata Database on AWS RDS MySQL.

Creates tables:
  - pipelines
  - pipeline_runs
  - source_asset_metadata
  - target_asset_metadata
"""

import os
import sys
from dotenv import load_dotenv

try:
    import pymysql
    import pymysql.cursors
except ImportError:
    pymysql = None

load_dotenv()

HOST = os.getenv("CENTRAL_DB_HOST") or os.getenv("MYSQL_HOST") or "localhost"
PORT = int(os.getenv("CENTRAL_DB_PORT") or os.getenv("MYSQL_PORT") or 3306)
DB_NAME = os.getenv("CENTRAL_DB_NAME") or os.getenv("MYSQL_DATABASE") or "mysql"
USER = os.getenv("CENTRAL_DB_USER") or os.getenv("MYSQL_USER") or "root"
PASSWORD = os.getenv("CENTRAL_DB_PASSWORD") or os.getenv("MYSQL_PASSWORD") or ""

MYSQL_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipelines (
    job_id VARCHAR(255) PRIMARY KEY,
    tool_type VARCHAR(64) NOT NULL,
    source_type VARCHAR(64) NOT NULL,
    source_config JSON NOT NULL,
    target_type VARCHAR(64) NOT NULL,
    target_config JSON NOT NULL,
    tool_config JSON NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id VARCHAR(64) PRIMARY KEY,
    pipeline_id VARCHAR(255) NOT NULL,
    pipeline_name VARCHAR(255),
    status VARCHAR(64),
    start_time DATETIME NULL,
    end_time DATETIME NULL,
    duration INT,
    tool_name VARCHAR(64),
    rows_read BIGINT,
    rows_written BIGINT,
    error_message TEXT,
    raw_log JSON,
    execution_mode VARCHAR(64),
    triggered_by VARCHAR(255),
    orchestrator_tool VARCHAR(64),
    orchestrator_dag_id VARCHAR(255),
    orchestrator_task_id VARCHAR(255),
    orchestrator_run_id VARCHAR(255),
    saved_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS source_asset_metadata (
    id VARCHAR(64) PRIMARY KEY,
    run_id VARCHAR(64),
    system_name VARCHAR(255),
    system_type VARCHAR(64),
    database_name VARCHAR(255),
    schema_name VARCHAR(255),
    object_name VARCHAR(255),
    object_type VARCHAR(64),
    row_count BIGINT,
    column_count INT,
    size_bytes BIGINT,
    column_names JSON NULL,
    last_updated_at DATETIME NULL,
    observed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_source_run_id FOREIGN KEY (run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS target_asset_metadata (
    id VARCHAR(64) PRIMARY KEY,
    run_id VARCHAR(64),
    system_name VARCHAR(255),
    system_type VARCHAR(64),
    database_name VARCHAR(255),
    schema_name VARCHAR(255),
    object_name VARCHAR(255),
    object_type VARCHAR(64),
    row_count BIGINT,
    column_count INT,
    size_bytes BIGINT,
    column_names JSON NULL,
    last_updated_at DATETIME NULL,
    observed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_target_run_id FOREIGN KEY (run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def setup_mysql():
    if pymysql is None:
        print("ERROR: pymysql package is required. Install via: pip install pymysql")
        sys.exit(1)

    print(f"Connecting to AWS RDS MySQL Central DB...")
    print(f"Host: {HOST}")
    print(f"Port: {PORT}")
    print(f"Database: {DB_NAME}")
    print(f"User: {USER}")

    try:
        # First connect without specifying database to create database if needed
        conn = pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            charset="utf8mb4",
            autocommit=True,
        )
        print("[OK] Connected to AWS RDS MySQL server successfully!")

        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}`;")
            cur.execute(f"USE `{DB_NAME}`;")
            print(f"[OK] Selected database `{DB_NAME}`")

            statements = [s.strip() for s in MYSQL_SCHEMA.split(";") if s.strip()]
            for stmt in statements:
                cur.execute(stmt)

        print("[OK] Central MySQL metadata schema created successfully!")
        print("  - Table: pipelines")
        print("  - Table: pipeline_runs")
        print("  - Table: source_asset_metadata")
        print("  - Table: target_asset_metadata")
        conn.close()

    except Exception as exc:
        print(f"FAILED to initialize Central MySQL DB: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    setup_mysql()
