# Webhook Server & Metadata Integration Platform

Production webhook integration platform for **dbt Cloud**. Receives webhook events on job completion, fetches run execution logs and data asset snapshots (Source & Target systems), and correlates execution metadata into a central AWS RDS MySQL repository (`repository_db`).

---

## 🏗️ Architecture

```
                                ┌──────────────────────────────────┐
                                │           Frontend UI            │
                                │       (frontend/index.html)      │
                                └────────────────┬─────────────────┘
                                                 │ REST API
                                                 ▼
dbt Cloud ──POST──▶ /webhooks/dbt/<user_id> ──▶ [ Flask API Server ] (backend/app.py)
                                                        │
                                         Look up pipeline config in AWS RDS
                                                        │
                           ┌────────────────────────────┼────────────────────────────┐
                           ▼                            ▼                            ▼
                    Log Adapter                  Source Adapter               Target Adapter
                  (dbt Admin API)               (MySQL/Snowflake/API)        (MySQL/Snowflake/API)
                           │                            │                            │
                           └────────────────────────────┼────────────────────────────┘
                                                        │
                                                        ▼
                                       AWS RDS MySQL (repository_db)
                                 - pipelines
                                 - pipeline_runs
                                 - source_asset_metadata
                                 - target_asset_metadata
```

Failures in any single adapter are captured and stored — they do **not** crash the request or cause dbt to retry.

---

## 🔌 Supported Adapters

| Role   | Type        | Notes                               |
|--------|-------------|-------------------------------------|
| Log    | `dbt`       | Real dbt Cloud Admin API v2         |
| Source | `mysql`     | PyMySQL connector                   |
| Source | `snowflake` | snowflake-connector-python          |
| Source | `csv`       | URL or local file path              |
| Source | `excel`     | .xlsx URL or local file (openpyxl)  |
| Source | `api`       | JSON REST endpoint                  |
| Target | `snowflake` | Same as source                      |
| Target | `mysql`     | Same as source                      |
| Target | `csv`       | Same as source                      |
| Target | `excel`     | Same as source                      |
| Target | `api`       | Same as source                      |

---

## 📁 Directory Structure

```
web_hooks_server/
├── backend/
│   ├── app.py                     # Main Flask app & endpoints
│   ├── wsgi.py                    # Production WSGI entrypoint for Gunicorn
│   ├── requirements.txt           # Backend dependencies
│   ├── .env                       # Central DB & server environment settings
│   ├── .env.example               # Environment variables template
│   ├── pyrightconfig.json         # Python linter configuration
│   ├── adapters/                  # Modular adapter engines (log, source, target)
│   │   ├── log/                   # dbt Cloud log fetcher
│   │   ├── source/                # Source database/file adapters
│   │   └── target/                # Target database/file adapters
│   ├── config/                    # Database access layers (pipelines & run logs)
│   │   ├── db.py                  # Config DB layer (repository_db.pipelines)
│   │   ├── results_db.py          # Execution logs DB layer (repository_db.pipeline_runs)
│   │   └── schema.sql             # SQL DDL reference
│   ├── shared/                    # Shared dataclasses and models
│   ├── scripts/                   # Organized admin utilities
│   │   ├── setup_mysql.py         # AWS RDS database table creation script
│   │   ├── seed_config.py         # Pipeline configuration seeder
│   │   └── get_dbt_webhooks.py    # dbt Cloud API helper script
│   ├── deploy/                    # Deployment configurations
│   │   └── webhook.service        # Gunicorn systemd service file
│   └── results/                   # Local result bundle cache (.json)
├── frontend/
│   └── index.html                 # Web Dashboard UI Interface
├── README.md                      # Platform documentation
└── .gitignore                     # Git ignore rules
```

---

## 🚀 Getting Started

### 1. Environment Setup

Navigate to the `backend/` directory and configure environment variables:

```bash
cd backend

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env
```

Your `backend/.env` file contains central server and database settings:

```env
# ── Central MySQL Database (AWS RDS) ───────────────────────────────────────────
CENTRAL_DB_HOST=database-1.cbsuuwi6y4bg.eu-north-1.rds.amazonaws.com
CENTRAL_DB_PORT=3306
CENTRAL_DB_NAME=repository_db
CENTRAL_DB_USER=admin
CENTRAL_DB_PASSWORD=Saiyalla

# ── Server & Security ────────────────────────────────────────────────────────
PORT=5000
FLASK_DEBUG=1
LOG_LEVEL=INFO
ADMIN_TOKEN=your_admin_secret_token
WEBHOOK_SECRET=
```

### 2. Initialize Central Database (AWS RDS MySQL)

Run the database setup script to create database `repository_db` and tables (`pipelines`, `pipeline_runs`, `source_asset_metadata`, `target_asset_metadata`):

```bash
python scripts/setup_mysql.py
```

### 3. Run the Server

```bash
python app.py
```

The server starts on `http://localhost:5000` and automatically serves the dashboard UI from `frontend/index.html`.

---

## 📡 API Reference

### `GET /`
Serves the interactive Web Dashboard UI (`frontend/index.html`).

### `GET /health`
Returns health check status and loaded adapter registry.

---

### `POST /webhooks/dbt/<user_id>`
Receives a dbt Cloud webhook. Looks up registered pipeline credentials dynamically from `repository_db.pipelines` and executes all adapters.

**Payload** (sent by dbt Cloud):
```json
{
  "jobId": "111111",
  "runId": "987654",
  "eventType": "job.run.completed"
}
```

---

### `POST /admin/register-config`
Register or update a pipeline configuration directly in `repository_db.pipelines`.

**Body:**
```json
{
  "job_id":       "111111",
  "tool_type":    "dbt",
  "tool_config":  {
    "account_id": "12345",
    "api_token":  "dbtc_..."
  },
  "source_type":  "mysql",
  "source_config": {
    "host": "db.example.com",
    "port": 3306,
    "database": "sales",
    "table": "orders",
    "username": "reader",
    "password": "secret"
  },
  "target_type":  "snowflake",
  "target_config": {
    "account":   "xy12345.us-east-1",
    "warehouse": "COMPUTE_WH",
    "database":  "DW",
    "schema":    "PUBLIC",
    "table":     "ORDERS",
    "username":  "svc_user",
    "password":  "secret"
  }
}
```

---

### `GET /admin/list-configs`
List all registered pipelines in `repository_db.pipelines` (credentials masked).

---

### `DELETE /admin/delete-config/<job_id>`
Remove a pipeline configuration from `repository_db.pipelines`.

---

### `GET /admin/runs`
List recent pipeline execution runs stored in `repository_db.pipeline_runs`.

---

### `GET /admin/runs/<run_id>`
Fetch full execution run detail including log, source asset snapshot, and target asset snapshot.

---

## 📊 Result Bundles

Correlated execution results are saved to `results/<job_id>/<run_id>.json` and stored relationally in AWS RDS MySQL (`repository_db`):

```json
{
  "correlation_id": "987654",
  "job_id": "111111",
  "user_id": "alice",
  "tool_type": "dbt",
  "received_at": "2024-01-15T10:30:00",
  "completed_at": "2024-01-15T10:30:05",
  "status": "success",
  "errors": [],
  "log": {
    "tool": "dbt",
    "run_id": "987654",
    "status": "success",
    "duration_s": 42.3
  },
  "source": {
    "connector": "mysql",
    "role": "source",
    "row_count": 50000,
    "columns": ["id", "amount", "created_at"]
  },
  "target": {
    "connector": "snowflake",
    "role": "target",
    "row_count": 50000,
    "columns": ["ID", "AMOUNT", "CREATED_AT"]
  }
}
```
