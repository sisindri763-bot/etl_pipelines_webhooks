# Webhook Server

Production webhook server for dbt Cloud. Receives webhook events, fetches the dbt run log **and** snapshots from source and target systems, then saves a correlated result bundle per run.

---

## Architecture

```
dbt Cloud ──POST──▶ /webhooks/dbt/<user_id>
                          │
                    look up config (job_id → config DB)
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
         Log adapter   Source      Target
         (dbt API)     adapter     adapter
              │           │           │
              └───────────┴───────────┘
                          │
                   results/<job_id>/<run_id>.json
```

Failures in any one adapter are captured and stored — they do **not** crash the request or cause dbt to retry.

---

## Supported Adapters

| Role   | Type       | Notes                              |
|--------|------------|------------------------------------|
| Log    | `dbt`      | Real dbt Cloud Admin API v2        |
| Source | `mysql`    | PyMySQL, pure-Python               |
| Source | `snowflake`| snowflake-connector-python         |
| Source | `csv`      | URL or local file path             |
| Source | `excel`    | .xlsx URL or local file, openpyxl  |
| Source | `api`      | Any JSON REST endpoint             |
| Target | `snowflake`| Same as source                     |
| Target | `mysql`    | Same as source                     |
| Target | `csv`      | Same as source                     |
| Target | `excel`    | Same as source                     |
| Target | `api`      | Same as source                     |

Every adapter returns the same shape:
```json
{
  "connector": "mysql",
  "role": "source",
  "row_count": 12345,
  "columns": ["id", "name", "amount"],
  "sample": [...],
  "extra": {},
  "fetched_at": "2024-01-15T10:30:00"
}
```

---

## Local Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Copy env template
cp .env.example .env
# (edit .env — leave secrets blank for local dev)

# 3. Seed example pipeline configs
python seed_config.py

# 4. Run the server
python app.py
```

The server starts on `http://localhost:5000`.

---

## API Reference

### `POST /webhooks/dbt/<user_id>`
Receives a dbt Cloud webhook. dbt sends this automatically on job completion.

**Headers** (optional if `WEBHOOK_SECRET` is set):
```
Authorization: Bearer <hmac_signature>
```

**Payload** (sent by dbt Cloud):
```json
{
  "jobId": "111111",
  "runId": "987654",
  "eventType": "job.run.completed"
}
```

**Response** (`200 OK`):
```json
{
  "correlation_id": "987654",
  "job_id": "111111",
  "status": "success",
  "log": { ... },
  "source": { ... },
  "target": { ... }
}
```

---

### `POST /admin/register-config`
Register or update a pipeline config. Safe to call while the server is running.

**Headers:**
```
Authorization: Bearer <ADMIN_TOKEN>
Content-Type: application/json
```

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

**Curl:**
```bash
curl -X POST https://your-app.onrender.com/admin/register-config \
     -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \
     -H "Content-Type: application/json" \
     -d @pipeline.json
```

---

### `GET /admin/list-configs`
List all registered pipelines. Passwords are masked in the response.

```bash
curl https://your-app.onrender.com/admin/list-configs \
     -H "Authorization: Bearer YOUR_ADMIN_TOKEN"
```

---

### `DELETE /admin/delete-config/<job_id>`
Remove a pipeline config.

```bash
curl -X DELETE https://your-app.onrender.com/admin/delete-config/111111 \
     -H "Authorization: Bearer YOUR_ADMIN_TOKEN"
```

---

### `GET /health`
Health check — returns adapter registry and status.

```bash
curl https://your-app.onrender.com/health
```

---

## Deploy to Render

1. Push this repo to GitHub
2. New Web Service → connect repo
3. **Build command:** `pip install -r requirements.txt`
4. **Start command:** `gunicorn app:app --bind 0.0.0.0:$PORT --timeout 120`
5. Add environment variables:
   - `ADMIN_TOKEN` → generate with `python -c "import secrets; print(secrets.token_hex(32))"`
   - `WEBHOOK_SECRET` → from dbt Cloud → Account Settings → Webhooks
   - `LOG_LEVEL` → `INFO`
6. Deploy → copy the service URL

7. Register your first pipeline:
```bash
curl -X POST https://your-app.onrender.com/admin/register-config \
     -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{
       "job_id": "YOUR_DBT_JOB_ID",
       "tool_type": "dbt",
       "tool_config": { "account_id": "YOUR_ACCOUNT_ID", "api_token": "YOUR_TOKEN" },
       "source_type": "mysql",
       "source_config": { "host": "...", "database": "...", "table": "...", "username": "...", "password": "..." },
       "target_type": "snowflake",
       "target_config": { "account": "...", "warehouse": "...", "database": "...", "schema": "PUBLIC", "table": "...", "username": "...", "password": "..." }
     }'
```

8. In dbt Cloud → Account Settings → Webhooks:
   - URL: `https://your-app.onrender.com/webhooks/dbt/YOUR_USER_ID`
   - Events: `job.run.completed`
   - Secret: same as `WEBHOOK_SECRET`

---

## Result Bundles

Results are saved to `results/<job_id>/<run_id>.json`:

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
    "duration_s": 42.3,
    "steps": [...]
  },
  "source": {
    "connector": "mysql",
    "role": "source",
    "row_count": 50000,
    "columns": ["id", "amount", "created_at"],
    "sample": [...]
  },
  "target": {
    "connector": "snowflake",
    "role": "target",
    "row_count": 50000,
    "columns": ["ID", "AMOUNT", "CREATED_AT"],
    "sample": [...]
  }
}
```

**Status values:**
- `success` — all 3 fetches succeeded
- `partial` — 1 or 2 fetches failed (still saved what succeeded)
- `failed` — all 3 fetches failed

---

## Project Structure

```
web_hooks_server/
├── app.py                        ← Main Flask app (all routes)
├── requirements.txt
├── seed_config.py                ← Seed config DB (local or remote)
├── .env.example                  ← Env var template
├── adapters/
│   ├── __init__.py               ← Top-level registry
│   ├── log/
│   │   ├── base.py               ← LogAdapter ABC
│   │   ├── dbt.py                ← dbt Cloud Admin API
│   │   └── __init__.py           ← LOG_ADAPTERS registry
│   ├── source/
│   │   ├── base.py               ← DataAdapter ABC
│   │   ├── mysql.py
│   │   ├── snowflake.py
│   │   ├── csv_adapter.py
│   │   ├── excel_adapter.py
│   │   ├── api_adapter.py
│   │   └── __init__.py           ← SOURCE_ADAPTERS registry
│   └── target/
│       ├── base.py
│       ├── snowflake.py
│       ├── mysql.py
│       ├── csv_adapter.py
│       ├── excel_adapter.py
│       ├── api_adapter.py
│       └── __init__.py           ← TARGET_ADAPTERS registry
├── config/
│   ├── db.py                     ← SQLite config DB (Postgres-ready)
│   └── schema.sql                ← DDL reference
├── shared/
│   └── models.py                 ← Shared dataclasses
└── results/                      ← Job result bundles saved here
```
