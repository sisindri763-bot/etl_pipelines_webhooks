"""
seed_config.py
--------------
Seeds the config DB with pipeline configurations.
Reads credentials dynamically from environment variables / .env file.

Usage:
    python seed_config.py
"""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load .env file
load_dotenv()


def get_pipeline_configs() -> list:
    """Build pipeline configs dynamically from environment variables."""
    job_id     = os.getenv("DBT_JOB_ID", "70506183135814")
    account_id = os.getenv("DBT_ACCOUNT_ID")
    api_token  = os.getenv("DBT_API_TOKEN")
    base_url   = os.getenv("DBT_BASE_URL", "https://cloud.getdbt.com")

    sf_account   = os.getenv("SNOWFLAKE_ACCOUNT")
    sf_user      = os.getenv("SNOWFLAKE_USER")
    sf_password  = os.getenv("SNOWFLAKE_PASSWORD")
    sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    sf_role      = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    sf_database  = os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS_DB")

    source_schema = os.getenv("SOURCE_SCHEMA", "RAW")
    source_table  = os.getenv("SOURCE_TABLE", "STOCK_DATA_RAW")

    target_schema = os.getenv("TARGET_SCHEMA", "STAGING_STAGING")
    target_table  = os.getenv("TARGET_TABLE", "STG_STOCK_DATA")

    if sf_account and sf_user and sf_password:
        return [
            {
                "job_id": job_id,
                "tool_type": "dbt",
                "tool_config": {
                    "account_id": account_id,
                    "api_token":  api_token,
                    "base_url":   base_url,
                },
                "source_type": "snowflake",
                "source_config": {
                    "account":   sf_account,
                    "user":      sf_user,
                    "password":  sf_password,
                    "warehouse": sf_warehouse,
                    "role":      sf_role,
                    "database":  sf_database,
                    "schema":    source_schema,
                    "table":     source_table,
                },
                "target_type": "snowflake",
                "target_config": {
                    "account":   sf_account,
                    "user":      sf_user,
                    "password":  sf_password,
                    "warehouse": sf_warehouse,
                    "role":      sf_role,
                    "database":  sf_database,
                    "schema":    target_schema,
                    "table":     target_table,
                },
            }
        ]

    return [
        {
            "job_id": job_id,
            "tool_type": "dbt",
            "tool_config": {
                "account_id": account_id,
                "api_token":  api_token,
                "base_url":   base_url,
            },
            "source_type": "csv",
            "source_config": {
                "url_or_path": "https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv",
                "delimiter":   ",",
                "sample_rows": 5,
            },
            "target_type": "api",
            "target_config": {
                "url":         "https://jsonplaceholder.typicode.com/posts",
                "method":      "GET",
                "sample_rows": 5,
            },
        }
    ]


def seed_local():
    """Write directly to local config DB."""
    sys.path.insert(0, str(Path(__file__).parent))
    from config.db import init_db, register_pipeline

    init_db()
    configs = get_pipeline_configs()
    for p in configs:
        register_pipeline(**p)
        print(f"  [OK] Registered pipeline: job_id={p['job_id']} (dbt | source={p['source_type']} -> target={p['target_type']})")
    print(f"\nDone. {len(configs)} pipeline configuration(s) seeded.")


def seed_remote(base_url: str, token: str):
    """POST configs to a deployed instance via /admin/register-config."""
    import requests

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }
    configs = get_pipeline_configs()
    for p in configs:
        url  = f"{base_url.rstrip('/')}/admin/register-config"
        resp = requests.post(url, headers=headers, json=p, timeout=30)
        if resp.ok:
            print(f"  [OK] Registered job_id={p['job_id']} -> {resp.json().get('message')}")
        else:
            print(f"  [FAIL] Failed job_id={p['job_id']}: {resp.status_code} {resp.text}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed webhook server pipeline configs")
    parser.add_argument("--url",   help="Remote server URL (if omitted, seeds local DB)")
    parser.add_argument("--token", help="Admin token for remote seeding")
    args = parser.parse_args()

    if args.url:
        if not args.token:
            print("Error: --token required when seeding a remote server")
            sys.exit(1)
        print(f"Seeding remote server: {args.url}")
        seed_remote(args.url, args.token)
    else:
        print("Seeding local pipeline config DB...")
        seed_local()
