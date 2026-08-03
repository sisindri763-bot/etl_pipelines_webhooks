"""
get_dbt_webhooks.py
-------------------
Utility to fetch existing webhooks or automatically register a webhook in dbt Cloud
using your dbt Cloud Service Token (Admin API Token).

Usage:
    python get_dbt_webhooks.py --account <ACCOUNT_ID> --token <DBT_API_TOKEN> [--url <YOUR_WEBHOOK_URL>]
"""

import argparse
import sys
import requests


def list_dbt_webhooks(account_id: str, api_token: str, base_url: str = "https://cloud.getdbt.com") -> list:
    """Fetch all webhooks registered in the dbt Cloud account."""
    url = f"{base_url.rstrip('/')}/api/v2/accounts/{account_id}/webhooks/subscription/"
    headers = {
        "Authorization": f"Token {api_token}",
        "Content-Type": "application/json",
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", [])


def create_dbt_webhook(
    account_id: str,
    api_token: str,
    webhook_url: str,
    name: str = "Webhook Production Integration",
    base_url: str = "https://cloud.getdbt.com"
) -> dict:
    """Automatically create a new webhook subscription in dbt Cloud."""
    url = f"{base_url.rstrip('/')}/api/v2/accounts/{account_id}/webhooks/subscription/"
    headers = {
        "Authorization": f"Token {api_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "name": name,
        "client_url": webhook_url,
        "event_types": ["job.run.completed", "job.run.errored"],
        "http_method": "POST",
        "active": True,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    resp.raise_for_status()
    return resp.json().get("data", {})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch or create dbt Cloud webhooks using Service Token")
    parser.add_argument("--account", required=True, help="dbt Cloud Account ID")
    parser.add_argument("--token", required=True, help="dbt Cloud Service Token / API Token")
    parser.add_argument("--url", help="Webhook receiver URL to register (e.g. https://your-app.onrender.com/webhooks/dbt/user1)")
    args = parser.parse_args()

    print(f"Connecting to dbt Cloud (Account ID: {args.account})...")
    try:
        webhooks = list_dbt_webhooks(args.account, args.token)
        print(f"\nFound {len(webhooks)} existing webhook(s):")
        for w in webhooks:
            print(f"  - Name:   {w.get('name')}")
            print(f"    URL:    {w.get('client_url')}")
            print(f"    Secret: {w.get('hmac_secret') or w.get('secret_key') or '***'}")

        if args.url:
            print(f"\nRegistering new webhook URL: {args.url}...")
            new_w = create_dbt_webhook(args.account, args.token, args.url)
            print("  [OK] Webhook created successfully!")
            print(f"  Secret Key: {new_w.get('hmac_secret') or new_w.get('secret_key')}")

    except Exception as exc:
        print(f"\nError: {exc}")
        sys.exit(1)
