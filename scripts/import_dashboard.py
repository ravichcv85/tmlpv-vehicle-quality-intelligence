#!/usr/bin/env python3
"""
Import the Lakeview dashboard into a Databricks workspace.

Usage:
    python3 scripts/import_dashboard.py \
        --profile <databricks-profile> \
        --warehouse-id <sql-warehouse-id> \
        [--catalog <catalog>] [--schema <schema>]

The script:
1. Reads dashboard/dashboard_definition.json
2. Updates catalog/schema references to match your environment
3. Creates the dashboard via Lakeview API
4. Publishes it
"""

import argparse
import json
import os
import re
import subprocess
import sys


def get_token(profile):
    """Get OAuth token from Databricks CLI."""
    result = subprocess.run(
        ["databricks", "auth", "token", "-p", profile],
        capture_output=True, text=True,
    )
    for line in result.stdout.strip().split("\n"):
        if "access_token" in line:
            return line.split(":", 1)[1].strip().strip('"')
    # Try JSON output
    try:
        data = json.loads(result.stdout)
        return data.get("access_token", "")
    except json.JSONDecodeError:
        pass
    raise RuntimeError(f"Could not get token. Output: {result.stdout[:200]}")


def get_host(profile):
    """Get workspace host from Databricks CLI."""
    result = subprocess.run(
        ["databricks", "auth", "describe", "-p", profile],
        capture_output=True, text=True,
    )
    for line in result.stdout.split("\n"):
        if "Host:" in line:
            host = line.split(":", 1)[1].strip()
            if not host.startswith("http"):
                host = f"https://{host}"
            return host.rstrip("/")
    raise RuntimeError("Could not determine workspace host")


def main():
    parser = argparse.ArgumentParser(description="Import Lakeview dashboard")
    parser.add_argument("--profile", "-p", required=True, help="Databricks CLI profile")
    parser.add_argument("--warehouse-id", "-w", required=True, help="SQL Warehouse ID")
    parser.add_argument("--catalog", default="cvr_dev_ai_kit", help="Unity Catalog name")
    parser.add_argument("--schema", default="cvr_tm_demo", help="Schema name")
    parser.add_argument("--old-catalog", default="cvr_dev_ai_kit", help="Original catalog to replace")
    parser.add_argument("--old-schema", default="cvr_tm_demo", help="Original schema to replace")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    dashboard_path = os.path.join(script_dir, "..", "dashboard", "dashboard_definition.json")

    if not os.path.exists(dashboard_path):
        print(f"Error: {dashboard_path} not found")
        sys.exit(1)

    print(f"Loading dashboard definition...")
    with open(dashboard_path, "r") as f:
        dashboard = json.load(f)

    # Replace catalog/schema references in serialized_dashboard
    serialized = dashboard.get("serialized_dashboard", "")
    if args.old_catalog != args.catalog or args.old_schema != args.schema:
        print(f"Replacing {args.old_catalog}.{args.old_schema} -> {args.catalog}.{args.schema}")
        serialized = serialized.replace(
            f"{args.old_catalog}.{args.old_schema}",
            f"{args.catalog}.{args.schema}",
        )

    # Update warehouse_id in serialized dashboard
    old_wh = "148ccb90800933a1"  # original warehouse ID
    if old_wh in serialized:
        print(f"Replacing warehouse ID {old_wh} -> {args.warehouse_id}")
        serialized = serialized.replace(old_wh, args.warehouse_id)

    # Build create request
    create_payload = {
        "display_name": dashboard.get("display_name", "TMLPV Vehicle Quality Dashboard"),
        "warehouse_id": args.warehouse_id,
        "serialized_dashboard": serialized,
    }

    host = get_host(args.profile)
    token = get_token(args.profile)

    print(f"Creating dashboard in {host}...")
    import urllib.request

    req = urllib.request.Request(
        f"{host}/api/2.0/lakeview/dashboards",
        data=json.dumps(create_payload).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())
            dashboard_id = result.get("dashboard_id", "")
            print(f"Dashboard created! ID: {dashboard_id}")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"Error creating dashboard: {e.code} {body}")
        sys.exit(1)

    # Publish the dashboard
    print("Publishing dashboard...")
    pub_req = urllib.request.Request(
        f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published",
        data=json.dumps({"warehouse_id": args.warehouse_id}).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(pub_req) as resp:
            print(f"Dashboard published!")
            print(f"\nView at: {host}/dashboardsv3/{dashboard_id}/published")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"Warning: publish failed: {e.code} {body}")
        print(f"View draft at: {host}/dashboardsv3/{dashboard_id}")


if __name__ == "__main__":
    main()
