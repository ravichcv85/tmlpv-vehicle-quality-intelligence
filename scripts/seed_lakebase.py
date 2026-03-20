#!/usr/bin/env python3
"""
Seed Lakebase staging tables from CSV files.

Usage:
    python3 scripts/seed_lakebase.py \
        --host <lakebase-host> \
        --database <database-name> \
        --user <user-email-or-sp-id> \
        --token <oauth-token>

The script:
1. Creates tables if they don't exist (using setup_lakebase_tables.sql)
2. Loads data/staging_complaints.csv into staging_complaints
3. Loads data/staging_inspections.csv into staging_inspections
4. Loads data/staging_deliveries.csv into staging_deliveries
"""

import argparse
import csv
import os
import sys

import psycopg2
import psycopg2.extras


def get_connection(host, port, database, user, token):
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=token,
        sslmode="require",
        connect_timeout=15,
    )


def load_csv_to_table(conn, csv_path, table_name):
    """Load a CSV file into a Lakebase table."""
    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)

    if not rows:
        print(f"  No data in {csv_path}, skipping")
        return 0

    # Build INSERT statement
    columns = ", ".join(headers)
    placeholders = ", ".join(["%s"] * len(headers))
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    cur = conn.cursor()

    # Truncate existing data
    cur.execute(f"DELETE FROM {table_name}")
    print(f"  Cleared existing {table_name} data")

    # Insert in batches
    batch_size = 100
    inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        # Convert empty strings to None
        cleaned = []
        for row in batch:
            cleaned.append([None if v == "" else v for v in row])
        psycopg2.extras.execute_batch(cur, sql, cleaned)
        inserted += len(batch)

    conn.commit()
    cur.close()
    print(f"  Inserted {inserted} rows into {table_name}")
    return inserted


def main():
    parser = argparse.ArgumentParser(description="Seed Lakebase staging tables from CSVs")
    parser.add_argument("--host", required=True, help="Lakebase host")
    parser.add_argument("--port", type=int, default=5432, help="Lakebase port")
    parser.add_argument("--database", required=True, help="Database name")
    parser.add_argument("--user", required=True, help="User (email or SP client ID)")
    parser.add_argument("--token", required=True, help="OAuth token")
    args = parser.parse_args()

    # Resolve data directory (relative to this script)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "..", "data")

    print(f"Connecting to {args.host}:{args.port}/{args.database}...")
    conn = get_connection(args.host, args.port, args.database, args.user, args.token)
    conn.autocommit = True
    print("Connected!")

    # Create tables if they don't exist
    setup_sql_path = os.path.join(script_dir, "setup_lakebase_tables.sql")
    if os.path.exists(setup_sql_path):
        print("\nCreating tables (if not exist)...")
        with open(setup_sql_path, "r") as f:
            sql = f.read()
        cur = conn.cursor()
        # Execute each statement separately
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                try:
                    cur.execute(stmt)
                except Exception as e:
                    print(f"  Warning: {e}")
        cur.close()
        print("  Tables ready")

    # Load seed data
    tables = [
        ("staging_complaints", "staging_complaints.csv"),
        ("staging_inspections", "staging_inspections.csv"),
        ("staging_deliveries", "staging_deliveries.csv"),
    ]

    total = 0
    for table_name, csv_file in tables:
        csv_path = os.path.join(data_dir, csv_file)
        if os.path.exists(csv_path):
            print(f"\nLoading {csv_file} -> {table_name}...")
            count = load_csv_to_table(conn, csv_path, table_name)
            total += count
        else:
            print(f"\nWarning: {csv_path} not found, skipping {table_name}")

    conn.close()
    print(f"\nDone! Total rows loaded: {total}")


if __name__ == "__main__":
    main()
