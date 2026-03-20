"""Database connections: SQL Warehouse (gold tables) and Lakebase (staging tables)."""
import json
import threading
import time
from typing import Any

import psycopg2
import psycopg2.extras

import os

from server.config import (
    LAKEBASE_HOST,
    LAKEBASE_PORT,
    LAKEBASE_DB,
    WAREHOUSE_ID,
    QUALITY_SCHEMA,
    IS_DATABRICKS_APP,
    get_oauth_token,
    get_workspace_client,
)

# ---------------------------------------------------------------------------
# SQL Warehouse queries (gold tables via Databricks SDK)
# ---------------------------------------------------------------------------

_warehouse_lock = threading.Lock()


def run_sql_warehouse_query(sql: str) -> list[dict[str, Any]]:
    """Execute SQL on the Databricks SQL Warehouse and return rows as dicts."""
    w = get_workspace_client()
    with _warehouse_lock:
        result = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=sql,
            wait_timeout="50s",
        )

    # Poll if still running
    if result.status and result.status.state:
        state_val = result.status.state.value
        if state_val == "PENDING" or state_val == "RUNNING":
            import time as _time
            for _ in range(24):  # up to ~2 minutes
                _time.sleep(5)
                result = w.statement_execution.get_statement(result.statement_id)
                state_val = result.status.state.value if result.status.state else "UNKNOWN"
                if state_val not in ("PENDING", "RUNNING"):
                    break

    if result.status and result.status.state and result.status.state.value == "FAILED":
        error_msg = result.status.error.message if result.status.error else "Unknown error"
        raise RuntimeError(f"SQL Warehouse query failed: {error_msg}")

    if not result.manifest or not result.result:
        return []

    columns = [col.name for col in result.manifest.schema.columns]
    rows = []
    if result.result.data_array:
        for row_data in result.result.data_array:
            rows.append(dict(zip(columns, row_data)))
    return rows


def query_gold_table(table_name: str, where: str = "", limit: int = 500) -> list[dict]:
    """Query a gold table from the quality schema."""
    sql = f"SELECT * FROM {QUALITY_SCHEMA}.{table_name}"
    if where:
        sql += f" WHERE {where}"
    sql += f" LIMIT {limit}"
    return run_sql_warehouse_query(sql)


# ---------------------------------------------------------------------------
# Lakebase connections (staging tables via psycopg2)
# ---------------------------------------------------------------------------

_lb_conn = None
_lb_token_time = 0
_lb_lock = threading.Lock()
TOKEN_REFRESH_SECS = 40 * 60  # refresh every 40 minutes


def _get_lakebase_conn():
    """Get or refresh Lakebase connection with OAuth token."""
    global _lb_conn, _lb_token_time
    now = time.time()
    if _lb_conn is not None and (now - _lb_token_time) < TOKEN_REFRESH_SECS:
        try:
            # Quick health check
            with _lb_conn.cursor() as cur:
                cur.execute("SELECT 1")
            return _lb_conn
        except Exception:
            _lb_conn = None

    token = get_oauth_token()
    # When Lakebase resource is added, Databricks injects PGHOST/PGUSER etc.
    pg_host = os.environ.get("PGHOST", LAKEBASE_HOST)
    pg_port = int(os.environ.get("PGPORT", str(LAKEBASE_PORT)))
    pg_db = os.environ.get("PGDATABASE", LAKEBASE_DB)
    pg_user = os.environ.get("PGUSER", "")
    pg_ssl = os.environ.get("PGSSLMODE", "require")

    if not pg_user:
        # Fallback: in Databricks Apps use SP client ID, locally use email
        if IS_DATABRICKS_APP:
            pg_user = os.environ.get("DATABRICKS_CLIENT_ID", token)
        else:
            pg_user = "ravichandan.cv@databricks.com"

    _lb_conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        dbname=pg_db,
        user=pg_user,
        password=token,
        sslmode=pg_ssl,
        connect_timeout=15,
    )
    _lb_conn.autocommit = True
    _lb_token_time = now
    return _lb_conn


def lakebase_query(sql: str, params: tuple | None = None) -> list[dict]:
    """Execute a SELECT on Lakebase and return rows as dicts."""
    with _lb_lock:
        conn = _get_lakebase_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def lakebase_execute(sql: str, params: tuple | None = None) -> None:
    """Execute an INSERT/UPDATE/DELETE on Lakebase."""
    with _lb_lock:
        conn = _get_lakebase_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params)
