"""Configuration and authentication for TMLPV Vehicle Quality app."""
import os
from databricks.sdk import WorkspaceClient

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "148ccb90800933a1")
QUALITY_SCHEMA = os.environ.get("QUALITY_SCHEMA", "main.tmlpv_vehicle_quality")
SERVING_ENDPOINT = os.environ.get("SERVING_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")

LAKEBASE_HOST = os.environ.get(
    "LAKEBASE_HOST",
    "instance-f757b185-8ae1-4db1-a76e-5ba630381cf6.database.azuredatabricks.net",
)
LAKEBASE_PORT = int(os.environ.get("LAKEBASE_PORT", "5432"))
LAKEBASE_DB = os.environ.get("LAKEBASE_DB", "tmlpv_staging_db")


def get_workspace_client() -> WorkspaceClient:
    """Get WorkspaceClient - auto-configures in Databricks App or uses profile locally."""
    if IS_DATABRICKS_APP:
        return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE", "azuree2demo")
    return WorkspaceClient(profile=profile)


def get_oauth_token() -> str:
    """Get OAuth token for Lakebase and FMAPI calls."""
    w = get_workspace_client()
    if w.config.token:
        return w.config.token
    auth_headers = w.config.authenticate()
    if auth_headers and "Authorization" in auth_headers:
        return auth_headers["Authorization"].replace("Bearer ", "")
    raise RuntimeError("Could not obtain OAuth token")


def get_workspace_host() -> str:
    """Get workspace host URL with https:// prefix."""
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        return host
    w = get_workspace_client()
    return w.config.host
