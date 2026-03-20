#!/bin/bash
# ============================================================
# Full End-to-End Setup Script
# Recreates ALL TMLPV assets from scratch in a Databricks workspace
#
# Usage:
#   ./scripts/full_setup.sh <databricks-profile>
#
# Prerequisites:
#   - Databricks CLI configured with the target profile
#   - Python 3.9+ with psycopg2-binary installed
#   - npm installed (for frontend build)
# ============================================================

set -e

PROFILE=${1:?"Usage: $0 <databricks-profile>"}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

echo "=============================================="
echo " TMLPV Vehicle Quality - Full Setup"
echo " Profile: $PROFILE"
echo "=============================================="
echo ""

# Get workspace URL and user
WORKSPACE_URL=$(databricks auth describe -p "$PROFILE" 2>/dev/null | grep "Host:" | awk '{print $2}')
CURRENT_USER=$(databricks current-user me -p "$PROFILE" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('userName','unknown'))" 2>/dev/null || echo "unknown")

echo "Workspace: $WORKSPACE_URL"
echo "User:      $CURRENT_USER"
echo ""

# -------------------------------------------------------
# Step 1: Create Lakebase instance
# -------------------------------------------------------
echo ">>> Step 1: Create Lakebase instance"
echo "    (If it already exists, this will be skipped)"
LAKEBASE_NAME="tmlpv-staging"
LAKEBASE_DB="tmlpv_staging_db"

# Try to create - will fail gracefully if exists
databricks database create-database-instance \
  --name "$LAKEBASE_NAME" \
  --capacity SMALL \
  -p "$PROFILE" 2>/dev/null || echo "    Lakebase instance may already exist, continuing..."

# Get instance details
echo "    Fetching Lakebase instance details..."
LAKEBASE_INFO=$(databricks database get-database-instance --name "$LAKEBASE_NAME" -p "$PROFILE" 2>/dev/null || echo "{}")
LAKEBASE_HOST=$(echo "$LAKEBASE_INFO" | python3 -c "import json,sys; print(json.load(sys.stdin).get('read_write_dns',''))" 2>/dev/null || echo "")

if [ -z "$LAKEBASE_HOST" ]; then
    echo "    WARNING: Could not get Lakebase host. You may need to create it manually."
    echo "    Run: databricks database create-database-instance --name $LAKEBASE_NAME --capacity SMALL -p $PROFILE"
    read -p "    Enter Lakebase host manually (or press Enter to skip): " LAKEBASE_HOST
fi
echo "    Lakebase host: $LAKEBASE_HOST"
echo ""

# -------------------------------------------------------
# Step 2: Seed Lakebase with data
# -------------------------------------------------------
echo ">>> Step 2: Seed Lakebase staging tables"
if [ -n "$LAKEBASE_HOST" ]; then
    # Get token for Lakebase connection
    TOKEN=$(databricks auth token -p "$PROFILE" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null || echo "")

    if [ -n "$TOKEN" ]; then
        pip install psycopg2-binary --quiet 2>/dev/null || true
        python3 "$SCRIPT_DIR/seed_lakebase.py" \
            --host "$LAKEBASE_HOST" \
            --database "$LAKEBASE_DB" \
            --user "$CURRENT_USER" \
            --token "$TOKEN"
    else
        echo "    WARNING: Could not get auth token. Seed data manually."
    fi
else
    echo "    Skipping - no Lakebase host configured"
fi
echo ""

# -------------------------------------------------------
# Step 3: Upload notebooks
# -------------------------------------------------------
echo ">>> Step 3: Upload pipeline notebooks"
NOTEBOOK_DIR="/Workspace/Users/${CURRENT_USER}/cvr_tm_demo"
databricks workspace mkdirs "$NOTEBOOK_DIR" -p "$PROFILE" 2>/dev/null || true

for nb in "$REPO_DIR"/notebooks/*; do
    NB_NAME=$(basename "$nb" | sed 's/\.[^.]*$//')
    echo "    Uploading $NB_NAME..."
    databricks workspace import "$nb" "$NOTEBOOK_DIR/$NB_NAME" \
        -p "$PROFILE" --overwrite 2>/dev/null || echo "    Warning: could not import $NB_NAME"
done
echo "    Notebooks uploaded to $NOTEBOOK_DIR"
echo ""

# -------------------------------------------------------
# Step 4: Set up Unity Catalog
# -------------------------------------------------------
echo ">>> Step 4: Set up Unity Catalog tables"
echo "    NOTE: You need to run the bronze/silver/gold notebooks to create all tables."
echo "    The bronze tables will be created by the first notebook run."
echo "    Run them from the app's Pipeline tab or manually in the workspace."
echo ""

# -------------------------------------------------------
# Step 5: Import Lakeview Dashboard
# -------------------------------------------------------
echo ">>> Step 5: Import Lakeview Dashboard"
read -p "    Enter SQL Warehouse ID: " WAREHOUSE_ID
read -p "    Enter UC Catalog name [cvr_dev_ai_kit]: " UC_CATALOG
UC_CATALOG=${UC_CATALOG:-cvr_dev_ai_kit}
read -p "    Enter UC Schema name [cvr_tm_demo]: " UC_SCHEMA
UC_SCHEMA=${UC_SCHEMA:-cvr_tm_demo}

python3 "$SCRIPT_DIR/import_dashboard.py" \
    -p "$PROFILE" \
    -w "$WAREHOUSE_ID" \
    --catalog "$UC_CATALOG" \
    --schema "$UC_SCHEMA"
echo ""

# -------------------------------------------------------
# Step 6: Build and deploy the app
# -------------------------------------------------------
echo ">>> Step 6: Build and deploy Databricks App"
APP_NAME="tmlpv-vehicle-quality"

# Build frontend
echo "    Building frontend..."
cd "$REPO_DIR/app/frontend"
npm install --silent 2>/dev/null
npx vite build 2>/dev/null
cd "$REPO_DIR"

# Prepare deploy directory
DEPLOY_DIR="/tmp/tmlpv-deploy-$$"
mkdir -p "$DEPLOY_DIR/frontend/dist" "$DEPLOY_DIR/server/routes"

# Copy backend (use monolithic main.py for deployment)
cp "$REPO_DIR/app/app.yaml" "$DEPLOY_DIR/"
cp "$REPO_DIR/app/requirements.txt" "$DEPLOY_DIR/"
cp "$REPO_DIR/app/main_deployed.py" "$DEPLOY_DIR/main.py"

# Copy built frontend
cp -r "$REPO_DIR/app/frontend/dist/"* "$DEPLOY_DIR/frontend/dist/"

# Upload to workspace
WORKSPACE_APP_PATH="/Workspace/Users/${CURRENT_USER}/${APP_NAME}"
echo "    Uploading to $WORKSPACE_APP_PATH..."
cd "$DEPLOY_DIR"
databricks sync . "$WORKSPACE_APP_PATH" -p "$PROFILE" --full

# Create app (if not exists) and deploy
echo "    Creating/deploying app..."
databricks apps create --name "$APP_NAME" -p "$PROFILE" 2>/dev/null || echo "    App may already exist"

# Get token for API call
TOKEN=$(databricks auth token -p "$PROFILE" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null || echo "")
curl -s -X POST "${WORKSPACE_URL}/api/2.0/apps/${APP_NAME}/deployments" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"source_code_path\": \"${WORKSPACE_APP_PATH}\", \"mode\": \"SNAPSHOT\"}" > /dev/null

# Cleanup
rm -rf "$DEPLOY_DIR"

echo ""
echo "=============================================="
echo " Setup Complete!"
echo "=============================================="
echo ""
echo " Assets created:"
echo "   - Lakebase instance: $LAKEBASE_NAME"
echo "   - Staging data: 504 complaints, 803 inspections, 701 deliveries"
echo "   - Pipeline notebooks: $NOTEBOOK_DIR"
echo "   - Lakeview Dashboard: check output above for URL"
echo "   - Databricks App: $APP_NAME"
echo ""
echo " Next steps:"
echo "   1. Grant SP permissions (see scripts/grant_sp_permissions.sql)"
echo "   2. Run the pipeline from the app's Pipeline tab"
echo "   3. Update app.yaml with your warehouse/lakebase IDs"
echo "   4. Redeploy if needed"
echo "=============================================="
