#!/bin/bash
# ============================================================
# Deploy TMLPV Vehicle Quality App to Databricks
# Usage: ./deploy.sh <databricks-profile> <app-name>
# Example: ./deploy.sh azuree2demo tmlpv-vehicle-quality
# ============================================================

set -e

PROFILE=${1:-"DEFAULT"}
APP_NAME=${2:-"tmlpv-vehicle-quality"}
WORKSPACE_PATH="/Workspace/Users/$(databricks auth describe --profile $PROFILE 2>/dev/null | grep -oP 'User:\s*\K.*' || echo 'your-email')/${APP_NAME}"

echo "=== TMLPV Vehicle Quality App Deployment ==="
echo "Profile:        $PROFILE"
echo "App Name:       $APP_NAME"
echo "Workspace Path: $WORKSPACE_PATH"
echo ""

# Step 1: Build frontend
echo ">>> Building frontend..."
cd "$(dirname "$0")/../app/frontend"
npm install
npx vite build
cd -

# Step 2: Prepare deploy directory (no node_modules)
echo ">>> Preparing deploy package..."
DEPLOY_DIR="/tmp/tmlpv-deploy-$(date +%s)"
mkdir -p "$DEPLOY_DIR/frontend/dist" "$DEPLOY_DIR/server/routes"

# Copy backend
cp "$(dirname "$0")/../app/app.yaml" "$DEPLOY_DIR/"
cp "$(dirname "$0")/../app/requirements.txt" "$DEPLOY_DIR/"
cp "$(dirname "$0")/../app/main_deployed.py" "$DEPLOY_DIR/main.py"

# Copy built frontend
cp -r "$(dirname "$0")/../app/frontend/dist/"* "$DEPLOY_DIR/frontend/dist/"

# Step 3: Upload to workspace
echo ">>> Uploading to workspace..."
cd "$DEPLOY_DIR"
databricks sync . "$WORKSPACE_PATH" --profile "$PROFILE" --full

# Step 4: Deploy the app
echo ">>> Deploying app..."
WORKSPACE_URL=$(databricks auth describe --profile "$PROFILE" 2>/dev/null | grep -oP 'Host:\s*\K.*' || echo "")
curl -s -X POST "${WORKSPACE_URL}/api/2.0/apps/${APP_NAME}/deployments" \
  -H "Authorization: Bearer $(databricks auth token --profile $PROFILE 2>/dev/null | grep -oP 'access_token:\s*\K.*' || echo '')" \
  -H "Content-Type: application/json" \
  -d "{\"source_code_path\": \"${WORKSPACE_PATH}\", \"mode\": \"SNAPSHOT\"}"

echo ""
echo "=== Deployment submitted! ==="
echo "Check status at: ${WORKSPACE_URL}/apps/${APP_NAME}"

# Cleanup
rm -rf "$DEPLOY_DIR"
