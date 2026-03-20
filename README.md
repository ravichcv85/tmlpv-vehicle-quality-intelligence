# TMLPV Vehicle Quality Intelligence

Full-stack Databricks App for vehicle quality management — built for automotive OEMs to manage pre-delivery inspections (PDI), track customer complaints, and use AI to identify checklist gaps.

## What It Does

| Tab | Description |
|-----|-------------|
| **CRM - Log Complaint** | Log customer complaints with AI-powered classification (category/subcategory) via Foundation Model API (Llama 3.3 70B) |
| **PDI Tablet** | 10-item pre-delivery inspection checklist with Pass/Fail/Quick Fix. Recurrence risk detection warns when similar fixes have failed before |
| **AI Checklist Agent** | Select a complaint category and get AI-generated recommendations for new PDI checklist items to close coverage gaps |
| **Pipeline Control** | Trigger Bronze/Silver/Gold ETL notebooks individually or all at once, with live status polling |

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Databricks App                      │
│  ┌──────────────┐    ┌───────────────────────────┐  │
│  │ React + MUI  │───▶│ FastAPI Backend            │  │
│  │ (4 tabs)     │    │ - Complaints API           │  │
│  └──────────────┘    │ - Inspections API          │  │
│                      │ - Checklist Agent API       │  │
│                      │ - Pipeline API              │  │
│                      │ - Metrics API               │  │
│                      └─────┬──────┬──────┬────────┘  │
│                            │      │      │           │
└────────────────────────────┼──────┼──────┼───────────┘
                             │      │      │
                    ┌────────┘      │      └────────┐
                    ▼               ▼                ▼
            ┌──────────────┐ ┌───────────┐  ┌──────────────┐
            │   Lakebase   │ │ SQL       │  │ Foundation   │
            │  PostgreSQL  │ │ Warehouse │  │ Model API    │
            │  (staging)   │ │ (gold)    │  │ (Llama 3.3)  │
            └──────────────┘ └───────────┘  └──────────────┘
                    │               ▲
                    │   ┌───────────┘
                    ▼   │
            ┌──────────────────────────┐
            │    Unity Catalog         │
            │  Bronze → Silver → Gold  │
            │  (Medallion Pipeline)    │
            └──────────────────────────┘
```

## Databricks Features Used

- **Databricks Apps** — Full-stack React + FastAPI app with SP authentication
- **Lakebase** — PostgreSQL-compatible database for real-time staging tables
- **Unity Catalog** — Medallion architecture (Bronze/Silver/Gold) for analytics
- **SQL Warehouse** — Query gold tables for dashboard metrics
- **Foundation Model API** — Llama 3.3 70B for complaint classification and gap analysis
- **Jobs API** — Trigger ETL notebooks from the app
- **MLflow + Model Serving** — Defect risk scoring model (optional)

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- SQL Warehouse (Serverless or Pro)
- Lakebase instance
- Foundation Model API access (pay-per-token, no endpoint setup needed)
- Databricks CLI configured with a profile

## Quick Start

### 1. Set up infrastructure

```bash
# Create a Lakebase instance
databricks database create-database-instance \
  --name my-staging-db \
  --capacity SMALL \
  --profile <your-profile>

# Run the table setup SQL against your Lakebase instance
# See: scripts/setup_lakebase_tables.sql

# Create Unity Catalog tables
# See: scripts/setup_unity_catalog.sql
```

### 2. Configure the app

Edit `app/app.yaml` — replace placeholders with your values:

| Placeholder | Example Value |
|------------|---------------|
| `${DATABRICKS_WAREHOUSE_ID}` | `148ccb90800933a1` |
| `${FMAPI_ENDPOINT}` | `databricks-meta-llama-3-3-70b-instruct` |
| `${LAKEBASE_INSTANCE_NAME}` | `my-staging-db` |
| `${LAKEBASE_DB_NAME}` | `tmlpv_staging_db` |
| `${UC_CATALOG}` | `your_catalog` |
| `${UC_SCHEMA}` | `your_schema` |

Also update `app/server/config.py` and `app/main_deployed.py` with your:
- Warehouse ID
- Lakebase host/port/database
- Unity Catalog catalog.schema
- Notebook paths (update to your workspace user path)

### 3. Upload notebooks

```bash
# Upload pipeline notebooks to your workspace
databricks workspace import-dir notebooks/ \
  /Workspace/Users/<your-email>/tmlpv-notebooks \
  --profile <your-profile>
```

### 4. Seed data

Run the notebooks in order to populate the medallion layers:
1. `01_lakebase_to_bronze.py` — Reads staging tables, writes to bronze
2. `02_bronze_to_silver.sql` — Joins and cleans into silver
3. `03_silver_to_gold.sql` — Aggregates into gold analytics tables
4. `04_ai_checklist_agent.py` — AI-enhanced gap recommendations (optional)
5. `05_risk_scorer_setup.py` — ML risk scoring model (optional)

### 5. Grant SP permissions

After creating the Databricks App, grant its Service Principal access:

```sql
-- See scripts/grant_sp_permissions.sql
GRANT USE CATALOG ON CATALOG your_catalog TO `<sp-client-id>`;
GRANT USE SCHEMA ON SCHEMA your_catalog.your_schema TO `<sp-client-id>`;
GRANT SELECT ON SCHEMA your_catalog.your_schema TO `<sp-client-id>`;
```

### 6. Deploy

```bash
# Build and deploy
./scripts/deploy.sh <your-profile> <your-app-name>
```

Or manually:

```bash
# Build frontend
cd app/frontend && npm install && npx vite build && cd ../..

# Sync to workspace
databricks sync app/ /Workspace/Users/<your-email>/tmlpv-vehicle-quality \
  --profile <your-profile> --full

# Create/deploy app
databricks apps create --name tmlpv-vehicle-quality --profile <your-profile>
```

## Project Structure

```
├── app/                          # Databricks App source
│   ├── app.yaml                  # App configuration (resources, env vars)
│   ├── app.py                    # Entry point
│   ├── main_deployed.py          # Monolithic deployed version (alternative)
│   ├── requirements.txt          # Python dependencies
│   ├── server/                   # FastAPI backend
│   │   ├── config.py             # Configuration and auth
│   │   ├── db.py                 # SQL Warehouse + Lakebase connections
│   │   ├── llm.py                # Foundation Model API client
│   │   └── routes/               # API route handlers
│   │       ├── complaints.py     # CRM complaint logging
│   │       ├── inspections.py    # PDI inspection submission
│   │       ├── checklist_agent.py# AI gap analysis
│   │       ├── pipeline.py       # Notebook execution
│   │       └── metrics.py        # Dashboard metrics
│   └── frontend/                 # React + TypeScript + MUI
│       ├── src/
│       │   ├── App.tsx           # Main app with tab navigation
│       │   ├── theme.ts          # Material UI theme
│       │   ├── hooks/useApi.ts   # Fetch/POST utility hooks
│       │   └── components/       # Tab components
│       │       ├── CRMTab.tsx
│       │       ├── PDITab.tsx
│       │       ├── ChecklistAgentTab.tsx
│       │       ├── PipelineTab.tsx
│       │       └── MetricCard.tsx
│       ├── package.json
│       ├── vite.config.ts
│       └── index.html
├── notebooks/                    # Databricks notebooks (medallion pipeline)
│   ├── 01_lakebase_to_bronze.py  # Lakebase → Bronze (Python)
│   ├── 02_bronze_to_silver.sql   # Bronze → Silver (SQL)
│   ├── 03_silver_to_gold.sql     # Silver → Gold (SQL)
│   ├── 04_ai_checklist_agent.py  # AI gap recommendations (Python)
│   └── 05_risk_scorer_setup.py   # ML risk model setup (Python)
├── scripts/                      # Setup and deployment scripts
│   ├── setup_lakebase_tables.sql # Lakebase table DDL
│   ├── setup_unity_catalog.sql   # UC catalog/schema/table setup
│   ├── grant_sp_permissions.sql  # Service Principal permissions
│   └── deploy.sh                 # Build and deploy script
└── README.md
```

## Data Model

### Staging (Lakebase PostgreSQL — real-time read/write)

| Table | Purpose | ~Rows |
|-------|---------|-------|
| `staging_complaints` | Customer complaints with AI classification | 500+ |
| `staging_inspections` | PDI inspection results (10-item checklist) | 800+ |
| `staging_deliveries` | Vehicle delivery records | 700 |

### Medallion (Unity Catalog Delta — analytics)

| Layer | Table | Purpose |
|-------|-------|---------|
| Bronze | `Customer_Complaints` | Raw complaints |
| Bronze | `ePDI_Inspections` | Raw inspections |
| Bronze | `ePOD_Delivery` | Raw deliveries |
| Silver | `silver_complaint_correlation` | Complaints joined with inspection + delivery data |
| Silver | `silver_epdi_gaps` | Complaint categories not covered by PDI checklist |
| Gold | `gold_complaint_dashboard` | Dealer-level complaint trends and risk flags |
| Gold | `gold_epdi_gap_analysis` | Gap severity and recommendations |
| Gold | `gold_delivery_service_link` | Post-delivery complaint patterns |
| Gold | `gold_complaint_detail` | Per-complaint detail with coverage gap analysis |
| Gold | `gold_checklist_recommendations` | Rule-based checklist improvement recommendations |
| Gold | `gold_checklist_recommendations_ai` | AI-enhanced recommendations (from FMAPI) |

## Key Technical Details

- **Authentication**: App uses Databricks Apps SP OAuth — auto-injected `PGUSER`/`PGHOST` env vars for Lakebase, `WorkspaceClient()` for SDK
- **Lakebase sequences**: Auto-increment IDs not accessible to SP — workaround: `SELECT COALESCE(MAX(id), 0) + 1`
- **FMAPI model**: `databricks-meta-llama-3-3-70b-instruct` (pay-per-token, no provisioned endpoint needed)
- **Frontend proxy**: Vite dev server proxies `/api` to FastAPI on port 8000 for local development

## Local Development

```bash
# Terminal 1: Backend
cd app
pip install -r requirements.txt
DATABRICKS_PROFILE=<your-profile> python app.py

# Terminal 2: Frontend
cd app/frontend
npm install
npm run dev
# Opens at http://localhost:5173 with API proxy to :8000
```

## License

MIT
