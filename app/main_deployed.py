import os
import json
import logging
from datetime import date, datetime
from typing import Optional
from contextlib import asynccontextmanager

import httpx
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Config ---
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "148ccb90800933a1")
# Lakebase: Use Databricks App resource env vars (DATABRICKS_DATABASE_*)
LAKEBASE_HOST = os.environ.get("DATABRICKS_DATABASE_HOST", os.environ.get("LAKEBASE_HOST", "instance-f757b185-8ae1-4db1-a76e-5ba630381cf6.database.azuredatabricks.net"))
LAKEBASE_PORT = int(os.environ.get("DATABRICKS_DATABASE_PORT", os.environ.get("LAKEBASE_PORT", "5432")))
LAKEBASE_DB = os.environ.get("DATABRICKS_DATABASE_NAME", os.environ.get("LAKEBASE_DB", "tmlpv_staging_db"))
UC_CATALOG = os.environ.get("UC_CATALOG", "cvr_dev_ai_kit")
UC_SCHEMA = os.environ.get("UC_SCHEMA", "cvr_tm_demo")
FMAPI_ENDPOINT = os.environ.get("FMAPI_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")

# Notebook paths for pipeline
NOTEBOOK_BRONZE = "/Workspace/Users/ravichandan.cv@databricks.com/cvr_tm_demo/lakebase_to_bronze"
NOTEBOOK_SILVER = "/Workspace/Users/ravichandan.cv@databricks.com/cvr_tm_demo/bronze_to_silver_pipeline"
NOTEBOOK_GOLD = "/Workspace/Users/ravichandan.cv@databricks.com/cvr_tm_demo/silver_to_gold_pipeline"

w = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global w
    w = WorkspaceClient()
    logger.info("WorkspaceClient initialized")
    yield

app = FastAPI(title="TMLPV Vehicle Quality Intelligence", lifespan=lifespan)


# --- Helpers ---
def get_token():
    return w.config.authenticate()


def get_lakebase_conn():
    # Use Databricks App's PG* env vars (auto-injected for database resource)
    # PGUSER = SP client ID, password = SP OAuth token
    headers = w.config.authenticate()
    access_token = headers.get("Authorization", "").replace("Bearer ", "")
    pg_user = os.environ.get("PGUSER", "token")
    pg_host = os.environ.get("PGHOST", LAKEBASE_HOST)
    pg_port = int(os.environ.get("PGPORT", str(LAKEBASE_PORT)))
    pg_db = os.environ.get("PGDATABASE", LAKEBASE_DB)
    logger.info(f"Connecting to Lakebase: {pg_host}:{pg_port}/{pg_db} as {pg_user}")
    return psycopg2.connect(
        host=pg_host,
        port=pg_port,
        dbname=pg_db,
        user=pg_user,
        password=access_token,
        sslmode="require",
    )


def run_sql(statement: str):
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        wait_timeout="30s",
    )
    if resp.status.state == StatementState.SUCCEEDED:
        cols = [c.name for c in resp.manifest.schema.columns]
        rows = [dict(zip(cols, row)) for row in (resp.result.data_array or [])]
        return rows
    raise HTTPException(500, f"SQL failed: {resp.status.error}")


async def call_fmapi(prompt: str, max_tokens: int = 500) -> str:
    headers = w.config.authenticate()
    host = w.config.host.rstrip("/")
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{host}/serving-endpoints/{FMAPI_ENDPOINT}/invocations",
            headers={**headers, "Content-Type": "application/json"},
            json={"messages": [{"role": "user", "content": prompt}], "max_tokens": max_tokens},
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]


# --- Models ---
class ComplaintRequest(BaseModel):
    vin: str
    model: str
    variant: str
    customer_name: str
    dealer_code: str
    dealer_name: str
    severity: str
    description: str

class InspectionRequest(BaseModel):
    vin: str
    model: str
    variant: str
    dealer_code: str
    dealer_name: str
    inspector_name: str
    checklist: dict  # {"exterior_body": "PASS", "paint_finish": "FAIL", ...}
    fix_actions: dict = {}  # {"engine_bay": "Replaced spark plugs", "brakes": "Adjusted brake pads"}

class GapAnalysisRequest(BaseModel):
    gap_category: str


# --- API: Metrics ---
@app.get("/api/metrics")
def get_metrics():
    complaints = run_sql(f"SELECT COUNT(*) as cnt FROM {UC_CATALOG}.{UC_SCHEMA}.customer_complaints")
    inspections = run_sql(f"SELECT COUNT(*) as cnt FROM {UC_CATALOG}.{UC_SCHEMA}.epdi_inspections")
    deliveries = run_sql(f"SELECT COUNT(*) as cnt FROM {UC_CATALOG}.{UC_SCHEMA}.epod_delivery")
    gaps = run_sql(f"SELECT COUNT(*) as cnt, SUM(complaint_count) as total_complaints FROM {UC_CATALOG}.{UC_SCHEMA}.gold_epdi_gap_analysis")
    risk = run_sql(f"SELECT dealer_risk_flag, COUNT(*) as cnt FROM {UC_CATALOG}.{UC_SCHEMA}.gold_complaint_dashboard GROUP BY dealer_risk_flag")
    return {
        "total_complaints": complaints[0]["cnt"] if complaints else 0,
        "total_inspections": inspections[0]["cnt"] if inspections else 0,
        "total_deliveries": deliveries[0]["cnt"] if deliveries else 0,
        "gap_categories": gaps[0]["cnt"] if gaps else 0,
        "gap_total_complaints": gaps[0]["total_complaints"] if gaps else 0,
        "dealer_risk": {r["dealer_risk_flag"]: r["cnt"] for r in risk},
    }


# --- API: Tab 1 - CRM Complaints ---
@app.post("/api/complaints")
async def log_complaint(req: ComplaintRequest):
    # Step 1: AI classification
    prompt = f"""You are a vehicle quality expert for Tata Motors. Classify this customer complaint into a category and subcategory.

Complaint: "{req.description}"
Vehicle: {req.model} {req.variant}
Severity: {req.severity}

Valid categories: Mechanical, Electrical, Cosmetic, Safety, Infotainment, Service, Recall / Software Update
Respond ONLY in JSON: {{"category": "...", "subcategory": "...", "confidence": 0.XX}}"""

    try:
        ai_resp = await call_fmapi(prompt, max_tokens=200)
        # Extract JSON from response
        json_str = ai_resp.strip()
        if "```" in json_str:
            json_str = json_str.split("```")[1].replace("json", "").strip()
        classification = json.loads(json_str)
    except Exception as e:
        logger.error(f"FMAPI classification failed: {e}")
        classification = {"category": "Unknown", "subcategory": "Unclassified", "confidence": 0.0}

    # Step 2: Write to Lakebase
    try:
        conn = get_lakebase_conn()
        cur = conn.cursor()
        # Get next ID manually (avoids sequence permission issues)
        cur.execute("SELECT COALESCE(MAX(complaint_id), 0) + 1 FROM staging_complaints")
        next_id = cur.fetchone()[0]
        cur.execute("""
            INSERT INTO staging_complaints
            (complaint_id, vin, customer_name, dealer_code, dealer_name, complaint_date, description,
             category, subcategory, ai_category, ai_subcategory, ai_confidence, severity, status, model, variant, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (
            next_id,
            req.vin, req.customer_name, req.dealer_code, req.dealer_name,
            date.today().isoformat(), req.description,
            classification["category"], classification.get("subcategory", ""),
            classification["category"], classification.get("subcategory", ""),
            classification.get("confidence", 0.0),
            req.severity, "Open", req.model, req.variant,
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Lakebase write failed: {e}")
        raise HTTPException(500, f"Failed to save complaint: {e}")

    return {
        "status": "success",
        "classification": classification,
        "message": f"Complaint logged for VIN {req.vin}",
    }


@app.get("/api/complaints/recent")
def get_recent_complaints():
    try:
        conn = get_lakebase_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT complaint_id, vin, model, variant, customer_name, dealer_name,
                   complaint_date, category, subcategory, ai_category, ai_subcategory,
                   ai_confidence, severity, status, description
            FROM staging_complaints ORDER BY created_at DESC LIMIT 20
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Failed to fetch complaints: {e}")
        return {"error": str(e)}


@app.get("/api/debug/lakebase")
def debug_lakebase():
    """Debug endpoint to test Lakebase connectivity."""
    # Dump all env vars that might be relevant
    db_envs = {k: v for k, v in os.environ.items() if any(x in k.upper() for x in ['DATABASE', 'LAKEBASE', 'DB_', 'PG', 'POSTGRES'])}
    info = {
        "LAKEBASE_HOST": LAKEBASE_HOST,
        "LAKEBASE_PORT": LAKEBASE_PORT,
        "LAKEBASE_DB": LAKEBASE_DB,
        "db_related_env_vars": db_envs,
    }
    try:
        conn = get_lakebase_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM staging_complaints")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        info["connection"] = "SUCCESS"
        info["staging_complaints_count"] = count
    except Exception as e:
        info["connection"] = "FAILED"
        info["error"] = str(e)
    return info


# --- API: Tab 2 - PDI Inspection ---
CHECKLIST_ITEMS = [
    "exterior_body", "paint_finish", "engine_bay", "ac_system", "brakes",
    "electrical", "tyres_wheels", "suspension", "infotainment", "safety_systems"
]

@app.post("/api/inspections")
async def log_inspection(req: InspectionRequest):
    items_failed = sum(1 for k, v in req.checklist.items() if v == "FAIL")
    items_quick_fix = sum(1 for k, v in req.checklist.items() if v == "QUICK_FIX")
    items_checked = len(req.checklist)
    risk_score = round(items_failed * 15.0 + items_quick_fix * 5.0 + (items_checked - items_failed - items_quick_fix) * 0.5, 2)
    risk_confidence = round(min(0.95, 0.5 + items_checked * 0.04), 2)
    overall_result = "FAIL" if items_failed > 0 else ("CONDITIONAL" if items_quick_fix > 0 else "PASS")
    delivery_cleared = items_failed == 0

    # --- Recurrence Analysis: Check if similar fixes were applied before and issues recurred ---
    recurrence_warnings = []
    ai_risk_adjustment = 0
    fix_items_applied = {k: v for k, v in req.fix_actions.items() if v and v.strip()}

    if fix_items_applied:
        try:
            conn = get_lakebase_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Find historical inspections where the same checklist items had issues
            # and the same/similar fix actions were taken
            for item_key, fix_desc in fix_items_applied.items():
                action_col = f"{item_key}_action"
                result_col = f"{item_key}_result"
                # Get past inspections where this item had an action AND the vehicle later had a complaint
                cur.execute(f"""
                    SELECT si.vin, si.model, si.{action_col} as past_action, si.{result_col} as past_result,
                           si.inspection_date, si.overall_result as past_overall,
                           sc.category as complaint_category, sc.description as complaint_desc
                    FROM staging_inspections si
                    LEFT JOIN staging_complaints sc ON si.vin = sc.vin AND sc.complaint_date > si.inspection_date
                    WHERE si.{action_col} IS NOT NULL AND si.{action_col} != ''
                      AND si.{result_col} IN ('FAIL', 'QUICK_FIX')
                    ORDER BY si.inspection_date DESC
                    LIMIT 20
                """)
                historical = cur.fetchall()

                if historical:
                    # Count how many had subsequent complaints
                    with_complaints = [h for h in historical if h.get('complaint_category')]
                    recurrence_rate = len(with_complaints) / len(historical) if historical else 0

                    if recurrence_rate > 0.3:
                        recurrence_warnings.append({
                            "item": item_key,
                            "fix_applied": fix_desc,
                            "historical_cases": len(historical),
                            "recurrence_count": len(with_complaints),
                            "recurrence_rate": round(recurrence_rate * 100, 1),
                            "severity": "HIGH" if recurrence_rate > 0.5 else "MEDIUM",
                        })
                        ai_risk_adjustment += 15 if recurrence_rate > 0.5 else 8

            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Recurrence check failed: {e}")

    # Adjust risk score based on recurrence analysis
    risk_score = round(risk_score + ai_risk_adjustment, 2)

    # If recurrence warnings exist, call FMAPI for detailed risk assessment
    ai_recurrence_analysis = None
    if recurrence_warnings:
        try:
            warnings_text = json.dumps(recurrence_warnings, default=str)
            prompt = f"""You are a vehicle quality risk analyst at Tata Motors. Analyze this PDI inspection recurrence risk.

Vehicle: {req.model} {req.variant} (VIN: {req.vin})
Current inspection found these issues with applied fixes:
{json.dumps(fix_items_applied, indent=2)}

RECURRENCE DATA (same fixes applied before, then complaints came back):
{warnings_text}

Based on this pattern:
1. Should this vehicle's delivery be HELD or CLEARED? Explain why.
2. What is the predicted recurrence probability (0-100%)?
3. What alternative fix should be tried instead?
4. Is this a systemic issue that affects the {req.model} model broadly?

Be specific and decisive. This is a real delivery decision."""
            ai_recurrence_analysis = await call_fmapi(prompt, max_tokens=500)
        except Exception as e:
            logger.error(f"FMAPI recurrence analysis failed: {e}")

        # Override delivery clearance if high recurrence risk
        if any(w["severity"] == "HIGH" for w in recurrence_warnings):
            delivery_cleared = False
            overall_result = "FAIL" if overall_result != "FAIL" else overall_result

    risk_rec = "Clear for delivery" if overall_result == "PASS" and not recurrence_warnings else (
        f"HOLD DELIVERY — Recurrence risk detected in {len(recurrence_warnings)} item(s). Same fixes applied before led to complaints in {recurrence_warnings[0]['recurrence_rate']}% of cases."
        if recurrence_warnings else (
            "Quick fixes required before delivery" if overall_result == "CONDITIONAL" else
            "Hold delivery — critical failures detected"
        )
    )

    try:
        conn = get_lakebase_conn()
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(MAX(inspection_id), 0) + 1 FROM staging_inspections")
        next_id = cur.fetchone()[0]
        cols_vals = {
            "inspection_id": next_id,
            "vin": req.vin, "dealer_code": req.dealer_code, "dealer_name": req.dealer_name,
            "inspector_name": req.inspector_name, "inspection_date": date.today().isoformat(),
            "model": req.model, "variant": req.variant, "overall_result": overall_result,
            "risk_score": risk_score, "risk_confidence": risk_confidence,
            "risk_recommendation": risk_rec, "delivery_cleared": delivery_cleared,
        }
        # Add checklist results with actual fix descriptions
        for item in CHECKLIST_ITEMS:
            result = req.checklist.get(item, "PASS")
            cols_vals[f"{item}_result"] = result
            if result == "PASS":
                cols_vals[f"{item}_action"] = ""
            else:
                # Use the inspector's fix description if provided, else default
                fix_desc = req.fix_actions.get(item, "")
                if not fix_desc:
                    fix_desc = "Quick fix applied" if result == "QUICK_FIX" else "Failed — needs repair"
                cols_vals[f"{item}_action"] = fix_desc

        columns = ", ".join(cols_vals.keys()) + ", created_at"
        placeholders = ", ".join(["%s"] * len(cols_vals)) + ", NOW()"
        cur.execute(
            f"INSERT INTO staging_inspections ({columns}) VALUES ({placeholders})",
            list(cols_vals.values()),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Lakebase inspection write failed: {e}")
        raise HTTPException(500, f"Failed to save inspection: {e}")

    return {
        "status": "success",
        "overall_result": overall_result,
        "risk_score": risk_score,
        "risk_confidence": risk_confidence,
        "items_failed": items_failed,
        "items_quick_fix": items_quick_fix,
        "delivery_cleared": delivery_cleared,
        "recommendation": risk_rec,
        "recurrence_warnings": recurrence_warnings,
        "ai_recurrence_analysis": ai_recurrence_analysis,
    }


@app.get("/api/inspections/recent")
def get_recent_inspections():
    try:
        conn = get_lakebase_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT inspection_id, vin, model, variant, dealer_name, inspector_name,
                   inspection_date, overall_result, risk_score, risk_confidence,
                   delivery_cleared, risk_recommendation
            FROM staging_inspections ORDER BY created_at DESC LIMIT 20
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Failed to fetch inspections: {e}")
        return []


# --- API: Tab 3 - AI Checklist Agent ---
@app.get("/api/gap-categories")
def get_gap_categories():
    rows = run_sql(f"""
        SELECT gap_category, complaint_count, vehicle_count, severity, recommendation
        FROM {UC_CATALOG}.{UC_SCHEMA}.gold_epdi_gap_analysis
        ORDER BY complaint_count DESC
    """)
    return rows


@app.get("/api/checklist-recommendations")
def get_checklist_recommendations():
    rows = run_sql(f"""
        SELECT what_customers_complain_about, total_complaints, caught_by_inspection,
               pct_caught_by_inspection, current_checklist_item, ai_recommendation, priority
        FROM {UC_CATALOG}.{UC_SCHEMA}.gold_checklist_recommendations_ai
        ORDER BY priority DESC
    """)
    return rows


@app.post("/api/gap-analysis")
async def analyze_gap(req: GapAnalysisRequest):
    # Get gap data
    gaps = run_sql(f"""
        SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.gold_epdi_gap_analysis
        WHERE gap_category = '{req.gap_category}'
    """)
    recommendations = run_sql(f"""
        SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.gold_checklist_recommendations_ai
        WHERE what_customers_complain_about LIKE '%{req.gap_category}%'
        ORDER BY priority DESC LIMIT 5
    """)

    gap_context = json.dumps(gaps, default=str)
    rec_context = json.dumps(recommendations, default=str)

    prompt = f"""You are a vehicle quality engineer at Tata Motors. Analyze this ePDI (electronic Pre-Delivery Inspection) gap for the "{req.gap_category}" category.

GAP DATA:
{gap_context}

EXISTING AI RECOMMENDATIONS:
{rec_context}

Based on this analysis:
1. What specific checklist items should be ADDED to the ePDI process?
2. What existing items need to be made MORE RIGOROUS?
3. What is the estimated complaint reduction if these changes are implemented?
4. Priority actions for the quality team this week.

Be specific and actionable. Reference actual data from above."""

    try:
        ai_response = await call_fmapi(prompt, max_tokens=800)
    except Exception as e:
        logger.error(f"FMAPI gap analysis failed: {e}")
        ai_response = "AI analysis unavailable. Please review the gap data manually."

    return {
        "gap_category": req.gap_category,
        "gap_data": gaps,
        "existing_recommendations": recommendations,
        "ai_analysis": ai_response,
    }


# --- API: Tab 4 - Pipeline ---
@app.post("/api/pipeline/run")
def run_pipeline(stage: str = "all"):
    notebooks = {
        "bronze": NOTEBOOK_BRONZE,
        "silver": NOTEBOOK_SILVER,
        "gold": NOTEBOOK_GOLD,
    }

    if stage == "all":
        tasks = [
            {"task_key": "bronze", "notebook_task": {"notebook_path": NOTEBOOK_BRONZE}},
            {"task_key": "silver", "notebook_task": {"notebook_path": NOTEBOOK_SILVER}, "depends_on": [{"task_key": "bronze"}]},
            {"task_key": "gold", "notebook_task": {"notebook_path": NOTEBOOK_GOLD}, "depends_on": [{"task_key": "silver"}]},
        ]
    elif stage in notebooks:
        tasks = [{"task_key": stage, "notebook_task": {"notebook_path": notebooks[stage]}}]
    else:
        raise HTTPException(400, f"Invalid stage: {stage}. Use bronze, silver, gold, or all")

    try:
        from databricks.sdk.service.jobs import SubmitTask, NotebookTask, TaskDependency

        submit_tasks = []
        for t in tasks:
            nt = NotebookTask(notebook_path=t["notebook_task"]["notebook_path"])
            deps = [TaskDependency(task_key=d["task_key"]) for d in t.get("depends_on", [])]
            st = SubmitTask(
                task_key=t["task_key"],
                notebook_task=nt,
                depends_on=deps if deps else None,
            )
            # Use serverless compute (environment_key)
            st.environment_key = "Default"
            submit_tasks.append(st)

        run = w.jobs.submit(
            run_name=f"tmlpv_pipeline_{stage}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            tasks=submit_tasks,
        )
        return {"run_id": run.bind()["run_id"] if hasattr(run, "bind") else str(run.run_id), "stage": stage, "status": "SUBMITTED"}
    except Exception as e:
        logger.error(f"Pipeline submit failed: {e}")
        raise HTTPException(500, f"Pipeline failed: {e}")


@app.get("/api/pipeline/status/{run_id}")
def get_pipeline_status(run_id: int):
    try:
        run = w.jobs.get_run(run_id=run_id)
        tasks_status = []
        if run.tasks:
            for t in run.tasks:
                tasks_status.append({
                    "task_key": t.task_key,
                    "state": t.state.life_cycle_state.value if t.state else "UNKNOWN",
                    "result": t.state.result_state.value if t.state and t.state.result_state else None,
                })
        return {
            "run_id": run_id,
            "state": run.state.life_cycle_state.value if run.state else "UNKNOWN",
            "result": run.state.result_state.value if run.state and run.state.result_state else None,
            "tasks": tasks_status,
        }
    except Exception as e:
        raise HTTPException(500, f"Failed to get status: {e}")


# --- API: Dealers dropdown ---
@app.get("/api/dealers")
def get_dealers():
    rows = run_sql(f"""
        SELECT DISTINCT dealer_id FROM {UC_CATALOG}.{UC_SCHEMA}.gold_complaint_dashboard ORDER BY dealer_id
    """)
    return [r["dealer_id"] for r in rows]


# --- Serve React frontend ---
app.mount("/assets", StaticFiles(directory="frontend/dist/assets"), name="assets")

@app.get("/{full_path:path}")
def serve_frontend(full_path: str):
    return FileResponse("frontend/dist/index.html")
