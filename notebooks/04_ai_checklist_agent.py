# Databricks notebook source
# MAGIC %md
# MAGIC ## CVR AI Checklist Agent
# MAGIC Reads complaint gaps from gold_checklist_recommendations, calls LLM for each gap,
# MAGIC and writes specific actionable procedures to gold_checklist_recommendations_ai.

# COMMAND ----------

import json
import requests
import time

# Databricks SDK for auth
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
HOST  = w.config.host if w.config.host.startswith("http") else f"https://{w.config.host}"
TOKEN = w.config.authenticate().get("Authorization", "").replace("Bearer ", "")

CATALOG  = "cvr_dev_ai_kit"
SCHEMA   = "cvr_tm_demo"
WH_ID    = "148ccb90800933a1"
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

print(f"Agent starting — host: {HOST}")

# COMMAND ----------

def sql_exec(stmt: str, fetch: bool = False):
    """Execute SQL via warehouse and optionally return rows as list of dicts."""
    resp = requests.post(
        f"{HOST}/api/2.0/sql/statements/",
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        json={
            "statement": stmt,
            "warehouse_id": WH_ID,
            "wait_timeout": "60s",
            "format": "JSON_ARRAY",
            "disposition": "INLINE",
        },
        timeout=90,
    )
    resp.raise_for_status()
    data = resp.json()

    if data.get("status", {}).get("state") == "FAILED":
        raise RuntimeError(f"SQL failed: {data['status'].get('error', {}).get('message', 'unknown')}")

    if not fetch:
        return None

    cols = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = data.get("result", {}).get("data_array", [])
    return [dict(zip(cols, r)) for r in rows]


def call_llm(system_prompt: str, user_prompt: str) -> str:
    """Call Foundation Model API."""
    resp = requests.post(
        f"{HOST}/serving-endpoints/{LLM_ENDPOINT}/invocations",
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        json={
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            "max_tokens": 250,
            "temperature": 0.3,
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


# COMMAND ----------
# MAGIC %md ### Step 1: Read current recommendations

rows = sql_exec("""
    SELECT
        what_customers_complain_about,
        total_complaints,
        caught_by_inspection,
        pct_caught_by_inspection,
        current_checklist_item,
        specific_action_required
    FROM cvr_dev_ai_kit.cvr_tm_demo.gold_checklist_recommendations
    ORDER BY total_complaints DESC
""", fetch=True)

print(f"Found {len(rows)} complaint categories to process")
for r in rows:
    print(f"  {r['what_customers_complain_about']}: {r['total_complaints']} complaints, {r['pct_caught_by_inspection']}% caught")

# COMMAND ----------
# MAGIC %md ### Step 2: Call LLM for each gap

SYSTEM_PROMPT = """You are a senior vehicle quality engineer helping improve Pre-Delivery Inspection (ePDI) checklists for an Indian automotive brand.

Your job: given a complaint category and its data, write a specific, testable inspection procedure.
Rules:
- Be concrete: include exact measurements, thresholds, tools
- Be brief: 2-3 sentences max
- Include a clear pass/fail criterion
- Suggest mandatory corrective action on fail (replace, not just clean/adjust for critical items)
- Write as an instruction to a technician, not a manager
- No preamble, no "I suggest", just the procedure"""


enhanced_rows = []

for i, row in enumerate(rows):
    issue  = row["what_customers_complain_about"]
    total  = row["total_complaints"]
    caught = row["caught_by_inspection"]
    pct    = row["pct_caught_by_inspection"]
    orig   = row["specific_action_required"]

    gap_pct = 100 - float(pct) if pct else 100

    prompt = f"""Complaint: {issue}
Complaints filed: {total}
Caught at PDI: {caught} ({pct}% catch rate) — {gap_pct:.0f}% slipping through
Current checklist action: {orig}

Improve this with a specific, testable procedure."""

    print(f"\n[{i+1}/{len(rows)}] Enhancing: {issue}...")
    try:
        ai_rec = call_llm(SYSTEM_PROMPT, prompt)
        print(f"  → {ai_rec[:100]}...")
    except Exception as e:
        print(f"  ⚠ LLM failed, using original: {e}")
        ai_rec = orig

    enhanced_rows.append({
        "what_customers_complain_about": issue,
        "total_complaints":             int(total),
        "caught_by_inspection":         int(caught),
        "pct_caught_by_inspection":     float(pct) if pct else 0.0,
        "current_checklist_item":       row.get("current_checklist_item") or "NONE",
        "original_action":              orig,
        "ai_recommendation":            ai_rec,
    })
    time.sleep(0.5)   # avoid rate limiting

print(f"\n✓ Enhanced {len(enhanced_rows)} recommendations")

# COMMAND ----------
# MAGIC %md ### Step 3: Create/replace AI recommendations table

sql_exec(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.gold_checklist_recommendations_ai (
    what_customers_complain_about STRING,
    total_complaints              BIGINT,
    caught_by_inspection          BIGINT,
    pct_caught_by_inspection      DOUBLE,
    current_checklist_item        STRING,
    original_action               STRING,
    ai_recommendation             STRING,
    priority                      INT,
    generated_at                  TIMESTAMP
)
USING DELTA
COMMENT 'AI-enhanced ePDI checklist recommendations — updated by ai_checklist_agent'
""")

# Truncate first (simpler than MERGE for a demo-scale table)
sql_exec(f"DELETE FROM {CATALOG}.{SCHEMA}.gold_checklist_recommendations_ai")

# COMMAND ----------
# MAGIC %md ### Step 4: Insert enhanced rows

for idx, row in enumerate(enhanced_rows):
    # Escape single quotes for SQL
    def esc(s): return str(s).replace("'", "''")

    sql_exec(f"""
        INSERT INTO {CATALOG}.{SCHEMA}.gold_checklist_recommendations_ai
        (what_customers_complain_about, total_complaints, caught_by_inspection,
         pct_caught_by_inspection, current_checklist_item, original_action,
         ai_recommendation, priority, generated_at)
        VALUES (
            '{esc(row["what_customers_complain_about"])}',
            {row["total_complaints"]},
            {row["caught_by_inspection"]},
            {row["pct_caught_by_inspection"]},
            '{esc(row["current_checklist_item"])}',
            '{esc(row["original_action"])}',
            '{esc(row["ai_recommendation"])}',
            {idx + 1},
            CURRENT_TIMESTAMP()
        )
    """)

print(f"✓ Wrote {len(enhanced_rows)} AI-enhanced recommendations to {CATALOG}.{SCHEMA}.gold_checklist_recommendations_ai")

# COMMAND ----------
# MAGIC %md ### Done

print("\n=== Agent Complete ===")
for row in enhanced_rows[:3]:
    print(f"\nIssue: {row['what_customers_complain_about']}")
    print(f"  Original : {row['original_action'][:80]}...")
    print(f"  AI       : {row['ai_recommendation'][:80]}...")
