# Databricks notebook source
# MAGIC %md
# MAGIC # CVR Defect Risk Scorer — Setup
# MAGIC Creates UC SQL function, MLflow pyfunc model, registers in UC, deploys serving endpoint.

# COMMAND ----------
# MAGIC %pip install mlflow databricks-sdk --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
import mlflow
import pandas as pd
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec, ParamSchema, ParamSpec
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

CATALOG = "cvr_dev_ai_kit"
SCHEMA  = "cvr_tm_demo"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.defect_risk_scorer"
ENDPOINT_NAME = "cvr-defect-risk-scorer"

# COMMAND ----------
# MAGIC %md ## Step 1: UC SQL Function

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.score_defect_risk(
  fail_count          INT     COMMENT 'Number of failed inspection items',
  has_engine_fail     BOOLEAN COMMENT 'Engine & Powertrain failed',
  has_brake_fail      BOOLEAN COMMENT 'Brakes & Suspension failed',
  has_electrical_fail BOOLEAN COMMENT 'Electrical & Infotainment failed',
  dealer_id           STRING  COMMENT 'Dealer identifier',
  delivery_condition  STRING  COMMENT 'Vehicle delivery condition: Good/Fair/Poor'
)
RETURNS DOUBLE
LANGUAGE SQL
COMMENT 'Rule-based defect risk scorer. Returns probability 0.0-1.0 that vehicle will generate a complaint within 60 days. Calibrated from DLR-003/002/004 complaint data. Replace model version with AutoML classifier when fleet data exceeds 1000 vehicles.'
AS $$
  LEAST(
    -- Base score from fail count
    CASE
      WHEN fail_count = 0                                                    THEN 0.05
      WHEN fail_count = 1 AND NOT has_engine_fail AND NOT has_brake_fail     THEN 0.22
      WHEN fail_count = 1 AND (has_engine_fail OR has_brake_fail)            THEN 0.38
      WHEN fail_count = 2 AND NOT has_engine_fail AND NOT has_brake_fail     THEN 0.44
      WHEN fail_count = 2 AND (has_engine_fail OR has_brake_fail)            THEN 0.60
      WHEN fail_count >= 3 AND (has_engine_fail OR has_brake_fail)           THEN 0.82
      WHEN fail_count >= 3                                                   THEN 0.68
      ELSE 0.10
    END
    -- Electrical compound risk (adds to base)
    + CASE WHEN has_electrical_fail THEN 0.06 ELSE 0.0 END
    -- Dealer risk premium (DLR-003/002/004 have highest complaint rates)
    + CASE WHEN dealer_id IN ('DLR-003','DLR-002','DLR-004') THEN 0.08 ELSE 0.0 END
    -- Delivery condition penalty
    + CASE WHEN delivery_condition IN ('Fair','Poor')         THEN 0.06 ELSE 0.0 END
  , 0.97)
$$
""")
print(f"✅ UC SQL function created: {CATALOG}.{SCHEMA}.score_defect_risk")

# COMMAND ----------
# Quick sanity check
result = spark.sql(f"""
SELECT
  {CATALOG}.{SCHEMA}.score_defect_risk(0, false, false, false, 'DLR-001', 'Good') AS all_pass,
  {CATALOG}.{SCHEMA}.score_defect_risk(1, false, false, false, 'DLR-001', 'Good') AS one_fail,
  {CATALOG}.{SCHEMA}.score_defect_risk(2, true,  false, false, 'DLR-003', 'Good') AS engine_fail_high_risk_dealer,
  {CATALOG}.{SCHEMA}.score_defect_risk(3, true,  true,  true,  'DLR-003', 'Fair') AS worst_case
""").collect()[0]

print(f"All PASS:                    {result.all_pass:.0%}")
print(f"1 fail (non-critical):       {result.one_fail:.0%}")
print(f"Engine fail + DLR-003:       {result.engine_fail_high_risk_dealer:.0%}")
print(f"3 fails + critical + DLR-003:{result.worst_case:.0%}")

# COMMAND ----------
# MAGIC %md ## Step 2: MLflow PythonModel

class DefectRiskScorer(mlflow.pyfunc.PythonModel):
    """
    Rule-based defect risk scorer for CVR vehicle pre-delivery inspections.
    Identical logic to the UC SQL function score_defect_risk().
    Designed to be replaced by AutoML classifier (same API contract) once
    fleet data exceeds ~1000 labelled vehicles.

    Input columns: fail_count, failed_items (list), dealer_id, delivery_condition
    Output columns: risk_score, risk_level, top_risk_factors (list)
    """

    HIGH_RISK_DEALERS = {'DLR-003', 'DLR-002', 'DLR-004'}
    HIGH_RISK_ITEMS   = {
        'Engine & Powertrain':      0.16,
        'Brakes & Suspension':      0.14,
        'Electrical & Infotainment': 0.06,
    }

    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        results = []
        for _, row in model_input.iterrows():
            score, factors = self._score(row)
            results.append({
                'risk_score':       round(min(score, 0.97), 2),
                'risk_level':       'HIGH' if score >= 0.70 else 'MEDIUM' if score >= 0.35 else 'LOW',
                'top_risk_factors': factors,
            })
        return pd.DataFrame(results)

    def _score(self, row):
        fail_count       = int(row.get('fail_count', 0))
        failed_items     = list(row.get('failed_items') or [])
        dealer_id        = str(row.get('dealer_id', ''))
        condition        = str(row.get('delivery_condition', 'Good'))

        factors = []

        # Base score from fail count
        base_map = {0: 0.05, 1: 0.22, 2: 0.44}
        score = base_map.get(min(fail_count, 2), 0.68)

        if fail_count == 1:
            factors.append("1 inspection item failed")
        elif fail_count == 2:
            factors.append("2 inspection items failed")
        elif fail_count >= 3:
            factors.append(f"{fail_count} inspection items failed — above safe threshold")

        # Critical item bonus
        for item, penalty in self.HIGH_RISK_ITEMS.items():
            if item in failed_items:
                score += penalty
                label = {
                    'Engine & Powertrain':       'Engine/Powertrain fault — high recurrence risk',
                    'Brakes & Suspension':        'Brakes/Suspension fault — safety-critical',
                    'Electrical & Infotainment': 'Electrical fault — known software recurrence',
                }[item]
                factors.append(label)

        # Dealer risk premium
        if dealer_id in self.HIGH_RISK_DEALERS:
            score += 0.08
            factors.append(f"Dealer {dealer_id} — elevated historical complaint rate")

        # Delivery condition
        if condition in ('Fair', 'Poor'):
            score += 0.06
            factors.append(f"Vehicle delivery condition: {condition}")

        return score, factors[:3]

# COMMAND ----------
# MAGIC %md ## Step 3: Register model in Unity Catalog

mlflow.set_registry_uri("databricks-uc")

input_schema = Schema([
    ColSpec("integer", "fail_count"),
    ColSpec("string",  "failed_items"),   # JSON-serialised list
    ColSpec("string",  "dealer_id"),
    ColSpec("string",  "delivery_condition"),
])
output_schema = Schema([
    ColSpec("double", "risk_score"),
    ColSpec("string", "risk_level"),
    ColSpec("string", "top_risk_factors"),  # JSON-serialised list
])
signature = ModelSignature(inputs=input_schema, outputs=output_schema)

with mlflow.start_run(run_name="defect_risk_scorer_v1"):
    model_info = mlflow.pyfunc.log_model(
        artifact_path="defect_risk_scorer",
        python_model=DefectRiskScorer(),
        signature=signature,
        registered_model_name=MODEL_NAME,
        pip_requirements=["mlflow", "pandas"],
    )
    print(f"✅ Model logged: {model_info.model_uri}")
    print(f"✅ Registered:   {MODEL_NAME}")

# COMMAND ----------
# Get the latest version
from mlflow.tracking import MlflowClient
client = MlflowClient(registry_uri="databricks-uc")
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest_version = max(int(v.version) for v in versions)
print(f"Latest model version: {latest_version}")

# COMMAND ----------
# MAGIC %md ## Step 4: Deploy Model Serving Endpoint

w = WorkspaceClient()

# Check if endpoint already exists
try:
    existing = w.serving_endpoints.get(ENDPOINT_NAME)
    print(f"Endpoint '{ENDPOINT_NAME}' already exists — updating config...")
    w.serving_endpoints.update_config(
        name=ENDPOINT_NAME,
        served_entities=[ServedEntityInput(
            entity_name=MODEL_NAME,
            entity_version=str(latest_version),
            scale_to_zero_enabled=True,
            workload_size="Small",
        )]
    )
    print(f"✅ Endpoint updated: {ENDPOINT_NAME}")
except Exception:
    print(f"Creating new endpoint '{ENDPOINT_NAME}'...")
    w.serving_endpoints.create_and_wait(
        name=ENDPOINT_NAME,
        config=EndpointCoreConfigInput(
            served_entities=[ServedEntityInput(
                entity_name=MODEL_NAME,
                entity_version=str(latest_version),
                scale_to_zero_enabled=True,
                workload_size="Small",
            )]
        ),
    )
    print(f"✅ Endpoint deployed: {ENDPOINT_NAME}")

# COMMAND ----------
# MAGIC %md ## Step 5: Smoke test the endpoint

import requests, json

ctx    = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host   = f"https://{ctx.browserHostName().get()}"
token  = ctx.apiToken().get()

test_payload = {
    "dataframe_records": [
        # All pass — should be LOW
        {"fail_count": 0, "failed_items": "[]", "dealer_id": "DLR-001", "delivery_condition": "Good"},
        # 2 critical fails at high-risk dealer — should be HIGH
        {"fail_count": 2, "failed_items": '["Engine & Powertrain","Brakes & Suspension"]', "dealer_id": "DLR-003", "delivery_condition": "Good"},
        # 3 fails worst case — should be HIGH
        {"fail_count": 3, "failed_items": '["Engine & Powertrain","Brakes & Suspension","Electrical & Infotainment"]', "dealer_id": "DLR-003", "delivery_condition": "Fair"},
    ]
}

resp = requests.post(
    f"{host}/serving-endpoints/{ENDPOINT_NAME}/invocations",
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    json=test_payload,
    timeout=60,
)
print(f"HTTP {resp.status_code}")
if resp.ok:
    for pred in resp.json().get("predictions", []):
        level = pred['risk_level']
        score = pred['risk_score']
        factors = pred['top_risk_factors']
        print(f"  {level:6s} ({score:.0%}) — {factors}")
else:
    print(resp.text[:500])

print("\n✅ Layer 3 setup complete!")
print(f"   UC Function : {CATALOG}.{SCHEMA}.score_defect_risk")
print(f"   UC Model    : {MODEL_NAME} v{latest_version}")
print(f"   Endpoint    : {ENDPOINT_NAME}")
