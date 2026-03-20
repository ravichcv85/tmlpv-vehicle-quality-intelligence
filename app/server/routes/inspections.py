"""PDI Tablet - Inspection logging routes."""
import random
from datetime import datetime, date

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.db import lakebase_query, lakebase_execute

router = APIRouter(prefix="/api/inspections", tags=["inspections"])

# Maps frontend checklist items to DB column prefixes
CHECKLIST_ITEMS = [
    {"name": "Engine Bay", "col": "engine_bay"},
    {"name": "Battery & Wiring", "col": "electrical"},
    {"name": "Brake System", "col": "brakes"},
    {"name": "Tyre Condition", "col": "tyres_wheels"},
    {"name": "Exterior Paint", "col": "paint_finish"},
    {"name": "Windshield & Glass", "col": "exterior_body"},
    {"name": "Interior Trim", "col": "exterior_body"},  # mapped to exterior_body
    {"name": "Infotainment System", "col": "infotainment"},
    {"name": "AC & Ventilation", "col": "ac_system"},
    {"name": "Seat Belt & Airbags", "col": "safety_systems"},
    {"name": "Steering & Suspension", "col": "suspension"},
    {"name": "Lights & Indicators", "col": "electrical"},
]

# Unique DB columns to use for insert
DB_CHECKLIST_COLS = [
    "exterior_body", "paint_finish", "engine_bay", "ac_system",
    "brakes", "electrical", "tyres_wheels", "suspension",
    "infotainment", "safety_systems",
]


class ChecklistItem(BaseModel):
    name: str
    status: str  # "Pass", "Fail", "Quick Fix"


class InspectionInput(BaseModel):
    vin: str
    model: str
    variant: str = ""
    dealer_code: str
    dealer_name: str
    inspector_name: str
    checklist: list[ChecklistItem]


class InspectionResponse(BaseModel):
    id: int = 0
    overall_result: str
    items_checked: int
    items_failed: int
    items_quick_fix: int
    risk_score: float
    risk_confidence: float


@router.get("/checklist-items")
async def get_checklist_items():
    """Return the standard PDI checklist items."""
    return [item["name"] for item in CHECKLIST_ITEMS]


def _map_status(status: str) -> tuple[str, str | None]:
    """Map frontend status to DB result and action."""
    if status == "Pass":
        return "PASS", None
    elif status == "Fail":
        return "FAIL", "Needs repair"
    else:  # Quick Fix
        return "QUICK_FIX", "Quick fix applied on site"


@router.post("/submit", response_model=InspectionResponse)
async def submit_inspection(inspection: InspectionInput):
    """Submit a PDI inspection."""
    try:
        items_checked = len(inspection.checklist)
        items_failed = sum(1 for item in inspection.checklist if item.status == "Fail")
        items_quick_fix = sum(1 for item in inspection.checklist if item.status == "Quick Fix")

        # Calculate risk score
        noise = random.uniform(-3, 3)
        risk_score = round(min(100, max(0, items_failed * 15 + items_quick_fix * 5 + noise)), 1)
        risk_confidence = round(0.85 + random.uniform(-0.1, 0.1), 2)

        # Determine overall result
        if items_failed == 0 and items_quick_fix == 0:
            overall_result = "PASS"
        elif items_failed == 0:
            overall_result = "CONDITIONAL_PASS"
        else:
            overall_result = "FAIL"

        now = datetime.utcnow()

        # Map checklist items to DB columns (take worst status per column)
        col_results: dict[str, tuple[str, str | None]] = {}
        for item in CHECKLIST_ITEMS:
            col = item["col"]
            # Find matching frontend item
            match = next((c for c in inspection.checklist if c.name == item["name"]), None)
            if match:
                result, action = _map_status(match.status)
                # Keep the worst status per column
                if col not in col_results or (result == "FAIL") or (result == "QUICK_FIX" and col_results[col][0] == "PASS"):
                    col_results[col] = (result, action)

        # Build column lists for insert
        col_names = ["vin", "dealer_code", "dealer_name", "inspector_name",
                      "inspection_date", "model", "variant",
                      "overall_result", "risk_score", "risk_confidence",
                      "delivery_cleared", "created_at"]
        values = [inspection.vin, inspection.dealer_code, inspection.dealer_name,
                  inspection.inspector_name, now.date().isoformat(), inspection.model,
                  inspection.variant, overall_result, risk_score, risk_confidence,
                  overall_result != "FAIL", now.isoformat()]

        for col in DB_CHECKLIST_COLS:
            result, action = col_results.get(col, ("PASS", None))
            col_names.append(f"{col}_result")
            values.append(result)
            col_names.append(f"{col}_action")
            values.append(action)

        placeholders = ", ".join(["%s"] * len(values))
        col_str = ", ".join(col_names)

        lakebase_execute(
            f"INSERT INTO staging_inspections ({col_str}) VALUES ({placeholders})",
            tuple(values),
        )

        return InspectionResponse(
            overall_result=overall_result,
            items_checked=items_checked,
            items_failed=items_failed,
            items_quick_fix=items_quick_fix,
            risk_score=risk_score,
            risk_confidence=risk_confidence,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recent")
async def get_recent_inspections(limit: int = 20):
    """Get recent inspections from Lakebase."""
    try:
        rows = lakebase_query(
            """SELECT inspection_id, vin, model, variant, dealer_code, dealer_name,
                      inspector_name, inspection_date, overall_result,
                      risk_score, risk_confidence, delivery_cleared, created_at
               FROM staging_inspections ORDER BY created_at DESC LIMIT %s""",
            (limit,),
        )
        for row in rows:
            for k, v in row.items():
                if isinstance(v, (datetime, date)):
                    row[k] = v.isoformat()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
