"""CRM - Complaint logging and management routes."""
from datetime import datetime, date

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.db import lakebase_query, lakebase_execute, query_gold_table
from server.llm import classify_complaint

router = APIRouter(prefix="/api/complaints", tags=["complaints"])


class ComplaintInput(BaseModel):
    vin: str
    model: str
    variant: str = ""
    customer_name: str
    dealer_code: str
    dealer_name: str
    severity: str
    description: str


class ComplaintResponse(BaseModel):
    id: int = 0
    category: str
    subcategory: str
    ai_confidence: float
    reasoning: str
    status: str


@router.post("/submit", response_model=ComplaintResponse)
async def submit_complaint(complaint: ComplaintInput):
    """Submit a new complaint with AI classification."""
    try:
        # Classify using LLM
        classification = classify_complaint(
            complaint.description, complaint.model, complaint.severity
        )

        now = datetime.utcnow()
        ai_cat = classification.get("category", "Uncategorized")
        ai_sub = classification.get("subcategory", "Unknown")
        ai_conf = classification.get("confidence", 0.0)

        # Insert into Lakebase (complaint_id is auto-increment)
        lakebase_execute(
            """INSERT INTO staging_complaints
               (vin, customer_name, dealer_code, dealer_name, complaint_date,
                description, category, subcategory, ai_category, ai_subcategory,
                ai_confidence, severity, status, model, variant, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                complaint.vin,
                complaint.customer_name,
                complaint.dealer_code,
                complaint.dealer_name,
                now.date().isoformat(),
                complaint.description,
                ai_cat,
                ai_sub,
                ai_cat,
                ai_sub,
                ai_conf,
                complaint.severity,
                "Open",
                complaint.model,
                complaint.variant,
                now.isoformat(),
            ),
        )

        return ComplaintResponse(
            category=ai_cat,
            subcategory=ai_sub,
            ai_confidence=ai_conf,
            reasoning=classification.get("reasoning", ""),
            status="Open",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recent")
async def get_recent_complaints(limit: int = 20):
    """Get recent complaints from Lakebase."""
    try:
        rows = lakebase_query(
            "SELECT * FROM staging_complaints ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
        # Convert date/datetime objects to strings for JSON serialization
        for row in rows:
            for k, v in row.items():
                if isinstance(v, (datetime, date)):
                    row[k] = v.isoformat()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trends")
async def get_complaint_trends():
    """Get complaint trends from gold table."""
    try:
        rows = query_gold_table("gold_complaint_trends", limit=200)
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
