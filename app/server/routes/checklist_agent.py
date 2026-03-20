"""AI Checklist Agent - Gap analysis routes."""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.db import query_gold_table, run_sql_warehouse_query
from server.config import QUALITY_SCHEMA
from server.llm import generate_gap_recommendation

router = APIRouter(prefix="/api/checklist-agent", tags=["checklist-agent"])


class GapAnalysisRequest(BaseModel):
    category: str
    subcategory: str = ""


class GapAnalysisResponse(BaseModel):
    gap_data: list[dict]
    recommendation: str


@router.get("/categories")
async def get_categories():
    """Get distinct categories/subcategories from gap analysis table."""
    try:
        rows = run_sql_warehouse_query(
            f"SELECT DISTINCT category, subcategory FROM {QUALITY_SCHEMA}.gold_checklist_gap_analysis ORDER BY category, subcategory"
        )
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze", response_model=GapAnalysisResponse)
async def analyze_gap(request: GapAnalysisRequest):
    """Run gap analysis for a category and get AI recommendation."""
    try:
        where = f"category = '{request.category}'"
        if request.subcategory:
            where += f" AND subcategory = '{request.subcategory}'"

        gap_data = query_gold_table("gold_checklist_gap_analysis", where=where, limit=50)

        if not gap_data:
            return GapAnalysisResponse(
                gap_data=[],
                recommendation="No gap data found for this category/subcategory combination.",
            )

        recommendation = generate_gap_recommendation(
            request.category, request.subcategory, gap_data
        )

        return GapAnalysisResponse(gap_data=gap_data, recommendation=recommendation)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/full-table")
async def get_full_gap_table():
    """Get the full checklist gap analysis table."""
    try:
        rows = query_gold_table("gold_checklist_gap_analysis", limit=200)
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
