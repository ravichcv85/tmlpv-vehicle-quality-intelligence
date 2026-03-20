"""Metrics - Summary data for dashboard cards."""
from fastapi import APIRouter, HTTPException

from server.db import run_sql_warehouse_query, query_gold_table
from server.config import QUALITY_SCHEMA

router = APIRouter(prefix="/api/metrics", tags=["metrics"])


@router.get("/summary")
async def get_summary_metrics():
    """Get summary metrics across all gold tables for dashboard cards."""
    try:
        # Complaint trends summary
        complaint_summary = run_sql_warehouse_query(f"""
            SELECT
                COALESCE(SUM(CAST(complaint_count AS INT)), 0) as total_complaints,
                COALESCE(SUM(CAST(open_count AS INT)), 0) as open_complaints,
                COALESCE(SUM(CAST(resolved_count AS INT)), 0) as resolved_complaints,
                COALESCE(ROUND(AVG(CAST(avg_ai_confidence AS DOUBLE)), 2), 0) as avg_ai_confidence
            FROM {QUALITY_SCHEMA}.gold_complaint_trends
        """)

        # Dealer performance summary
        dealer_summary = run_sql_warehouse_query(f"""
            SELECT
                COUNT(*) as total_dealers,
                COALESCE(SUM(CAST(total_inspections AS INT)), 0) as total_inspections,
                COALESCE(ROUND(AVG(CAST(complaint_rate_pct AS DOUBLE)), 1), 0) as avg_complaint_rate,
                COALESCE(ROUND(AVG(CAST(fail_rate_pct AS DOUBLE)), 1), 0) as avg_fail_rate
            FROM {QUALITY_SCHEMA}.gold_dealer_performance
        """)

        # Vehicle risk summary
        risk_summary = run_sql_warehouse_query(f"""
            SELECT
                COUNT(*) as total_vehicles,
                COALESCE(SUM(CASE WHEN risk_level = 'High' THEN 1 ELSE 0 END), 0) as high_risk,
                COALESCE(SUM(CASE WHEN risk_level = 'Medium' THEN 1 ELSE 0 END), 0) as medium_risk,
                COALESCE(SUM(CASE WHEN risk_level = 'Low' THEN 1 ELSE 0 END), 0) as low_risk
            FROM {QUALITY_SCHEMA}.gold_vehicle_risk_profile
        """)

        # Gap analysis summary
        gap_summary = run_sql_warehouse_query(f"""
            SELECT
                COUNT(*) as total_gaps,
                COALESCE(SUM(CASE WHEN gap_classification = 'Critical Gap' THEN 1 ELSE 0 END), 0) as critical_gaps,
                COALESCE(ROUND(AVG(CAST(pdi_catch_rate_pct AS DOUBLE)), 1), 0) as avg_catch_rate
            FROM {QUALITY_SCHEMA}.gold_checklist_gap_analysis
        """)

        return {
            "complaints": complaint_summary[0] if complaint_summary else {},
            "dealers": dealer_summary[0] if dealer_summary else {},
            "vehicles": risk_summary[0] if risk_summary else {},
            "gaps": gap_summary[0] if gap_summary else {},
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dealers")
async def get_dealers():
    """Get dealer list for dropdowns."""
    try:
        rows = run_sql_warehouse_query(f"""
            SELECT DISTINCT dealer_code, dealer_name
            FROM {QUALITY_SCHEMA}.gold_dealer_performance
            ORDER BY dealer_name
        """)
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
