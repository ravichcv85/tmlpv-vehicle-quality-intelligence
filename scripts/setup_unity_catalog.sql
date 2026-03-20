-- ============================================================
-- Unity Catalog Setup
-- Run these in a Databricks notebook or SQL editor
-- Replace <YOUR_CATALOG> and <YOUR_SCHEMA> with your values
-- ============================================================

-- Step 1: Create catalog and schema
CREATE CATALOG IF NOT EXISTS <YOUR_CATALOG>;
USE CATALOG <YOUR_CATALOG>;
CREATE SCHEMA IF NOT EXISTS <YOUR_SCHEMA>;
USE SCHEMA <YOUR_SCHEMA>;

-- Step 2: Bronze tables (raw ingested from Lakebase)
CREATE TABLE IF NOT EXISTS Customer_Complaints (
    complaint_id BIGINT,
    vehicle_id STRING,
    customer_id STRING,
    complaint_date DATE,
    complaint_category STRING,
    description STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS ePDI_Inspections (
    inspection_id BIGINT,
    vehicle_id STRING,
    dealer_id STRING,
    inspection_date DATE,
    checklist_item STRING,
    status STRING,
    notes STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS ePOD_Delivery (
    delivery_id BIGINT,
    vehicle_id STRING,
    dealer_id STRING,
    delivery_date DATE,
    delivery_condition STRING,
    customer_signature STRING
) USING DELTA;

-- Step 3: Silver and Gold tables are created by the pipeline notebooks
-- Run notebooks in order:
--   1. notebooks/01_lakebase_to_bronze.py
--   2. notebooks/02_bronze_to_silver.sql
--   3. notebooks/03_silver_to_gold.sql
--   4. notebooks/04_ai_checklist_agent.py (optional - requires FMAPI)
--   5. notebooks/05_risk_scorer_setup.py (optional - creates ML model)
