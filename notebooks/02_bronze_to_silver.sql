-- Databricks notebook source
-- Bronze to Silver Pipeline
-- cvr_tm_demo: Customer Complaint Correlation & ePDI Gap Analysis
-- Transforms bronze tables into curated silver tables

-- ============================================================
-- Task 1: Silver Complaint Correlation
-- One row per complaint (no JOIN fanout)
-- Uses CTE to deduplicate inspections and deliveries per vehicle
-- Data quality guard: only complaints filed AFTER delivery (days_since_delivery >= 1)
-- ============================================================

CREATE OR REPLACE TABLE cvr_dev_ai_kit.cvr_tm_demo.silver_complaint_correlation AS
WITH latest_delivery AS (
  SELECT
    vehicle_id, delivery_id, dealer_id, delivery_date, delivery_condition,
    ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY delivery_date DESC) AS rn
  FROM cvr_dev_ai_kit.cvr_tm_demo.ePOD_Delivery
),
inspection_summary AS (
  SELECT
    vehicle_id,
    CASE WHEN MAX(CASE WHEN UPPER(status) = 'FAIL' THEN 1 ELSE 0 END) = 1
         THEN 'FAIL' ELSE 'PASS' END                              AS inspection_status,
    COUNT(CASE WHEN UPPER(status) = 'FAIL' THEN 1 END)            AS fail_count,
    COLLECT_SET(CASE WHEN UPPER(status) = 'FAIL' THEN checklist_item END) AS failed_items,
    MAX(inspection_date)                                           AS latest_inspection_date
  FROM cvr_dev_ai_kit.cvr_tm_demo.ePDI_Inspections
  GROUP BY vehicle_id
)
SELECT
  c.complaint_id,
  c.vehicle_id,
  c.customer_id,
  c.complaint_date,
  c.complaint_category,
  c.description,
  i.inspection_status,
  i.fail_count,
  i.failed_items,
  i.latest_inspection_date,
  d.delivery_id,
  d.dealer_id,
  d.delivery_date,
  d.delivery_condition,
  DATEDIFF(c.complaint_date, d.delivery_date) AS days_since_delivery
FROM cvr_dev_ai_kit.cvr_tm_demo.Customer_Complaints c
LEFT JOIN inspection_summary i  ON c.vehicle_id = i.vehicle_id
LEFT JOIN latest_delivery d     ON c.vehicle_id = d.vehicle_id AND d.rn = 1
-- Data quality guard: exclude complaints filed before or on delivery date
WHERE d.delivery_date IS NULL OR DATEDIFF(c.complaint_date, d.delivery_date) >= 1;

-- ============================================================
-- Task 2: Silver ePDI Gaps
-- Complaint categories NOT covered by any ePDI checklist item
-- ============================================================

CREATE OR REPLACE TABLE cvr_dev_ai_kit.cvr_tm_demo.silver_epdi_gaps AS
SELECT DISTINCT
  c.complaint_category,
  COUNT(c.complaint_id)          AS complaint_count,
  COLLECT_SET(c.vehicle_id)      AS affected_vehicles,
  MIN(c.complaint_date)          AS first_reported,
  MAX(c.complaint_date)          AS last_reported
FROM cvr_dev_ai_kit.cvr_tm_demo.Customer_Complaints c
WHERE c.complaint_category NOT IN (
  SELECT DISTINCT checklist_item FROM cvr_dev_ai_kit.cvr_tm_demo.ePDI_Inspections
)
GROUP BY c.complaint_category;

-- ============================================================
-- Verification
-- ============================================================
SELECT
  (SELECT COUNT(*) FROM cvr_dev_ai_kit.cvr_tm_demo.silver_complaint_correlation) AS corr_rows,
  (SELECT COUNT(DISTINCT complaint_id) FROM cvr_dev_ai_kit.cvr_tm_demo.silver_complaint_correlation) AS distinct_complaints,
  (SELECT COUNT(*) FROM cvr_dev_ai_kit.cvr_tm_demo.silver_epdi_gaps) AS gap_rows,
  (SELECT COUNT(*) FROM cvr_dev_ai_kit.cvr_tm_demo.silver_complaint_correlation WHERE days_since_delivery <= 0) AS bad_rows_remaining;
