-- Databricks notebook source
-- Silver to Gold Pipeline
-- cvr_tm_demo: Business-ready gold views from curated silver tables
-- Runs after bronze_to_silver_pipeline completes

-- ============================================================
-- Task 1: gold_complaint_dashboard
-- Complaint trends + dealer accountability scoring
-- Source: silver_complaint_correlation
-- ============================================================

CREATE OR REPLACE TABLE cvr_dev_ai_kit.cvr_tm_demo.gold_complaint_dashboard AS
SELECT
  sc.dealer_id,
  sc.complaint_category,
  DATE_TRUNC('month', sc.complaint_date)                        AS complaint_month,
  COUNT(sc.complaint_id)                                        AS total_complaints,
  COUNT(DISTINCT sc.vehicle_id)                                 AS affected_vehicles,
  COUNT(DISTINCT sc.customer_id)                                AS affected_customers,
  ROUND(AVG(sc.days_since_delivery), 1)                         AS avg_days_since_delivery,
  MIN(sc.days_since_delivery)                                   AS min_days_since_delivery,
  MAX(sc.days_since_delivery)                                   AS max_days_since_delivery,
  COUNT(CASE WHEN sc.inspection_status = 'FAIL' THEN 1 END)    AS complaints_with_failed_inspection,
  ROUND(
    COUNT(CASE WHEN sc.inspection_status = 'FAIL' THEN 1 END)
    * 100.0 / COUNT(*), 1
  )                                                             AS pct_with_failed_inspection,
  COUNT(CASE WHEN sc.days_since_delivery <= 7  THEN 1 END)     AS complaints_within_7days,
  COUNT(CASE WHEN sc.days_since_delivery <= 30 THEN 1 END)     AS complaints_within_30days,
  CASE
    WHEN COUNT(DISTINCT sc.vehicle_id) >= 3
     AND ROUND(COUNT(CASE WHEN sc.inspection_status = 'FAIL'
                     THEN 1 END) * 100.0 / COUNT(*), 1) >= 50
    THEN 'High Risk Dealer'
    WHEN COUNT(DISTINCT sc.vehicle_id) >= 2 THEN 'Monitor'
    ELSE 'Normal'
  END                                                           AS dealer_risk_flag
FROM cvr_dev_ai_kit.cvr_tm_demo.silver_complaint_correlation sc
WHERE sc.dealer_id IS NOT NULL
GROUP BY sc.dealer_id, sc.complaint_category, DATE_TRUNC('month', sc.complaint_date);

-- ============================================================
-- Task 2: gold_epdi_gap_analysis
-- Which complaint categories are NOT in the ePDI checklist
-- Source: silver_epdi_gaps
-- ============================================================

CREATE OR REPLACE TABLE cvr_dev_ai_kit.cvr_tm_demo.gold_epdi_gap_analysis AS
SELECT
  sg.complaint_category                                         AS gap_category,
  sg.complaint_count,
  SIZE(sg.affected_vehicles)                                    AS vehicle_count,
  sg.affected_vehicles,
  sg.first_reported,
  sg.last_reported,
  DATEDIFF(sg.last_reported, sg.first_reported)                 AS gap_duration_days,
  CASE
    WHEN sg.complaint_count >= 15 THEN 'Critical'
    WHEN sg.complaint_count >= 8  THEN 'High'
    WHEN sg.complaint_count >= 4  THEN 'Medium'
    ELSE 'Low'
  END AS severity,
  CASE
    WHEN sg.complaint_count >= 15 THEN 'Mandatory - add to ePDI checklist immediately'
    WHEN sg.complaint_count >= 8  THEN 'Recommended - schedule for next ePDI revision'
    WHEN sg.complaint_count >= 4  THEN 'Optional - monitor for 30 days'
    ELSE 'Watchlist - insufficient data'
  END AS action_required,
  CONCAT('Add [', sg.complaint_category, '] to ePDI checklist (affects ',
         SIZE(sg.affected_vehicles), ' vehicles)')              AS recommendation
FROM cvr_dev_ai_kit.cvr_tm_demo.silver_epdi_gaps sg
ORDER BY sg.complaint_count DESC;

-- ============================================================
-- Task 3: gold_delivery_service_link
-- Post-delivery complaints tied to ePOD delivery conditions
-- Source: silver_complaint_correlation
-- ============================================================

CREATE OR REPLACE TABLE cvr_dev_ai_kit.cvr_tm_demo.gold_delivery_service_link AS
SELECT
  sc.dealer_id,
  sc.delivery_condition,
  sc.complaint_category,
  COUNT(sc.complaint_id)                                         AS complaint_count,
  COUNT(DISTINCT sc.vehicle_id)                                  AS vehicle_count,
  ROUND(AVG(sc.days_since_delivery), 1)                          AS avg_days_to_complaint,
  COUNT(CASE WHEN sc.inspection_status = 'FAIL'          THEN 1 END) AS known_inspection_failures,
  COUNT(CASE WHEN sc.delivery_condition = 'Damage'       THEN 1 END) AS damage_at_delivery,
  COUNT(CASE WHEN sc.delivery_condition = 'Signature Withheld'
             THEN 1 END)                                         AS signature_withheld,
  CASE
    WHEN sc.delivery_condition IN ('Damage','Signature Withheld')
     AND sc.inspection_status = 'FAIL'  THEN 'High Risk'
    WHEN sc.delivery_condition IN ('Damage','Signature Withheld')
     OR  sc.inspection_status = 'FAIL'  THEN 'Medium Risk'
    ELSE 'Low Risk'
  END AS risk_level
FROM cvr_dev_ai_kit.cvr_tm_demo.silver_complaint_correlation sc
WHERE sc.dealer_id IS NOT NULL
GROUP BY
  sc.dealer_id, sc.delivery_condition, sc.complaint_category,
  CASE
    WHEN sc.delivery_condition IN ('Damage','Signature Withheld')
     AND sc.inspection_status = 'FAIL'  THEN 'High Risk'
    WHEN sc.delivery_condition IN ('Damage','Signature Withheld')
     OR  sc.inspection_status = 'FAIL'  THEN 'Medium Risk'
    ELSE 'Low Risk'
  END;

-- ============================================================
-- Task 4: gold_complaint_detail
-- One row per complaint with specific sub-issue classification
-- and checklist coverage gap analysis
-- Source: silver_complaint_correlation + Customer_Complaints
-- ============================================================

CREATE OR REPLACE TABLE cvr_dev_ai_kit.cvr_tm_demo.gold_complaint_detail AS
SELECT
  c.complaint_id,
  c.vehicle_id,
  c.complaint_category,
  c.description,
  sc.inspection_status,
  sc.failed_items,
  sc.delivery_condition,
  sc.days_since_delivery,
  sc.dealer_id,
  CASE
    WHEN c.description LIKE '%infotainment%' OR c.description LIKE '%touchscreen%' OR c.description LIKE '%Infotainment%' THEN 'Infotainment System'
    WHEN c.description LIKE '%Bluetooth%' OR c.description LIKE '%bluetooth%' THEN 'Bluetooth Connectivity'
    WHEN c.description LIKE '%ADAS%' OR c.description LIKE '%collision%' OR c.description LIKE '%lane departure%' OR c.description LIKE '%emergency braking%' THEN 'ADAS / Safety Systems'
    WHEN c.description LIKE '%camera%' OR c.description LIKE '%parking sensor%' OR c.description LIKE '%radar%' THEN 'Parking Aids & Camera'
    WHEN c.description LIKE '%AC%' OR c.description LIKE '%air condition%' OR c.description LIKE '%climate%' OR c.description LIKE '%cooling%' THEN 'Climate Control / AC'
    WHEN c.description LIKE '%USB%' OR c.description LIKE '%charging%' OR c.description LIKE '%wireless%' THEN 'Charging & Connectivity Ports'
    WHEN c.description LIKE '%GPS%' OR c.description LIKE '%navigation%' OR c.description LIKE '%OTA%' THEN 'Software / Navigation'
    WHEN c.description LIKE '%engine stall%' OR c.description LIKE '%Engine stall%' OR c.description LIKE '%cutting out%' THEN 'Engine Cold Start & Idle Stability'
    WHEN c.description LIKE '%engine noise%' OR c.description LIKE '%Engine noise%' OR c.description LIKE '%vibration%' OR c.description LIKE '%humming%' THEN 'Engine NVH (Noise / Vibration)'
    WHEN c.description LIKE '%oil%' OR c.description LIKE '%Oil%' THEN 'Engine Oil Consumption & Leaks'
    WHEN c.description LIKE '%brake%' OR c.description LIKE '%Brake%' OR c.description LIKE '%ABS%' THEN 'Brake System & ABS'
    WHEN c.description LIKE '%suspension%' OR c.description LIKE '%Suspension%' OR c.description LIKE '%clunk%' OR c.description LIKE '%steering%' THEN 'Suspension & Steering'
    WHEN c.description LIKE '%gearbox%' OR c.description LIKE '%gear%' OR c.description LIKE '%clutch%' OR c.description LIKE '%drivetrain%' THEN 'Transmission & Drivetrain'
    WHEN c.description LIKE '%fuel%' OR c.description LIKE '%Fuel%' THEN 'Fuel System'
    WHEN c.description LIKE '%paint%' OR c.description LIKE '%Paint%' OR c.description LIKE '%rust%' OR c.description LIKE '%colour mismatch%' THEN 'Paint & Surface Finish'
    WHEN c.description LIKE '%dent%' OR c.description LIKE '%scratch%' OR c.description LIKE '%kerb rash%' THEN 'Body Damage (Dents / Scratches)'
    WHEN c.description LIKE '%trim%' OR c.description LIKE '%Trim%' OR c.description LIKE '%rattle%' OR c.description LIKE '%roof lining%' THEN 'Interior Trim & Fittings'
    WHEN c.description LIKE '%headlight%' OR c.description LIKE '%condensation%' OR c.description LIKE '%spoiler%' OR c.description LIKE '%mirror%' THEN 'Exterior Fittings & Lamps'
    WHEN c.description LIKE '%seat%' OR c.description LIKE '%Seat%' THEN 'Seat Condition & Adjustment'
    WHEN c.description LIKE '%windshield%' OR c.description LIKE '%wiper%' OR c.description LIKE '%washer%' THEN 'Windshield & Wipers'
    WHEN c.description LIKE '%tyre%' OR c.description LIKE '%Tyre%' OR c.description LIKE '%wheel%' OR c.description LIKE '%Wheel%' THEN 'Tyre & Wheel Condition'
    WHEN c.description LIKE '%sunroof%' OR c.description LIKE '%wind noise%' THEN 'Sunroof & Seals'
    WHEN c.description LIKE '%door seal%' OR c.description LIKE '%Door seal%' THEN 'Door Seals & Weather Strips'
    WHEN c.description LIKE '%service%' OR c.description LIKE '%Service%' OR c.description LIKE '%warranty%' OR c.description LIKE '%appointment%' THEN 'Service Process / Scheduling'
    WHEN c.description LIKE '%fuel efficiency%' OR c.description LIKE '%kmpl%' THEN 'Fuel Efficiency Advisory'
    WHEN c.description LIKE '%recall%' OR c.description LIKE '%ECU%' OR c.description LIKE '%software%' THEN 'Recall / Software Update Notification'
    ELSE 'Uncategorised - Review Needed'
  END AS specific_issue,
  CASE
    WHEN c.complaint_category = 'Mechanical' AND (c.description LIKE '%brake%' OR c.description LIKE '%Brake%' OR c.description LIKE '%ABS%' OR c.description LIKE '%suspension%' OR c.description LIKE '%Suspension%' OR c.description LIKE '%steering%') THEN 'Brakes & Suspension'
    WHEN c.complaint_category = 'Mechanical' THEN 'Engine & Powertrain'
    WHEN c.complaint_category = 'Electrical' THEN 'Electrical & Infotainment'
    WHEN c.complaint_category = 'Cosmetic' AND (c.description LIKE '%seat%' OR c.description LIKE '%trim%' OR c.description LIKE '%Trim%' OR c.description LIKE '%dashboard%' OR c.description LIKE '%roof lining%') THEN 'Interior & Comfort'
    WHEN c.complaint_category = 'Cosmetic' THEN 'Exterior & Body'
    ELSE NULL
  END AS mapped_checklist_item,
  CASE
    WHEN c.complaint_category = 'Service' THEN 'NOT COVERED - No checklist item exists for Service/Process complaints'
    WHEN c.complaint_category = 'Mechanical' AND c.description LIKE '%oil consumption%' THEN 'NOT COVERED - Engine & Powertrain checklist lacks oil consumption test'
    WHEN c.complaint_category = 'Mechanical' AND c.description LIKE '%gear%' THEN 'NOT COVERED - Engine & Powertrain checklist lacks gearbox shift quality test'
    WHEN c.complaint_category = 'Electrical' AND (c.description LIKE '%ADAS%' OR c.description LIKE '%collision%' OR c.description LIKE '%lane departure%') THEN 'NOT COVERED - Electrical checklist lacks ADAS calibration verification'
    WHEN c.complaint_category = 'Electrical' AND c.description LIKE '%OTA%' THEN 'NOT COVERED - Electrical checklist lacks software/OTA update verification'
    WHEN c.complaint_category = 'Cosmetic' AND c.description LIKE '%rust%' THEN 'NOT COVERED - Exterior checklist lacks rust/corrosion inspection'
    WHEN c.complaint_category = 'Cosmetic' AND c.description LIKE '%condensation%' THEN 'NOT COVERED - Exterior checklist lacks headlight moisture/seal check'
    ELSE 'COVERED by mapped checklist item - but inspection may have been superficial'
  END AS coverage_gap
FROM cvr_dev_ai_kit.cvr_tm_demo.Customer_Complaints c
LEFT JOIN cvr_dev_ai_kit.cvr_tm_demo.silver_complaint_correlation sc
  ON c.complaint_id = sc.complaint_id;

-- ============================================================
-- Task 5: gold_checklist_recommendations
-- One row per specific sub-issue type
-- Drives actionable ePDI checklist improvements
-- Source: gold_complaint_detail
-- ============================================================

CREATE OR REPLACE TABLE cvr_dev_ai_kit.cvr_tm_demo.gold_checklist_recommendations AS
WITH base AS (
  SELECT
    specific_issue,
    mapped_checklist_item,
    coverage_gap,
    COUNT(*) AS complaint_count,
    COUNT(CASE WHEN inspection_status = 'FAIL' THEN 1 END) AS inspection_failures
  FROM cvr_dev_ai_kit.cvr_tm_demo.gold_complaint_detail
  WHERE specific_issue != 'Uncategorised - Review Needed'
  GROUP BY specific_issue, mapped_checklist_item, coverage_gap
),
agg AS (
  SELECT
    specific_issue,
    FIRST(mapped_checklist_item)                                        AS current_checklist_item,
    SUM(complaint_count)                                                AS total_complaints,
    SUM(inspection_failures)                                            AS caught_by_inspection,
    ROUND(SUM(inspection_failures)*100.0/SUM(complaint_count),1)       AS pct_caught,
    MAX(CASE WHEN coverage_gap LIKE 'NOT COVERED%' THEN 1 ELSE 0 END)  AS is_gap
  FROM base
  GROUP BY specific_issue
)
SELECT
  specific_issue                                                        AS what_customers_complain_about,
  total_complaints,
  caught_by_inspection,
  pct_caught                                                            AS pct_caught_by_inspection,
  COALESCE(current_checklist_item, 'NONE')                             AS current_checklist_item,
  CASE
    WHEN is_gap = 1                                      THEN 'NOT COVERED - Add new checklist item'
    WHEN pct_caught = 0 AND total_complaints >= 2        THEN 'COVERED BUT NOT CATCHING DEFECTS - Enhance test depth'
    WHEN pct_caught > 0                                  THEN 'COVERED - But vehicles delivered with known defects - enforce pass/fail gate'
    ELSE 'MONITOR'
  END AS checklist_status,
  CASE
    WHEN specific_issue = 'ADAS / Safety Systems'           THEN 'Add: ADAS calibration test - 200m straight drive, verify zero false triggers on collision/lane alerts'
    WHEN specific_issue = 'Infotainment System'             THEN 'Add: Full reboot test - system must fully load within 30s, all menus responsive'
    WHEN specific_issue = 'Bluetooth Connectivity'          THEN 'Add: Bluetooth pairing + 5-min audio stream test before delivery sign-off'
    WHEN specific_issue = 'Climate Control / AC'            THEN 'Add: AC performance test - cabin must reach 20C within 10 min at 35C ambient'
    WHEN specific_issue = 'Parking Aids & Camera'           THEN 'Add: Reverse camera visual clarity check + all parking sensor beep test (front + rear)'
    WHEN specific_issue = 'Software / Navigation'           THEN 'Add: OTA firmware version check - confirm latest release installed, GPS lock within 60s'
    WHEN specific_issue = 'Charging & Connectivity Ports'   THEN 'Add: All USB ports and wireless pad functional test - charge indicator must show on device'
    WHEN specific_issue = 'Engine Cold Start & Idle Stability' THEN 'Add: Cold start test - engine must idle stable for 5 min, no stall, no rough idle flag'
    WHEN specific_issue = 'Engine Oil Consumption & Leaks'  THEN 'Add: Oil level marker check + underside visual inspection for leaks before delivery'
    WHEN specific_issue = 'Engine NVH (Noise / Vibration)'  THEN 'Enhance Engine & Powertrain: Add 60 kmph road test - NVH must be within acceptable band'
    WHEN specific_issue = 'Brake System & ABS'              THEN 'Enhance Brakes: Add pedal feel test (firm, not spongy) + ABS warning light must be off at ignition'
    WHEN specific_issue = 'Suspension & Steering'           THEN 'Enhance Brakes & Suspension: Add kerb bump test at 20 kmph - zero clunk tolerance'
    WHEN specific_issue = 'Transmission & Drivetrain'       THEN 'Add: Gear shift quality test - all gears 1 to 6 in moving vehicle, smooth transition required'
    WHEN specific_issue = 'Paint & Surface Finish'          THEN 'Enhance Exterior & Body: Add rust spot scan under lamp + colour panel uniformity check in daylight'
    WHEN specific_issue = 'Body Damage (Dents / Scratches)' THEN 'Enhance Exterior & Body: Mandatory 4-panel + roof photographic record before customer handover'
    WHEN specific_issue = 'Interior Trim & Fittings'        THEN 'Enhance Interior & Comfort: Add trim pull test (no loose panels) + rattle drive on rough surface'
    WHEN specific_issue = 'Tyre & Wheel Condition'          THEN 'Add: All 4 TPMS sensor readings must match gauge pressure, kerb rash check on all rims'
    WHEN specific_issue = 'Windshield & Wipers'             THEN 'Add: Wiper blade streak test (both speeds) + washer fluid spray direction check'
    WHEN specific_issue = 'Sunroof & Seals'                 THEN 'Add: Sunroof water seal test + highway wind noise check at 80 kmph'
    WHEN specific_issue = 'Service Process / Scheduling'    THEN 'Process Gap: Mandatory service handover briefing - show customer portal, explain intervals, warranty terms, sign-off sheet'
    WHEN specific_issue = 'Fuel Efficiency Advisory'        THEN 'Process Gap: Add real-world vs ARAI fuel efficiency expectation card to delivery kit'
    WHEN specific_issue = 'Recall / Software Update Notification' THEN 'Process Gap: VIN recall check mandatory before delivery; customer signs recall status acknowledgement'
    WHEN specific_issue = 'Exterior Fittings & Lamps'       THEN 'Add: Headlight condensation check + all exterior clips/fittings torque verified'
    WHEN specific_issue = 'Door Seals & Weather Strips'     THEN 'Add: Door seal compression check all 4 doors - no daylight gap, no wind noise at 80 kmph'
    WHEN specific_issue = 'Seat Condition & Adjustment'     THEN 'Enhance Interior & Comfort: All seat adjustment motors functional + fabric condition check'
    WHEN specific_issue = 'Fuel System'                     THEN 'Add: Fuel filler cap seal check + no fuel smell inside cabin test'
    ELSE 'Define specific test procedure based on complaint descriptions'
  END AS specific_action_required,
  CASE
    WHEN is_gap = 1 THEN 1
    WHEN pct_caught = 0 AND total_complaints >= 2 THEN 2
    WHEN pct_caught > 0 THEN 3
    ELSE 4
  END AS priority
FROM agg
ORDER BY priority, total_complaints DESC;

-- ============================================================
-- Verification
-- ============================================================
SELECT
  (SELECT COUNT(*) FROM cvr_dev_ai_kit.cvr_tm_demo.gold_complaint_dashboard)          AS dashboard_rows,
  (SELECT COUNT(*) FROM cvr_dev_ai_kit.cvr_tm_demo.gold_epdi_gap_analysis)            AS gap_rows,
  (SELECT COUNT(*) FROM cvr_dev_ai_kit.cvr_tm_demo.gold_delivery_service_link)        AS delivery_rows,
  (SELECT COUNT(*) FROM cvr_dev_ai_kit.cvr_tm_demo.gold_complaint_detail)             AS detail_rows,
  (SELECT COUNT(*) FROM cvr_dev_ai_kit.cvr_tm_demo.gold_checklist_recommendations)    AS reco_rows;
