-- Meridian Uptime — Lakeflow Spark Declarative Pipeline (bronze → silver → gold)
-- Config keys (set in databricks.yml): catalog, schema
-- Auto Loader (read_files) incrementally picks up each new daily historian drop.

------------------------------------------------------------------------------
-- BRONZE: raw, append-only, with lineage columns
------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_telemetry
COMMENT 'Raw hourly sensor rollups from the plant historian (JSON lines, one file/day)'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('/Volumes/${catalog}/${schema}/raw/telemetry', format => 'json',
  schemaHints => 'machine_id STRING, ts STRING, spindle_vibration_mm_s DOUBLE, spindle_temp_c DOUBLE, spindle_load_pct DOUBLE, coolant_pressure_bar DOUBLE, power_kw DOUBLE, parts_produced BIGINT, status STRING');

CREATE OR REFRESH STREAMING TABLE bronze_work_orders
COMMENT 'Raw CMMS work-order export'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('/Volumes/${catalog}/${schema}/raw/work_orders', format => 'csv', header => true);

CREATE OR REFRESH STREAMING TABLE bronze_production_orders
COMMENT 'Raw MES production-order export (next 7 days)'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('/Volumes/${catalog}/${schema}/raw/production_orders', format => 'csv', header => true);

CREATE OR REFRESH STREAMING TABLE bronze_machines
COMMENT 'Raw asset registry export'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('/Volumes/${catalog}/${schema}/raw/machines', format => 'csv', header => true);

CREATE OR REFRESH STREAMING TABLE bronze_technicians
COMMENT 'Raw shift roster from HR (contains PII)'
TBLPROPERTIES ('quality' = 'bronze', 'contains_pii' = 'true')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('/Volumes/${catalog}/${schema}/raw/technicians', format => 'csv', header => true);

CREATE OR REFRESH STREAMING TABLE bronze_manuals
COMMENT 'Raw maintenance-manual sections (grounding for GenAI repair plans)'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('/Volumes/${catalog}/${schema}/raw/manuals', format => 'json');

------------------------------------------------------------------------------
-- SILVER: typed, validated, governed
------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_telemetry (
  CONSTRAINT valid_machine EXPECT (machine_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_ts EXPECT (ts IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT plausible_temp EXPECT (spindle_temp_c BETWEEN -10 AND 200) ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_vibration EXPECT (spindle_vibration_mm_s >= 0)
)
COMMENT 'Validated hourly machine telemetry. Drops null ids and physically impossible temperatures.'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  machine_id,
  split(machine_id, '-')[1] AS plant_id,
  to_timestamp(ts) AS ts,
  spindle_vibration_mm_s, spindle_temp_c, spindle_load_pct, coolant_pressure_bar, power_kw,
  CAST(parts_produced AS INT) AS parts_produced,
  status
FROM STREAM(bronze_telemetry);

CREATE OR REFRESH MATERIALIZED VIEW silver_machines
COMMENT 'Asset registry: one row per CNC machine'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT machine_id, plant_id, plant_name, region, line, machine_type, model,
          CAST(install_year AS INT) AS install_year, criticality
FROM bronze_machines;

CREATE OR REFRESH MATERIALIZED VIEW silver_technicians (
  technician_id STRING,
  full_name STRING,
  plant_id STRING,
  shift STRING,
  skills ARRAY<STRING>,
  phone STRING MASK ${catalog}.${schema}.mask_pii,
  email STRING MASK ${catalog}.${schema}.mask_pii
)
WITH ROW FILTER ${catalog}.${schema}.plant_filter ON (plant_id)
COMMENT 'Maintenance technicians. Phone/e-mail masked (PII); rows filtered by plant.'
TBLPROPERTIES ('quality' = 'silver', 'contains_pii' = 'true')
AS SELECT technician_id, full_name, plant_id, shift, split(skills, '\\|') AS skills, phone, email
FROM bronze_technicians;

CREATE OR REFRESH MATERIALIZED VIEW silver_work_orders (
  CONSTRAINT valid_wo EXPECT (work_order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'CMMS work orders (corrective + preventive) with typed costs and downtime'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT work_order_id, machine_id, split(machine_id, '-')[1] AS plant_id, wo_type,
          NULLIF(failure_mode, '') AS failure_mode,
          to_timestamp(opened_ts) AS opened_ts, to_timestamp(closed_ts) AS closed_ts,
          CAST(downtime_hours AS DOUBLE) AS downtime_hours, CAST(labor_hours AS DOUBLE) AS labor_hours,
          CAST(parts_cost_usd AS DOUBLE) AS parts_cost_usd, technician_id, notes
FROM bronze_work_orders;

CREATE OR REFRESH MATERIALIZED VIEW silver_production_orders
COMMENT 'MES production orders for the next 7 days, with OEM line-down penalty terms'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT production_order_id, machine_id, split(machine_id, '-')[1] AS plant_id, oem_customer,
          part_number, part_description, CAST(quantity AS INT) AS quantity,
          to_timestamp(start_ts) AS start_ts, to_timestamp(due_ts) AS due_ts,
          CAST(oem_line_down_penalty_usd_per_hour AS DOUBLE) AS oem_line_down_penalty_usd_per_hour,
          CAST(finished_goods_buffer_hours AS DOUBLE) AS finished_goods_buffer_hours
FROM bronze_production_orders;

CREATE OR REFRESH MATERIALIZED VIEW silver_manuals
COMMENT 'Maintenance-manual sections keyed by machine type + failure mode'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT doc_id, machine_type, model, failure_mode, title, content FROM bronze_manuals;

------------------------------------------------------------------------------
-- GOLD: business-level KPIs
------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_machine_daily_kpis
COMMENT 'Per machine per day: availability, unplanned downtime, output, sensor health'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT
  t.machine_id, m.plant_id, m.plant_name, m.line, m.machine_type, m.criticality,
  date(t.ts) AS day,
  count(*) AS hours_observed,
  sum(CASE WHEN t.status = 'RUNNING' THEN 1 ELSE 0 END) AS running_hours,
  sum(CASE WHEN t.status = 'DOWN' THEN 1 ELSE 0 END) AS unplanned_down_hours,
  round(sum(CASE WHEN t.status = 'RUNNING' THEN 1 ELSE 0 END) / count(*), 4) AS availability,
  sum(t.parts_produced) AS parts_produced,
  round(avg(CASE WHEN t.status = 'RUNNING' THEN t.spindle_vibration_mm_s END), 3) AS avg_vibration_mm_s,
  round(max(t.spindle_vibration_mm_s), 3) AS max_vibration_mm_s,
  round(avg(CASE WHEN t.status = 'RUNNING' THEN t.spindle_temp_c END), 2) AS avg_spindle_temp_c,
  round(avg(CASE WHEN t.status = 'RUNNING' THEN t.coolant_pressure_bar END), 2) AS avg_coolant_pressure_bar
FROM silver_telemetry t JOIN silver_machines m USING (machine_id)
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW gold_failure_events
COMMENT 'Historical unplanned failures with cost of downtime (parts + labor + lost contribution)'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT w.work_order_id, w.machine_id, w.plant_id, m.machine_type, m.criticality, w.failure_mode,
          w.opened_ts AS failure_ts, w.downtime_hours, w.parts_cost_usd,
          round(w.labor_hours * 85, 2) AS labor_cost_usd,
          round(w.downtime_hours * 1450, 2) AS lost_contribution_usd,  -- $1,450/h contribution margin per machine
          round(w.parts_cost_usd + w.labor_hours * 85 + w.downtime_hours * 1450, 2) AS total_cost_usd
FROM silver_work_orders w JOIN silver_machines m USING (machine_id)
WHERE w.wo_type = 'CORRECTIVE';
