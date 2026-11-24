# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Uptime — 05 · Publish to Lakebase (operational serving + write-back)
# MAGIC
# MAGIC The planner app needs **millisecond** reads and **transactional writes** (create / assign /
# MAGIC close a work order) — an OLTP workload, not a warehouse query. Lakebase (managed Postgres)
# MAGIC serves it:
# MAGIC
# MAGIC | Lakebase table | Source | Access pattern |
# MAGIC |---|---|---|
# MAGIC | `machine_risk` | `gold_machine_risk` | read — ranked action list |
# MAGIC | `repair_plans` | `gold_repair_plans` | read — GenAI plan per machine |
# MAGIC | `telemetry_recent` | `silver_telemetry` (last 7 days) | read — sparkline charts |
# MAGIC | `technicians` | `silver_technicians` (**no PII columns**) | read — crew assignment |
# MAGIC | `work_orders` | app | **write-back** — planner decisions, status, audit columns |
# MAGIC
# MAGIC Note: this workspace's metastore has no managed storage root, so UC *synced tables* can't
# MAGIC initialise; we publish with an idempotent psycopg load instead (same tables, same shape).
# MAGIC In a standard workspace these would be synced tables in snapshot/triggered mode.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.1"
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "meridian")
dbutils.widgets.text("lakebase_instance", "meridian-db")
dbutils.widgets.text("pg_database", "meridian")
C, S = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
INSTANCE, PGDB = dbutils.widgets.get("lakebase_instance"), dbutils.widgets.get("pg_database")

import uuid, requests, psycopg
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
hdr = w.config.authenticate()
host = w.config.host.rstrip("/")
inst = requests.get(f"{host}/api/2.0/database/instances/{INSTANCE}", headers=hdr).json()
PGHOST = inst["read_write_dns"]
# REST credential call (serverless runtime's SDK may predate w.database.*)
token = requests.post(f"{host}/api/2.0/database/credentials", headers=hdr,
                      json={"instance_names": [INSTANCE], "request_id": str(uuid.uuid4())}).json()["token"]
USER = w.current_user.me().user_name
print("Lakebase:", INSTANCE, PGHOST, "state:", inst.get("state"))

def connect(db):
    return psycopg.connect(host=PGHOST, port=5432, dbname=db, user=USER, password=token, sslmode="require", autocommit=True)

with connect("databricks_postgres") as cx:
    if not cx.execute("SELECT 1 FROM pg_database WHERE datname=%s", (PGDB,)).fetchone():
        cx.execute(f'CREATE DATABASE "{PGDB}"')
        print("created database", PGDB)

# COMMAND ----------

DDL = """
CREATE TABLE IF NOT EXISTS machine_risk (
  machine_id text PRIMARY KEY, priority_rank int, as_of_ts timestamp, plant_id text, plant_name text, line text,
  machine_type text, model text, criticality text, failure_prob_48h double precision, risk_band text,
  likely_failure_mode text, reason text, oem_customer text, part_number text, part_description text,
  finished_goods_buffer_hours double precision, oem_line_down_penalty_usd_per_hour double precision,
  exp_downtime_h double precision, cost_if_fails_usd double precision, exposure_usd double precision,
  recommended_action text, vib_6h double precision, temp_6h double precision, press_6h double precision, load_6h double precision);
CREATE TABLE IF NOT EXISTS repair_plans (machine_id text PRIMARY KEY, likely_failure_mode text, repair_plan_md text, llm_endpoint text, generated_at timestamp);
CREATE TABLE IF NOT EXISTS telemetry_recent (machine_id text, ts timestamp, spindle_vibration_mm_s double precision,
  spindle_temp_c double precision, spindle_load_pct double precision, coolant_pressure_bar double precision, status text,
  PRIMARY KEY (machine_id, ts));
CREATE TABLE IF NOT EXISTS technicians (technician_id text PRIMARY KEY, full_name text, plant_id text, shift text, skills text);
CREATE TABLE IF NOT EXISTS work_orders (
  work_order_id text PRIMARY KEY DEFAULT ('WO-P-' || to_char(now(), 'YYMMDDHH24MISS') || '-' || substr(md5(random()::text), 1, 4)),
  machine_id text NOT NULL, failure_mode text, technician_id text, scheduled_start timestamp, est_hours double precision,
  exposure_usd double precision, status text NOT NULL DEFAULT 'SCHEDULED' CHECK (status IN ('SCHEDULED','IN_PROGRESS','DONE','CANCELLED')),
  planner_note text, created_by text, created_at timestamptz DEFAULT now(), updated_at timestamptz DEFAULT now());
CREATE INDEX IF NOT EXISTS ix_wo_machine ON work_orders(machine_id);
"""

risk = spark.sql(f"""SELECT machine_id, priority_rank, as_of_ts, plant_id, plant_name, line, machine_type, model, criticality,
  failure_prob_48h, risk_band, likely_failure_mode, reason, oem_customer, part_number, part_description,
  finished_goods_buffer_hours, oem_line_down_penalty_usd_per_hour, exp_downtime_h, cost_if_fails_usd, exposure_usd,
  recommended_action, spindle_vibration_mm_s_mean_6h, spindle_temp_c_mean_6h, coolant_pressure_bar_mean_6h, spindle_load_pct_mean_6h
  FROM {C}.{S}.gold_machine_risk""").collect()
plans = spark.sql(f"SELECT machine_id, likely_failure_mode, repair_plan_md, llm_endpoint, generated_at FROM {C}.{S}.gold_repair_plans").collect()
tele = spark.sql(f"""SELECT machine_id, ts, spindle_vibration_mm_s, spindle_temp_c, spindle_load_pct, coolant_pressure_bar, status
  FROM {C}.{S}.silver_telemetry WHERE ts >= (SELECT max(ts) FROM {C}.{S}.silver_telemetry) - INTERVAL 7 DAYS""").collect()
techs = spark.sql(f"SELECT technician_id, full_name, plant_id, shift, array_join(skills, '|') FROM {C}.{S}.silver_technicians").collect()

def ph(n): return ",".join(["%s"] * n)
with connect(PGDB) as cx:
    cx.execute(DDL)
    with cx.transaction():
        for t in ["machine_risk", "repair_plans", "telemetry_recent", "technicians"]:
            cx.execute(f"TRUNCATE {t}")
        with cx.cursor() as cur:
            cur.executemany(f"INSERT INTO machine_risk VALUES ({ph(26)})", [tuple(r) for r in risk])
            cur.executemany(f"INSERT INTO repair_plans VALUES ({ph(5)})", [tuple(r) for r in plans])
            cur.executemany(f"INSERT INTO telemetry_recent VALUES ({ph(7)})", [tuple(r) for r in tele])
            cur.executemany(f"INSERT INTO technicians VALUES ({ph(5)})", [tuple(r) for r in techs])
    counts = {t: cx.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in ["machine_risk", "repair_plans", "telemetry_recent", "technicians", "work_orders"]}
    top = cx.execute("SELECT priority_rank, machine_id, plant_name, round(failure_prob_48h::numeric,2), risk_band, oem_customer, round(exposure_usd) FROM machine_risk ORDER BY priority_rank LIMIT 5").fetchall()
print("Lakebase row counts:", counts)
print("Top 5 from Lakebase (what the app reads):")
for r in top: print("  ", r)

# COMMAND ----------

# MAGIC %md ## Least-privilege grants for the app's service principal
# MAGIC The app can read the serving tables and write **only** `work_orders`. It never sees technician PII
# MAGIC (the roster published here has no phone / e-mail columns).

# COMMAND ----------

dbutils.widgets.text("app_sp_client_id", "")
APP_SP = dbutils.widgets.get("app_sp_client_id").strip()
if APP_SP:
    with connect(PGDB) as cx:
        if not cx.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (APP_SP,)).fetchone():
            cx.execute("SELECT databricks_create_role(%s, 'SERVICE_PRINCIPAL')", (APP_SP,))
        for stmt in [f'GRANT CONNECT ON DATABASE "{PGDB}" TO "{APP_SP}"',
                     f'GRANT USAGE ON SCHEMA public TO "{APP_SP}"',
                     f'GRANT SELECT ON machine_risk, repair_plans, telemetry_recent, technicians TO "{APP_SP}"',
                     f'GRANT SELECT, INSERT, UPDATE ON work_orders TO "{APP_SP}"']:
            cx.execute(stmt)
        grants = cx.execute("""SELECT table_name, string_agg(privilege_type, ',' ORDER BY privilege_type)
                               FROM information_schema.role_table_grants WHERE grantee=%s GROUP BY 1 ORDER BY 1""", (APP_SP,)).fetchall()
    print("App SP grants:", grants)
else:
    print("No app SP supplied — skipping grants")
