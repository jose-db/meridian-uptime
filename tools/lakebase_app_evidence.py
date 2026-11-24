#!/usr/bin/env python3
"""Lakebase serving + app write-back evidence -> evidence/lakebase_and_app.md

Lakebase database `meridian` is registered in Unity Catalog as catalog `meridian_lakebase`, so the
operational tables (including work orders written by the app) are queryable from the lakehouse.
"""
import json, subprocess, sys, time
from pathlib import Path

PROFILE = sys.argv[sys.argv.index("--profile") + 1] if "--profile" in sys.argv else "DEFAULT"


def run(args):
    r = subprocess.run(args + ["--profile", PROFILE, "-o", "json"], capture_output=True, text=True, cwd="/tmp")
    return json.loads(r.stdout) if r.returncode == 0 and r.stdout.strip() else [{"error": r.stderr.strip()[:400]}]


def sql(q):
    return run(["databricks", "experimental", "aitools", "tools", "query", q])


def table(rows):
    if not rows:
        return "_(no rows)_"
    cols = list(rows[0].keys())
    return "\n".join(["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)] +
                     ["| " + " | ".join(str(r.get(c, "")).replace("|", "\\|").replace("\n", " ")[:140] for c in cols) + " |" for r in rows])


inst = run(["databricks", "database", "get-database-instance", "meridian-db"])
app = run(["databricks", "apps", "get", "meridian-uptime-planner"])
out = ["# Lakebase + Databricks App evidence", "", f"Captured {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}.", "",
       "## Lakebase instance", "",
       f"- name `{inst.get('name')}` · state **{inst.get('state')}** · capacity {inst.get('capacity')} · pg {inst.get('pg_version')}",
       "- registered in Unity Catalog as catalog `meridian_lakebase` (database `meridian`)", "",
       "## Databricks App", "",
       f"- `{app.get('name')}` · app **{(app.get('app_status') or {}).get('state')}** · compute **{(app.get('compute_status') or {}).get('state')}**",
       f"- url: {app.get('url')}",
       f"- service principal: `{app.get('service_principal_client_id')}`",
       f"- resources: {json.dumps(app.get('resources'))}", ""]
for title, q in [
    ("Row counts per serving table", """SELECT 'machine_risk' t, count(*) n FROM meridian_lakebase.public.machine_risk UNION ALL
        SELECT 'repair_plans', count(*) FROM meridian_lakebase.public.repair_plans UNION ALL
        SELECT 'telemetry_recent', count(*) FROM meridian_lakebase.public.telemetry_recent UNION ALL
        SELECT 'technicians', count(*) FROM meridian_lakebase.public.technicians UNION ALL
        SELECT 'work_orders', count(*) FROM meridian_lakebase.public.work_orders"""),
    ("What the app's action list reads (top 5)", """SELECT priority_rank, machine_id, plant_name, round(failure_prob_48h, 2) AS p_fail_48h, risk_band,
        likely_failure_mode, oem_customer, finished_goods_buffer_hours, round(exposure_usd) AS exposure_usd
        FROM meridian_lakebase.public.machine_risk ORDER BY priority_rank LIMIT 5"""),
    ("Technician roster published to Lakebase (no PII columns)", "SELECT * FROM meridian_lakebase.public.technicians ORDER BY technician_id LIMIT 4"),
    ("Work orders written back BY THE APP (planner decisions)", """SELECT work_order_id, machine_id, failure_mode, technician_id, scheduled_start, est_hours,
        round(exposure_usd) AS exposure_usd, status, created_by, created_at, planner_note FROM meridian_lakebase.public.work_orders ORDER BY created_at"""),
    ("Lakehouse ↔ Lakebase join: exposure covered by a planned work order", """SELECT r.machine_id, r.risk_band, round(r.exposure_usd) AS exposure_usd, w.work_order_id, w.status
        FROM main.meridian.gold_machine_risk r
        LEFT JOIN meridian_lakebase.public.work_orders w ON w.machine_id = r.machine_id
        WHERE r.risk_band <> 'LOW' ORDER BY r.priority_rank"""),
]:
    out += [f"## {title}", "", "```sql", q.strip(), "```", "", table(sql(q)), ""]
Path(__file__).resolve().parent.parent.joinpath("evidence", "lakebase_and_app.md").write_text("\n".join(out) + "\n")
print("wrote evidence/lakebase_and_app.md")
