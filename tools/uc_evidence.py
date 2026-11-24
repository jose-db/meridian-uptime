#!/usr/bin/env python3
"""Unity Catalog governance + lineage + model registry evidence -> evidence/uc_governance.md"""
import json, subprocess, sys, time
from pathlib import Path

PROFILE = sys.argv[sys.argv.index("--profile") + 1] if "--profile" in sys.argv else "DEFAULT"
C, S = "main", "meridian"
APP_SP = "<app-sp-client-id>"


def sql(q):
    r = subprocess.run(["databricks", "experimental", "aitools", "tools", "query", q, "--profile", PROFILE, "-o", "json"],
                       capture_output=True, text=True, cwd="/tmp")
    return json.loads(r.stdout) if r.returncode == 0 else [{"error": r.stderr.strip()[:400]}]


def api(path):
    r = subprocess.run(["databricks", "api", "get", path, "--profile", PROFILE], capture_output=True, text=True)
    return json.loads(r.stdout or "{}")


def table(rows):
    if not rows:
        return "_(no rows)_"
    cols = list(rows[0].keys())
    return "\n".join(["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)] +
                     ["| " + " | ".join(str(r.get(c, "")).replace("|", "\\|").replace("\n", " ")[:160] for c in cols) + " |" for r in rows])


sections = [
    ("Tables in the governed schema (with comments)",
     f"SELECT table_name, table_type, comment FROM {C}.information_schema.tables WHERE table_schema = '{S}' ORDER BY table_name"),
    ("Column masks attached (PII)",
     f"SELECT table_name, column_name, mask_name FROM {C}.information_schema.column_masks WHERE table_schema = '{S}'"),
    ("Row filters attached (plant scoping)",
     f"SELECT table_name, filter_name, target_columns FROM {C}.information_schema.row_filters WHERE table_schema = '{S}'"),
    ("Governance functions",
     f"SELECT routine_name, comment FROM {C}.information_schema.routines WHERE routine_schema = '{S}' ORDER BY 1"),
    ("Mask definition (non-admin, non-HR callers get the ELSE branch)",
     f"SELECT routine_name, routine_definition FROM {C}.information_schema.routines WHERE routine_schema = '{S}' AND routine_name = 'mask_pii'"),
    ("Mask in action — this capture runs as a workspace admin, so `phone`/`email` come back clear; the last two columns apply the mask's non-HR branch to the same rows",
     f"SELECT technician_id, plant_id, phone, email, concat('***', right(phone, 4)) AS phone_as_seen_by_non_hr, concat('***', right(email, 4)) AS email_as_seen_by_non_hr FROM {C}.{S}.silver_technicians ORDER BY technician_id LIMIT 4"),
    ("Grants on the schema (app service principal = read-only)",
     f"SHOW GRANTS ON SCHEMA {C}.{S}"),
    ("Lineage captured by Unity Catalog (system.access.table_lineage)",
     f"""SELECT DISTINCT source_table_name, target_table_name, entity_type FROM system.access.table_lineage
         WHERE target_table_schema = '{S}' AND target_table_catalog = '{C}' AND source_table_name IS NOT NULL
           AND event_date >= current_date() - 2 ORDER BY target_table_name, source_table_name"""),
]
out = ["# Unity Catalog governance evidence", "", f"Captured {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())} via SQL on warehouse.", ""]
for title, q in sections:
    out += [f"## {title}", "", "```sql", q.strip(), "```", "", table(sql(q)), ""]

name = f"{C}.{S}.machine_failure_48h"
m = api(f"/api/2.1/unity-catalog/models/{name}?include_aliases=true")
vers = api(f"/api/2.1/unity-catalog/models/{name}/versions").get("model_versions", [])
out += ["## Registered model in Unity Catalog", "", f"- name: `{name}`", f"- description: {m.get('comment')}",
        f"- aliases: {[(a['alias_name'], a['version_num']) for a in m.get('aliases', [])]}", "",
        table([{"version": v["version"], "status": v.get("status"), "run_id": v.get("run_id"),
                "created": time.strftime('%Y-%m-%d %H:%M', time.gmtime(v["created_at"] / 1000))} for v in sorted(vers, key=lambda v: v["version"])]), ""]
Path(__file__).resolve().parent.parent.joinpath("evidence", "uc_governance.md").write_text("\n".join(out) + "\n")
print("wrote evidence/uc_governance.md")
