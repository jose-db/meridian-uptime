#!/usr/bin/env python3
"""Create (or update) the 'Meridian Reliability' Genie Agent over the governed gold/silver tables.

usage: python3 genie/create_genie_space.py [--profile DEFAULT] [--space-id <id-to-update>]
Writes the serialized space to genie/meridian_reliability_space.json (version-controlled).
"""
import json, subprocess, sys, hashlib
from pathlib import Path

PROFILE = sys.argv[sys.argv.index("--profile") + 1] if "--profile" in sys.argv else "DEFAULT"
SPACE_ID = sys.argv[sys.argv.index("--space-id") + 1] if "--space-id" in sys.argv else None
C, S = "main", "meridian"
WAREHOUSE = "<warehouse-id>"
T = lambda t: f"{C}.{S}.{t}"


def hid(s: str) -> str:  # deterministic 32-hex ids so re-runs produce stable diffs
    return hashlib.md5(s.encode()).hexdigest()


tables = [
    {"identifier": T("gold_failure_events"), "description": ["Historical unplanned machine failures with downtime and full cost (parts + labor + lost contribution)."],
     "column_configs": [{"column_name": "failure_mode", "enable_format_assistance": True, "enable_entity_matching": True},
                        {"column_name": "plant_id", "enable_format_assistance": True, "enable_entity_matching": True}]},
    {"identifier": T("gold_machine_daily_kpis"), "description": ["Per machine per day availability, unplanned down hours, parts produced and sensor health."],
     "column_configs": [{"column_name": "plant_name", "enable_format_assistance": True, "enable_entity_matching": True}]},
    {"identifier": T("gold_machine_risk"), "description": ["Current 48h failure risk per machine (ML model), likely failure mode, next OEM order and $ exposure. One row per machine."],
     "column_configs": [{"column_name": "oem_customer", "enable_format_assistance": True, "enable_entity_matching": True},
                        {"column_name": "risk_band", "enable_format_assistance": True, "enable_entity_matching": True}]},
    {"identifier": T("gold_model_backtest"), "description": ["Backtest of the failure model on the hold-out window: failures caught, downtime avoided, net $ value."]},
    {"identifier": T("silver_machines"), "description": ["Asset registry: machine, plant, line, type, model, criticality."]},
    {"identifier": T("silver_production_orders"), "description": ["MES production orders for the next 7 days with OEM line-down penalties and finished-goods buffer."]},
    {"identifier": T("silver_work_orders"), "description": ["CMMS work orders, corrective and preventive, with technician notes."]},
]
for t in tables:
    if "column_configs" in t:
        t["column_configs"].sort(key=lambda c: c["column_name"])
tables.sort(key=lambda t: t["identifier"])

instructions = [
    "Company: Meridian Precision, automotive Tier-1 supplier. Plants: SAL=Saltillo, MTY=Monterrey, QRO=Querétaro. 60 CNC machines.",
    "Unplanned downtime = status DOWN hours in gold_machine_daily_kpis.unplanned_down_hours, or downtime_hours in gold_failure_events. Preventive work orders are planned and are NOT unplanned downtime.",
    "Cost of downtime: use gold_failure_events.total_cost_usd (parts + labor at $85/h + lost contribution at $1,450 per machine-hour).",
    "Risk questions ('which machines are at risk', 'what should we fix first') use gold_machine_risk ordered by priority_rank; risk_band values: 'ACT NOW' (>=60%), 'WATCH' (30-60%), 'LOW'. exposure_usd is the probability-weighted cost if the machine fails within 48 hours, including OEM line-down penalties beyond the finished-goods buffer.",
    "Time windows: the data's 'today' is the max(day) in gold_machine_daily_kpis; compute 'last N days' relative to that, not current_date().",
    "Failure modes: SPINDLE_BEARING, COOLANT_PUMP, SERVO_AXIS, TOOL_CHANGER. Present them in title case without underscores.",
    "'At risk', 'exposed' or 'line-down risk' means risk_band IN ('ACT NOW', 'WATCH') in gold_machine_risk; LOW machines are not at risk. Exposure questions should filter to those bands unless the user asks for all machines.",
    "Always round money to whole dollars and percentages to one decimal.",
]

examples = [
    ("Which plant lost the most hours to unplanned downtime in the last 90 days?",
     f"SELECT plant_name, SUM(unplanned_down_hours) AS unplanned_down_hours FROM {T('gold_machine_daily_kpis')} WHERE day > (SELECT date_sub(max(day), 90) FROM {T('gold_machine_daily_kpis')}) GROUP BY plant_name ORDER BY unplanned_down_hours DESC"),
    ("What is the total cost of failures by failure mode?",
     f"SELECT failure_mode, COUNT(*) AS failures, ROUND(SUM(downtime_hours)) AS downtime_hours, ROUND(SUM(total_cost_usd)) AS total_cost_usd FROM {T('gold_failure_events')} GROUP BY failure_mode ORDER BY total_cost_usd DESC"),
    ("Which machines should we fix first?",
     f"SELECT priority_rank, machine_id, plant_name, ROUND(failure_prob_48h*100,1) AS failure_prob_pct, risk_band, likely_failure_mode, oem_customer, finished_goods_buffer_hours, ROUND(exposure_usd) AS exposure_usd FROM {T('gold_machine_risk')} WHERE risk_band <> 'LOW' ORDER BY priority_rank"),
]

samples = ["Which machines should we fix first?",
           "Which plant lost the most hours to unplanned downtime in the last 90 days?",
           "What is the total cost of spindle bearing failures by plant?",
           "Which OEM customers are exposed to a line-down risk in the next 48 hours?",
           "How much net value did the failure model deliver in the backtest?"]

benchmarks = [
    ("How many unplanned failures happened in total?", f"SELECT COUNT(*) FROM {T('gold_failure_events')}"),
    ("Total exposure in dollars for machines in ACT NOW", f"SELECT ROUND(SUM(exposure_usd)) FROM {T('gold_machine_risk')} WHERE risk_band = 'ACT NOW'"),
    ("Average availability by plant", f"SELECT plant_name, ROUND(AVG(availability)*100,1) AS availability_pct FROM {T('gold_machine_daily_kpis')} GROUP BY plant_name ORDER BY plant_name"),
    ("Which failure mode has the longest average downtime?", f"SELECT failure_mode, ROUND(AVG(downtime_hours),1) AS avg_downtime_h FROM {T('gold_failure_events')} GROUP BY failure_mode ORDER BY avg_downtime_h DESC LIMIT 1"),
    ("Which machine has the highest failure probability right now?", f"SELECT machine_id, failure_prob_48h FROM {T('gold_machine_risk')} ORDER BY failure_prob_48h DESC LIMIT 1"),
]

space = {
    "version": 2,
    "config": {"sample_questions": sorted([{"id": hid("sq" + q), "question": [q]} for q in samples], key=lambda x: x["id"])},
    "data_sources": {"tables": tables},
    "instructions": {
        "text_instructions": [{"id": hid("ti"), "content": instructions}],
        "example_question_sqls": sorted([{"id": hid("ex" + q), "question": [q], "sql": [s]} for q, s in examples], key=lambda x: x["id"]),
        "join_specs": sorted([
            {"id": hid("j1"), "left": {"identifier": T("gold_failure_events"), "alias": "gold_failure_events"},
             "right": {"identifier": T("silver_machines"), "alias": "silver_machines"},
             "sql": ["`gold_failure_events`.`machine_id` = `silver_machines`.`machine_id`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]},
            {"id": hid("j2"), "left": {"identifier": T("silver_production_orders"), "alias": "silver_production_orders"},
             "right": {"identifier": T("silver_machines"), "alias": "silver_machines"},
             "sql": ["`silver_production_orders`.`machine_id` = `silver_machines`.`machine_id`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]},
        ], key=lambda x: x["id"]),
    },
    "benchmarks": {"questions": sorted([{"id": hid("bm" + q), "question": [q], "answer": [{"format": "SQL", "content": [s]}]} for q, s in benchmarks], key=lambda x: x["id"])},
}
out = Path(__file__).with_name("meridian_reliability_space.json")
out.write_text(json.dumps(space, indent=2))
ser = json.dumps(space)
title = "Meridian Reliability — Downtime & Risk"
desc = "Ask about unplanned downtime, failure costs, current machine risk and OEM exposure across Meridian Precision's 3 plants."

if SPACE_ID:
    cmd = ["databricks", "genie", "update-space", SPACE_ID, "--json", json.dumps({"serialized_space": ser, "title": title, "description": desc, "warehouse_id": WAREHOUSE})]
else:
    cmd = ["databricks", "genie", "create-space", WAREHOUSE, ser, "--title", title, "--description", desc]
r = subprocess.run(cmd + ["--profile", PROFILE, "-o", "json"], capture_output=True, text=True)
print(r.stdout or r.stderr)
