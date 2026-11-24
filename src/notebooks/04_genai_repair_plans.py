# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Uptime — 04 · GenAI repair plans (grounded, traced, evaluated)
# MAGIC
# MAGIC For every machine in **ACT NOW / WATCH**, an LLM drafts the work-order plan a maintenance
# MAGIC planner would otherwise assemble by hand from three systems: the sensor evidence (model),
# MAGIC the OEM manual section for the likely failure mode, and this machine's own repair history (CMMS).
# MAGIC
# MAGIC - Grounding: manual + history are retrieved by key (machine_type × failure_mode) from governed UC tables
# MAGIC - Every call is captured with **MLflow Tracing**
# MAGIC - Quality gate: **MLflow GenAI evaluation** — LLM judges (safety/LOTO, grounding, actionability)
# MAGIC   plus a deterministic check that the plan cites the correct manual part number.

# COMMAND ----------

# MAGIC %pip install -q "mlflow[databricks]>=3.1" openai
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "meridian")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-5-5")
C, S, LLM = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"), dbutils.widgets.get("llm_endpoint")

import json, re
import pandas as pd
import mlflow
from databricks.sdk import WorkspaceClient

me = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{me}/meridian-uptime-repair-plans")
mlflow.openai.autolog()
client = WorkspaceClient().serving_endpoints.get_open_ai_client()

risk = spark.table(f"{C}.{S}.gold_machine_risk").where("risk_band IN ('ACT NOW','WATCH')").orderBy("priority_rank").toPandas()
manuals = spark.table(f"{C}.{S}.silver_manuals").toPandas()
history = spark.table(f"{C}.{S}.silver_work_orders").where("wo_type = 'CORRECTIVE'").toPandas()
print(len(risk), "machines need a plan; LLM endpoint:", LLM)

# COMMAND ----------

SYSTEM = """You are a senior reliability engineer at an automotive Tier-1 machining plant.
Write a concise maintenance work-order plan for a planner and a technician.
Use ONLY the sensor evidence, manual section and repair history provided.
- Never invent part numbers, specs, procedures, inspections or crew roles. If something is needed but not in the manual, write "not in manual".
- Only use numbers from the inputs. If you derive a figure (e.g. penalty hours beyond buffer x penalty rate), show the formula inline.
- Crew: name the technician skill as the failure mode (e.g. SPINDLE_BEARING-skilled technician); do not add other roles.
Format (markdown):
**Diagnosis** (1-2 sentences, cite the sensor evidence)
**Business impact** (OEM order, buffer hours, what happens if it fails unplanned)
**Plan** (numbered steps; step 1 must be lockout/tagout)
**Parts to stage** (part numbers from the manual)
**Window & crew** (a concrete window relative to now, e.g. "start within 6 h", chosen so the repair finishes before the finished-goods buffer runs out, or say how many hours of stock to pre-build / which order to move first if the repair is longer than the buffer; estimated hours; skill needed). State explicitly how the window protects the OEM delivery."""

def context_for(row: dict) -> dict:
    man = manuals[(manuals.machine_type == row["machine_type"]) & (manuals.failure_mode == row["likely_failure_mode"])]
    hist = history[history.machine_id == row["machine_id"]].sort_values("opened_ts").tail(3)
    return {
        "machine": f'{row["machine_id"]} ({row["model"]}, {row["plant_name"]} {row["line"]}, criticality {row["criticality"]})',
        "evidence": f'P(failure in 48h)={row["failure_prob_48h"]:.0%}; likely mode {row["likely_failure_mode"]}; {row["reason"]}',
        "order": f'{row["oem_customer"]} {row["part_number"]} {row["part_description"]}; finished-goods buffer {row["finished_goods_buffer_hours"]:.0f} h; OEM line-down penalty ${row["oem_line_down_penalty_usd_per_hour"]:,.0f}/h; exposure ${row["exposure_usd"]:,.0f}',
        "manual": (man.iloc[0]["title"] + ": " + man.iloc[0]["content"]) if len(man) else "No manual section found.",
        "history": "; ".join(f'{h.opened_ts:%Y-%m-%d} {h.failure_mode}: {h.notes}' for h in hist.itertuples()) or "No prior corrective work orders.",
    }

@mlflow.trace(name="generate_repair_plan", span_type="CHAIN")
def generate_repair_plan(machine: str, evidence: str, order: str, manual: str, history: str) -> str:
    user = f"Machine: {machine}\nSensor evidence: {evidence}\nProduction exposure: {order}\nManual: {manual}\nRepair history: {history}"
    resp = client.chat.completions.create(model=LLM, max_tokens=4000,
                                          messages=[{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}])
    content = resp.choices[0].message.content
    if isinstance(content, list):  # reasoning models return [{"type": "reasoning"...}, {"type": "text", "text": ...}]
        content = "".join(p.get("text") or "" for p in content if isinstance(p, dict) and p.get("type") == "text")
    elif isinstance(content, str) and content.lstrip().startswith("[{"):
        parts = json.loads(content)
        content = "".join(p.get("text") or "" for p in parts if p.get("type") == "text")
    return content.strip()

# COMMAND ----------

plans = []
for row in risk.to_dict("records"):
    ctx = context_for(row)
    plans.append(dict(machine_id=row["machine_id"], priority_rank=int(row["priority_rank"]), likely_failure_mode=row["likely_failure_mode"],
                      repair_plan_md=generate_repair_plan(**ctx), llm_endpoint=LLM, context_json=json.dumps(ctx)))
plans_df = pd.DataFrame(plans)
from pyspark.sql import functions as F
spark.createDataFrame(plans_df).withColumn("generated_at", F.current_timestamp()) \
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{C}.{S}.gold_repair_plans")
print(f"Wrote {len(plans_df)} plans to {C}.{S}.gold_repair_plans\n")
print("=" * 100, f"\nTOP PRIORITY — {plans_df.iloc[0].machine_id}\n", "=" * 100)
print(plans_df.iloc[0].repair_plan_md)

# COMMAND ----------

# MAGIC %md ## Quality gate — MLflow GenAI evaluation

# COMMAND ----------

from mlflow.genai.scorers import Guidelines, scorer

PN = re.compile(r"\b[A-Z]{2}-\d{1,4}(?:-[A-Z0-9]{1,3})?\b")   # manual part-number shape, e.g. SB-7014-P4, CP-220-SK

@scorer
def cites_manual_part_number(inputs, outputs) -> bool:
    """Deterministic grounding check: plan cites >=1 manual part number and no part number absent from the manual."""
    manual_pns = set(PN.findall(inputs["manual"]))
    plan_pns = {p for p in PN.findall(outputs) if not p.startswith(("WO-", "PO-"))}
    return bool(plan_pns & manual_pns) and plan_pns <= manual_pns

JUDGE = f"databricks:/{LLM}"   # a strong judge; the default judge produced false negatives (see evidence/iterations)
scorers = [
    Guidelines(name="safety_first", model=JUDGE,
               guidelines="The plan's first numbered step must be lockout/tagout (LOTO) or equivalent energy isolation."),
    Guidelines(name="grounded", model=JUDGE,
               guidelines="Every part number, part name, numeric specification (preload, runout, pressure, vibration limit) and repair-time estimate in the response must appear in the manual or repair history in the inputs, and dollar or hour figures must be copied from the inputs or derived from them with the formula shown. "
                          "Generic good practice (recording readings, a functional test, cleaning up) is acceptable and is NOT a violation. Quantities not stated in the manual are acceptable. "
                          "Answer no only if the response states a part, spec or time that is absent from or contradicts the inputs."),
    Guidelines(name="business_aware", model=JUDGE,
               guidelines="The response must mention the OEM customer and the finished-goods buffer, and recommend a repair window that protects the OEM delivery."),
    cites_manual_part_number,
]
eval_data = [{"inputs": json.loads(c)} for c in plans_df.context_json]
results = mlflow.genai.evaluate(data=eval_data, predict_fn=generate_repair_plan, scorers=scorers)
print(json.dumps({k: round(v, 3) for k, v in results.metrics.items()}, indent=2))
