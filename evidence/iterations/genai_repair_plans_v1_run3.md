# `genai_repair_plans` — executed notebook with outputs

Task run `850742833031332` · result **SUCCESS** · exported from job run `970702408484733`

# Meridian Uptime — 04 · GenAI repair plans (grounded, traced, evaluated)

For every machine in **ACT NOW / WATCH**, an LLM drafts the work-order plan a maintenance
planner would otherwise assemble by hand from three systems: the sensor evidence (model),
the OEM manual section for the likely failure mode, and this machine's own repair history (CMMS).

- Grounding: manual + history are retrieved by key (machine_type × failure_mode) from governed UC tables
- Every call is captured with **MLflow Tracing**
- Quality gate: **MLflow GenAI evaluation** — LLM judges (safety/LOTO, grounding, actionability)
  plus a deterministic check that the plan cites the correct manual part number.

```python
%pip install -q "mlflow[databricks]>=3.1" openai
%restart_python
```
**Output:**
```text
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

```python
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
```
**Output:**
```text
If you are using MLflow Tracing, you can migrate your traces to Unity Catalog for unlimited storage, fine-grained access controls, and queryability from notebooks, SQL, and dashboards. Learn more: https://docs.databricks.com/aws/en/mlflow3/genai/tracing/migrate-traces-to-uc
/home/spark-575ee77b-d4bf-4abf-9f2a-15/.ipykernel/76/command-7172842707123241-1167726450:14: DeprecationWarning: get_open_ai_client() is deprecated. Please install the databricks-openai package and use 'from databricks_openai import DatabricksOpenAI' instead. See https://api-docs.databricks.com/python/databricks-ai-bridge/latest/databricks_openai.html for more information.
  client = WorkspaceClient().serving_endpoints.get_open_ai_client()

2 machines need a plan; LLM endpoint: databricks-claude-sonnet-5-5
```

```python
SYSTEM = """You are a senior reliability engineer at an automotive Tier-1 machining plant.
Write a concise maintenance work-order plan for a planner and a technician.
Use ONLY the sensor evidence, manual section and repair history provided. Never invent part numbers.
Format (markdown):
**Diagnosis** (1-2 sentences, cite the sensor evidence)
**Business impact** (OEM order, buffer hours, what happens if it fails unplanned)
**Plan** (numbered steps; step 1 must be lockout/tagout)
**Parts to stage** (part numbers from the manual)
**Window & crew** (recommended repair window, estimated hours, skill needed)"""

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
```

```python
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
```
**Output:**
```text
2026/11/24 19:20:03 WARNING mlflow.utils.autologging_utils: MLflow autologging encountered a warning: "/databricks/python/lib/python3.12/site-packages/pydantic/main.py:475: UserWarning: Pydantic serializer warnings:
  PydanticSerializationUnexpectedValue(Expected `str` - serialized value may not be as expected [field_name='content', input_value=[{'type': 'reasoning', 's...OTO and Z-axis check."}], input_type=list])"
2026/11/24 19:20:19 WARNING mlflow.utils.autologging_utils: MLflow autologging encountered a warning: "/databricks/python/lib/python3.12/site-packages/pydantic/main.py:475: UserWarning: Pydantic serializer warnings:
  PydanticSerializationUnexpectedValue(Expected `str` - serialized value may not be as expected [field_name='content', input_value=[{'type': 'reasoning', 's...achine is taken down."}], input_type=list])"

Wrote 2 plans to main.meridian.gold_repair_plans

==================================================================================================== 
TOP PRIORITY — M-SAL-07
 ====================================================================================================
**Diagnosis**
Spindle bearing failure is imminent. The model gives P(failure in 48 h) = 100%, and spindle vibration is +2.2 mm/s above its 14-day baseline (12.1σ). The prior Z-axis servo fault (2026-11-07, amplifier replaced) is a different subsystem and does not explain this signature.

**Business impact**
- **Order at risk:** Orion Motors BRK-4471 brake caliper bracket, with a $22,000/h line-down penalty and $452,958 exposure.
- **Buffer:** 12 h of finished goods.
- **Planned repair:** 10-14 h. Even planned, this is at most about 2 h beyond the buffer (up to about $44k) if the buffer is full at start. Pre-building stock before tag-out would close that gap.
- **Unplanned seizure:** 18-40 h, with collateral damage to the drawbar and housing. That is 6-28 h beyond the buffer, or roughly $132k-$616k in penalties (my arithmetic: hours beyond buffer × $22k/h), plus extra parts.

**Plan**
1. **LOTO:** Lock out and tag out M-SAL-07, and verify zero energy.
2. Before teardown, record the absolute spindle vibration and the temperature delta vs baseline. The manual's early-failure signs are vibration > 4 mm/s and +8 °C. This confirms the diagnosis and documents the as-found condition.
3. Remove the drawbar and spindle cartridge.
4. Press out the bearings. Inspect the housing bore (max 0.005 mm runout) and check the drawbar for collateral damage.
5. Install the new bearing set, spacer kit and grease GBU-Y. Set preload to 1,200 N.
6. Run-in: 15 min at 2,000 rpm, 15 min at 6,000 rpm, then 15 min at 10,000 rpm. Vibration must stay < 2.8 mm/s.
7. During run-in, watch Z-axis load for overload trips, given the recent servo history. Then release the machine to production.

**Parts to stage**
- Angular-contact bearing set, P/N **SB-7014-P4**, qty 2
- Spacer kit (no P/N in the manual, so the planner must look up the number before ordering)
- Grease **GBU-Y**

**Window & crew**
- **Window:** Start at the earliest opportunity, well inside the 48 h horizon. Do not wait for the full window to run out, because the risk is 100%. Use the shift with the fullest buffer.
- **Estimated duration:** 10-14 h, including the 45 min run-in.
- **Crew:** One senior machine-tool/spindle technician qualified in precision bearing installation and preload, plus one assistant. A controls or electrical technician should be on call for the LOTO and Z-axis check.
```

## Quality gate — MLflow GenAI evaluation

```python
from mlflow.genai.scorers import Guidelines, scorer

PN = re.compile(r"\b[A-Z]{2}-\d{1,4}(?:-[A-Z0-9]{1,3})?\b")   # manual part-number shape, e.g. SB-7014-P4, CP-220-SK

@scorer
def cites_manual_part_number(inputs, outputs) -> bool:
    """Deterministic grounding check: plan cites >=1 manual part number and no part number absent from the manual."""
    manual_pns = set(PN.findall(inputs["manual"]))
    plan_pns = {p for p in PN.findall(outputs) if not p.startswith(("WO-", "PO-"))}
    return bool(plan_pns & manual_pns) and plan_pns <= manual_pns

scorers = [
    Guidelines(name="safety_first", guidelines="The plan's first step must be lockout/tagout (LOTO) or equivalent energy isolation."),
    Guidelines(name="grounded", guidelines="All part numbers, torque/preload values and time estimates must come from the manual or history in the inputs; nothing invented."),
    Guidelines(name="business_aware", guidelines="The response must mention the OEM customer and the finished-goods buffer, and recommend a repair window that protects the OEM delivery."),
    cites_manual_part_number,
]
eval_data = [{"inputs": json.loads(c)} for c in plans_df.context_json]
results = mlflow.genai.evaluate(data=eval_data, predict_fn=generate_repair_plan, scorers=scorers)
print(json.dumps({k: round(v, 3) for k, v in results.metrics.items()}, indent=2))
```
**Output:**
```text
2026/11/24 19:20:22 INFO mlflow.models.evaluation.utils.trace: Auto tracing is temporarily enabled during the model evaluation for computing some metrics and debugging. To disable tracing, call `mlflow.autolog(disable=True)`.
2026/11/24 19:20:22 INFO mlflow.genai.utils.data_validation: Testing model prediction with the first sample in the dataset. To disable this check, set the MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION environment variable to True.

{
  "cites_manual_part_number/mean": 1.0,
  "business_aware/mean": 1.0,
  "safety_first/mean": 1.0,
  "grounded/mean": 0.0
}
```
