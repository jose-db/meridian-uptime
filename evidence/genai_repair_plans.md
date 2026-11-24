# `genai_repair_plans` — executed notebook with outputs

Task run `74944654146003` · result **SUCCESS** · exported from job run `266743852559069`

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
/home/spark-ce740221-42c3-4f09-b44a-33/.ipykernel/77/command-7172842707123410-1167726450:14: DeprecationWarning: get_open_ai_client() is deprecated. Please install the databricks-openai package and use 'from databricks_openai import DatabricksOpenAI' instead. See https://api-docs.databricks.com/python/databricks-ai-bridge/latest/databricks_openai.html for more information.
  client = WorkspaceClient().serving_endpoints.get_open_ai_client()

3 machines need a plan; LLM endpoint: databricks-claude-sonnet-5-5
```

```python
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
2026/11/24 19:36:45 WARNING mlflow.utils.autologging_utils: MLflow autologging encountered a warning: "/databricks/python/lib/python3.12/site-packages/pydantic/main.py:475: UserWarning: Pydantic serializer warnings:
  PydanticSerializationUnexpectedValue(Expected `str` - serialized value may not be as expected [field_name='content', input_value=[{'type': 'reasoning', 's...rs are in the inputs."}], input_type=list])"
2026/11/24 19:37:00 WARNING mlflow.utils.autologging_utils: MLflow autologging encountered a warning: "/databricks/python/lib/python3.12/site-packages/pydantic/main.py:475: UserWarning: Pydantic serializer warnings:
  PydanticSerializationUnexpectedValue(Expected `str` - serialized value may not be as expected [field_name='content', input_value=[{'type': 'reasoning', 's...air time are unknown."}], input_type=list])"
2026/11/24 19:37:15 WARNING mlflow.utils.autologging_utils: MLflow autologging encountered a warning: "/databricks/python/lib/python3.12/site-packages/pydantic/main.py:475: UserWarning: Pydantic serializer warnings:
  PydanticSerializationUnexpectedValue(Expected `str` - serialized value may not be as expected [field_name='content', input_value=[{'type': 'reasoning', 's...repair overruns 16 h.'}], input_type=list])"

Wrote 3 plans to main.meridian.gold_repair_plans

==================================================================================================== 
TOP PRIORITY — M-SAL-07
 ====================================================================================================
**Diagnosis**
M-SAL-07 shows spindle vibration +2.2 mm/s above its 14-day baseline (12.1σ), with P(failure in 48 h) = 100% and likely mode SPINDLE_BEARING. The manual's early-sign thresholds are vibration > 4 mm/s and temperature +8 °C over baseline. The inputs give no absolute vibration or temperature reading, so we cannot say how close the machine is to those limits.

**Business impact**
- The machine feeds Orion Motors BRK-4471 (brake caliper bracket). The finished-goods buffer is 12 h and the OEM line-down penalty is $22,000/h. The stated exposure is $441,995.
- **Unplanned seizure:** the manual gives 18-40 h of repair, with collateral damage to the drawbar and housing. Penalty beyond the buffer is (18 − 12) × $22,000 = $132,000 up to (40 − 12) × $22,000 = $616,000.
- **Planned repair:** 10-14 h. At the 14 h upper end it overruns the buffer by 2 h, or (14 − 12) × $22,000 = $44,000, unless stock is pre-built.

**Plan**
1. LOTO and verify zero energy.
2. Remove the drawbar and spindle cartridge.
3. Press out the bearings and inspect the housing bore (max 0.005 mm runout). If out of limit, the corrective action is not in manual, so escalate before continuing.
4. Install the new bearing set and spacer kit, with grease GBU-Y. Set preload to 1,200 N.
5. Run-in: 15 min at 2,000 rpm, 15 min at 6,000 rpm, then 15 min at 10,000 rpm. Vibration must be < 2.8 mm/s.
6. Release to production only if step 5 passes. Otherwise hold the machine and escalate.
7. Until the stop, monitor the machine. If vibration exceeds 4 mm/s or temperature rises 8 °C over baseline, stop it immediately and start the repair, since these are the manual's early signs.

The Y-axis servo repair (2026-11-07) is a different failure mode. The manual has no linked step, so none is added.

**Parts to stage**
- Angular-contact bearing set, P/N SB-7014-P4, ×2
- Spacer kit (P/N not in manual)
- Grease GBU-Y
- Drawbar and housing spares: not in manual. The manual only says the drawbar and housing are at risk in a seizure.

**Window & crew**
- **Start within 6 h.** With a 100% failure probability inside 48 h, waiting risks the 18-40 h seizure scenario.
- **Pre-build:** before the stop, build at least 2 extra hours of BRK-4471 stock, taking the buffer from 12 h to at least 14 h. This covers the 14 h worst-case planned repair. The inputs do not say whether the buffer is currently full, so confirm that first. The pre-build hours come from the same vibrating spindle, so keep the load light and stop at the early-sign limits.
- **Estimated duration:** 10-14 h.
- **Crew:** one SPINDLE_BEARING-skilled technician.
- **Delivery protection:** with at least 14 h of stock against a repair of at most 14 h, the buffer does not run out before the spindle is back in service. That avoids the $22,000/h line-down penalty and the $132,000-$616,000 seizure scenario.
- **If stock cannot reach 14 h:** the buffer shortfall is the repair hours minus the stock hours, at $22,000/h. Moving other orders is not possible to plan, because no other orders are in the inputs.
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
```
**Output:**
```text
2026/11/24 19:37:18 INFO mlflow.models.evaluation.utils.trace: Auto tracing is temporarily enabled during the model evaluation for computing some metrics and debugging. To disable tracing, call `mlflow.autolog(disable=True)`.
2026/11/24 19:37:18 INFO mlflow.genai.utils.data_validation: Testing model prediction with the first sample in the dataset. To disable this check, set the MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION environment variable to True.

{
  "cites_manual_part_number/mean": 1.0,
  "safety_first/mean": 1.0,
  "business_aware/mean": 1.0,
  "grounded/mean": 1.0
}
```
