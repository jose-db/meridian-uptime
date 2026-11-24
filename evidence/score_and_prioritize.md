# `score_and_prioritize` — executed notebook with outputs

Task run `190863214680454` · result **SUCCESS** · exported from job run `266743852559069`

# Meridian Uptime — 03 · Score the fleet & rank by **$ exposure**

Probability alone doesn't tell a planner what to do first. A 70 % risk on a Class-C lathe
with 40 h of finished-goods buffer is less urgent than a 60 % risk on the machine feeding
Orion Motors' brake line with 12 h of buffer. This notebook turns model output into a
**ranked action list**:

`exposure_usd = P(fail 48h) × [ downtime_h × $1,450 + parts + max(0, downtime_h − buffer_h) × OEM penalty/h ]`

Output: `gold_machine_risk` (one row per machine, as of the latest telemetry hour).

```python
%pip install -q "mlflow[databricks]>=3.1" scikit-learn==1.5.2
%restart_python
```
**Output:**
```text
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

```python
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "meridian")
C, S = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
MODEL_URI = f"models:/{C}.{S}.machine_failure_48h@champion"
CM_PER_HOUR = 1450

import numpy as np, pandas as pd, mlflow
mlflow.set_registry_uri("databricks-uc")

feat = spark.table(f"{C}.{S}.gold_machine_features_hourly").toPandas()
latest = feat.sort_values("ts").groupby("machine_id").tail(1).reset_index(drop=True)
model = mlflow.pyfunc.load_model(MODEL_URI)
FEATURES = [c.name for c in model.metadata.get_input_schema().inputs]
proba = np.asarray(model.predict(latest[FEATURES]))
latest["failure_prob_48h"] = proba[:, 1] if proba.ndim == 2 else proba
print("Scored", len(latest), "machines with", MODEL_URI, "as of", latest.ts.max())
```
**Output:**
```text
Scored 60 machines with models:/main.meridian.machine_failure_48h@champion as of 2026-11-24 05:00:00
```

## Explainability: which signal is drifting → likely failure mode
Drift is expressed in fleet-standard-deviations so signals with different units are comparable.

```python
SIGNAL_TO_MODE = {"spindle_vibration_mm_s": "SPINDLE_BEARING", "spindle_temp_c": "SPINDLE_BEARING",
                  "coolant_pressure_bar": "COOLANT_PUMP", "spindle_load_pct": "SERVO_AXIS"}
SIGN = {"spindle_vibration_mm_s": 1, "spindle_temp_c": 1, "coolant_pressure_bar": -1, "spindle_load_pct": 1}
fleet_sd = {s: feat[f"{s}_std_24h"].median() for s in SIGNAL_TO_MODE}
drv = pd.DataFrame({s: SIGN[s] * latest[f"{s}_drift_24h"] / fleet_sd[s] for s in SIGNAL_TO_MODE})
latest["top_signal"] = drv.idxmax(axis=1)
latest["top_signal_sigma"] = drv.max(axis=1).round(1)
latest["likely_failure_mode"] = latest["top_signal"].map(SIGNAL_TO_MODE)
UNIT = {"spindle_vibration_mm_s": "mm/s", "spindle_temp_c": "°C", "coolant_pressure_bar": "bar", "spindle_load_pct": "% load"}
LABEL = {"spindle_vibration_mm_s": "Spindle vibration", "spindle_temp_c": "Spindle temperature",
         "coolant_pressure_bar": "Coolant pressure", "spindle_load_pct": "Spindle load"}
latest["reason"] = latest.apply(lambda r: f"{LABEL[r.top_signal]} {r[r.top_signal + '_drift_24h']:+.1f} {UNIT[r.top_signal]} vs its 14-day baseline ({r.top_signal_sigma:.1f}σ)"
                                if r.top_signal_sigma >= 1 else "No significant sensor drift vs 14-day baseline", axis=1)
```

## Join to production schedule + historical cost of each failure mode

```python
machines = spark.table(f"{C}.{S}.silver_machines").toPandas()
po = spark.table(f"{C}.{S}.silver_production_orders").toPandas()
next_po = po.sort_values("start_ts").groupby("machine_id").head(1)
mode_cost = spark.table(f"{C}.{S}.gold_failure_events").groupBy("failure_mode").agg(
    {"downtime_hours": "avg", "parts_cost_usd": "avg"}).toPandas().rename(
    columns={"avg(downtime_hours)": "exp_downtime_h", "avg(parts_cost_usd)": "exp_parts_usd"})

r = (latest[["machine_id", "ts", "failure_prob_48h", "likely_failure_mode", "reason", "top_signal_sigma",
             "spindle_vibration_mm_s_mean_6h", "spindle_temp_c_mean_6h", "coolant_pressure_bar_mean_6h", "spindle_load_pct_mean_6h"]]
     .merge(machines, on="machine_id").merge(mode_cost, left_on="likely_failure_mode", right_on="failure_mode", how="left")
     .merge(next_po[["machine_id", "production_order_id", "oem_customer", "part_number", "part_description",
                     "oem_line_down_penalty_usd_per_hour", "finished_goods_buffer_hours"]], on="machine_id", how="left"))
r["penalty_hours"] = (r.exp_downtime_h - r.finished_goods_buffer_hours).clip(lower=0)
r["cost_if_fails_usd"] = r.exp_downtime_h * CM_PER_HOUR + r.exp_parts_usd + r.penalty_hours * r.oem_line_down_penalty_usd_per_hour
r["exposure_usd"] = (r.failure_prob_48h * r.cost_if_fails_usd).round(0)
r["risk_band"] = pd.cut(r.failure_prob_48h, [-0.01, 0.3, 0.6, 1.01], labels=["LOW", "WATCH", "ACT NOW"]).astype(str)
r["recommended_action"] = np.select(
    [r.risk_band == "ACT NOW", r.risk_band == "WATCH"],
    ["Schedule planned repair within 24h; move OEM order or build buffer", "Inspect next changeover; stage parts"],
    "Normal PM cadence")
# planners work ACT NOW first, then WATCH; within a band the $ exposure decides the order
r["_band_order"] = r.risk_band.map({"ACT NOW": 0, "WATCH": 1, "LOW": 2})
r = r.sort_values(["_band_order", "exposure_usd"], ascending=[True, False]).drop(columns=["_band_order"])
r["priority_rank"] = range(1, len(r) + 1)
r = r.rename(columns={"ts": "as_of_ts"}).drop(columns=["failure_mode"])

(spark.createDataFrame(r).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{C}.{S}.gold_machine_risk"))
spark.sql(f"COMMENT ON TABLE {C}.{S}.gold_machine_risk IS 'One row per CNC machine: P(failure 48h) from UC model machine_failure_48h@champion, likely failure mode + reason, next OEM order, and $ exposure. Ranked by priority_rank.'")

show = ["priority_rank", "machine_id", "plant_name", "criticality", "failure_prob_48h", "risk_band", "likely_failure_mode",
        "reason", "oem_customer", "finished_goods_buffer_hours", "exposure_usd"]
pd.set_option("display.width", 250); pd.set_option("display.max_colwidth", 70)
print(r[show].head(10).to_string(index=False))
print("\nRisk bands:", r.risk_band.value_counts().to_dict(), "| total exposure next 48h: $%s" % f"{r.exposure_usd.sum():,.0f}")
```
**Output:**
```text
priority_rank machine_id plant_name criticality  failure_prob_48h risk_band likely_failure_mode                                                     reason   oem_customer  finished_goods_buffer_hours  exposure_usd
             1   M-SAL-07   Saltillo           A          0.999834   ACT NOW     SPINDLE_BEARING Spindle vibration +2.2 mm/s vs its 14-day baseline (12.1σ)   Orion Motors                         12.0      441995.0
             2   M-QRO-03  Querétaro           A          0.791069   ACT NOW          SERVO_AXIS     Spindle load +6.3 % load vs its 14-day baseline (1.1σ)       Vanta EV                         35.0       20916.0
             3   M-MTY-12  Monterrey           A          0.976141   ACT NOW        COOLANT_PUMP    Coolant pressure -3.7 bar vs its 14-day baseline (3.1σ) Kestrel Trucks                         16.0       16882.0
             4   M-MTY-17  Monterrey           C          0.148324       LOW     SPINDLE_BEARING             No significant sensor drift vs 14-day baseline       Vanta EV                         40.0        8301.0
             5   M-SAL-04   Saltillo           B          0.004310       LOW     SPINDLE_BEARING             No significant sensor drift vs 14-day baseline       Vanta EV                         12.0        1376.0
             6   M-QRO-16  Querétaro           B          0.005597       LOW     SPINDLE_BEARING             No significant sensor drift vs 14-day baseline Kestrel Trucks                         12.0        1197.0
             7   M-QRO-07  Querétaro           A          0.003336       LOW     SPINDLE_BEARING             No significant sensor drift vs 14-day baseline Kestrel Trucks                         19.0         503.0
             8   M-QRO-17  Querétaro           C          0.000741       LOW     SPINDLE_BEARING             No significant sensor drift vs 14-day baseline   Orion Motors                         19.0         214.0
             9   M-QRO-20  Querétaro           B          0.000312       LOW     SPINDLE_BEARING             No significant sensor drift vs 14-day baseline   Orion Motors                         11.0         145.0
            10   M-QRO-15  Querétaro           A          0.002046       LOW     SPINDLE_BEARING             No significant sensor drift vs 14-day baseline       Vanta EV                         29.0         131.0

Risk bands: {'LOW': 57, 'ACT NOW': 3} | total exposure next 48h: $492,640
```
