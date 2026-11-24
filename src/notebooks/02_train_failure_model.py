# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Uptime — 02 · Failure-prediction model (MLflow + Unity Catalog)
# MAGIC
# MAGIC **Question the model answers:** *"Will this machine have an unplanned failure in the next 48 hours?"*
# MAGIC
# MAGIC Why 48 h: a planned spindle-bearing swap takes 10-14 h, a seizure 18-40 h; with 48 h of warning
# MAGIC planners can move the OEM order to a sister machine or build buffer stock, and maintenance can
# MAGIC stage parts + the right technician.
# MAGIC
# MAGIC - Features: rolling 6 h / 24 h sensor statistics and drift vs each machine's own 14-day baseline
# MAGIC - Model: gradient-boosted trees (sklearn `HistGradientBoostingClassifier`)
# MAGIC - Validation: **time-based split** (no leakage), event-level recall and lead time, alert volume
# MAGIC - Tracking: MLflow experiment; model registered to **Unity Catalog** with alias `champion`

# COMMAND ----------

# MAGIC %pip install -q "mlflow[databricks]>=3.1" scikit-learn==1.5.2
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "meridian")
C, S = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
MODEL_NAME = f"{C}.{S}.machine_failure_48h"
HORIZON_H = 48

import json
import numpy as np
import pandas as pd
import mlflow
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score, average_precision_score, precision_score, recall_score

mlflow.set_registry_uri("databricks-uc")
me = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{me}/meridian-uptime-failure-model")

# COMMAND ----------

# MAGIC %md ## 1 · Load governed silver tables

# COMMAND ----------

tele = spark.table(f"{C}.{S}.silver_telemetry").toPandas()
fails = spark.table(f"{C}.{S}.gold_failure_events").select("machine_id", "failure_ts", "failure_mode", "downtime_hours").toPandas()
machines = spark.table(f"{C}.{S}.silver_machines").toPandas()
tele = tele.sort_values(["machine_id", "ts"]).reset_index(drop=True)
print(f"telemetry rows: {len(tele):,} | machines: {tele.machine_id.nunique()} | failures: {len(fails)}")
print(fails.failure_mode.value_counts().to_string())

# COMMAND ----------

# MAGIC %md ## 2 · Feature engineering

# COMMAND ----------

SENSORS = ["spindle_vibration_mm_s", "spindle_temp_c", "spindle_load_pct", "coolant_pressure_bar", "power_kw"]

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    out = []
    for mid, g in df.groupby("machine_id", sort=False):
        g = g.set_index("ts").sort_index()
        run = g[SENSORS].where(g.status == "RUNNING")          # ignore down hours in statistics
        f = pd.DataFrame(index=g.index)
        base = run.rolling("336h", min_periods=48).median().shift(24)   # own 14-day baseline, lagged 1 day
        for c in SENSORS:
            f[f"{c}_mean_6h"] = run[c].rolling("6h", min_periods=3).mean()
            f[f"{c}_mean_24h"] = run[c].rolling("24h", min_periods=12).mean()
            f[f"{c}_std_24h"] = run[c].rolling("24h", min_periods=12).std()
            f[f"{c}_drift_24h"] = f[f"{c}_mean_24h"] - base[c]
        f["vib_max_6h"] = run["spindle_vibration_mm_s"].rolling("6h", min_periods=3).max()
        f["vib_slope_24h"] = f["spindle_vibration_mm_s_mean_6h"] - f["spindle_vibration_mm_s_mean_6h"].shift(18)
        f["press_slope_24h"] = f["coolant_pressure_bar_mean_6h"] - f["coolant_pressure_bar_mean_6h"].shift(18)
        f["load_spikiness_24h"] = f["spindle_load_pct_std_24h"] / f["spindle_load_pct_mean_24h"]
        f["status"] = g["status"]
        f["machine_id"] = mid
        out.append(f.reset_index())
    return pd.concat(out, ignore_index=True)

feat = build_features(tele)
feat = feat.merge(machines[["machine_id", "machine_type", "criticality", "install_year"]], on="machine_id")
feat["machine_age_years"] = 2026 - feat["install_year"]
feat["is_criticality_a"] = (feat["criticality"] == "A").astype(int)
FEATURES = [c for c in feat.columns if c.endswith(("_6h", "_24h"))] + ["machine_age_years", "is_criticality_a"]
print(len(FEATURES), "features")
# persist the feature table so batch scoring (notebook 03) uses exactly the same features
(spark.createDataFrame(feat[["machine_id", "ts", "status"] + FEATURES])
      .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{C}.{S}.gold_machine_features_hourly"))
spark.sql(f"COMMENT ON TABLE {C}.{S}.gold_machine_features_hourly IS 'Hourly ML features per machine (rolling stats + drift vs own 14-day baseline). Produced by notebook 02.'")

# COMMAND ----------

# MAGIC %md ## 3 · Labels: failure within the next 48 h (right-censored tail excluded)

# COMMAND ----------

feat["label"] = 0
feat["hours_to_failure"] = np.nan
for r in fails.itertuples():
    m = (feat.machine_id == r.machine_id) & (feat.ts < r.failure_ts) & (feat.ts >= r.failure_ts - pd.Timedelta(hours=HORIZON_H))
    feat.loc[m, "label"] = 1
    feat.loc[m, "hours_to_failure"] = (r.failure_ts - feat.loc[m, "ts"]).dt.total_seconds() / 3600
as_of = feat.ts.max() + pd.Timedelta(hours=1)
labelled = feat[(feat.status == "RUNNING") & (feat.ts < as_of - pd.Timedelta(hours=HORIZON_H))].dropna(subset=["spindle_vibration_mm_s_drift_24h"])
split_ts = labelled.ts.min() + (labelled.ts.max() - labelled.ts.min()) * 0.60
train, test = labelled[labelled.ts < split_ts], labelled[labelled.ts >= split_ts]
val_ts = train.ts.min() + (train.ts.max() - train.ts.min()) * 0.75          # inner validation for threshold tuning
fit_part, val_part = train[train.ts < val_ts], train[train.ts >= val_ts]
print(f"as_of={as_of}  val_split={val_ts:%Y-%m-%d}  test_split={split_ts:%Y-%m-%d}")
print(f"train rows {len(train):,} (pos {train.label.mean():.2%}) | test rows {len(test):,} (pos {test.label.mean():.2%})")

# COMMAND ----------

# MAGIC %md ## 4 · Train, tune the alert threshold on validation, evaluate on the hold-out window, register
# MAGIC An **alert** = probability above threshold for 2 consecutive hours (filters one-off sensor spikes).
# MAGIC An alert is **false** only if no failure follows on that machine within 96 h — bearing wear shows
# MAGIC up to 4 days ahead, so a 60-h-early alert is an early warning, not a false alarm.
# MAGIC The threshold is chosen on the validation window to maximize **net $ value**, then frozen for the test window.

# COMMAND ----------

CM_PER_HOUR, PLANNED_FRACTION, INSPECTION_H, EARLY_WINDOW_H = 1450, 0.45, 1.5, 96
params = dict(max_iter=300, learning_rate=0.06, max_leaf_nodes=31, l2_regularization=1.0, random_state=7)

def alerts(df, proba, thr):
    t = df[["machine_id", "ts"]].assign(p=proba).sort_values(["machine_id", "ts"])
    above = t.p >= thr
    t["alert"] = above & above.groupby(t.machine_id).shift(1, fill_value=False)
    return t

def evaluate(df, proba, thr):
    t = alerts(df, proba, thr)
    win = (fails.failure_ts > df.ts.min() + pd.Timedelta(hours=HORIZON_H)) & (fails.failure_ts <= df.ts.max() + pd.Timedelta(hours=2))
    rows = []
    for r in fails[win].itertuples():
        w = t[(t.machine_id == r.machine_id) & (t.ts < r.failure_ts - pd.Timedelta(hours=2)) & (t.ts >= r.failure_ts - pd.Timedelta(hours=HORIZON_H)) & t.alert]
        lead = (r.failure_ts - w.ts.min()).total_seconds() / 3600 if len(w) else 0.0
        rows.append(dict(machine_id=r.machine_id, failure_ts=r.failure_ts, failure_mode=r.failure_mode,
                         downtime_hours=r.downtime_hours, caught=bool(len(w)), lead_time_hours=round(lead, 1)))
    ev = pd.DataFrame(rows, columns=["machine_id", "failure_ts", "failure_mode", "downtime_hours", "caught", "lead_time_hours"])
    a = t[t.alert].merge(fails[["machine_id", "failure_ts"]], on="machine_id", how="left")
    a["precedes_failure"] = (a.failure_ts > a.ts) & (a.failure_ts <= a.ts + pd.Timedelta(hours=EARLY_WINDOW_H))
    good = a.groupby(["machine_id", "ts"]).precedes_failure.any().reset_index()
    fp_days = good[~good.precedes_failure].assign(d=lambda x: x.ts.dt.date)[["machine_id", "d"]].drop_duplicates()
    value = (ev[ev.caught].downtime_hours * (1 - PLANNED_FRACTION)).sum() * CM_PER_HOUR - len(fp_days) * INSPECTION_H * CM_PER_HOUR
    return ev, fp_days, value

# threshold tuning on validation (model fit on the earlier part of train only)
m_val = HistGradientBoostingClassifier(**params).fit(fit_part[FEATURES], fit_part.label)
p_val = m_val.predict_proba(val_part[FEATURES])[:, 1]
grid = []
for thr in np.round(np.arange(0.2, 0.86, 0.05), 2):
    ev_v, fp_v, val_v = evaluate(val_part, p_val, thr)
    grid.append(dict(threshold=thr, val_failures=len(ev_v), val_caught=int(ev_v.caught.sum()), val_false_alert_days=len(fp_v), val_net_value_usd=round(val_v)))
grid = pd.DataFrame(grid)
ALERT_THRESHOLD = float(grid.sort_values(["val_net_value_usd", "threshold"], ascending=[False, False]).iloc[0].threshold)
print(grid.to_string(index=False)); print("\nChosen alert threshold:", ALERT_THRESHOLD)

with mlflow.start_run(run_name="hgb_failure_48h") as run:
    model = HistGradientBoostingClassifier(**params).fit(train[FEATURES], train.label)
    p_test = model.predict_proba(test[FEATURES])[:, 1]
    t_alert = alerts(test, p_test, ALERT_THRESHOLD).set_index(["machine_id", "ts"]).alert
    pred = t_alert.reindex(pd.MultiIndex.from_frame(test[["machine_id", "ts"]])).fillna(False).astype(int).values
    ev, fp_days, net_value = evaluate(test, p_test, ALERT_THRESHOLD)
    weeks = (test.ts.max() - test.ts.min()).days / 7
    predictable = ev[ev.failure_mode != "TOOL_CHANGER"]
    metrics = {
        "roc_auc": roc_auc_score(test.label, p_test),
        "pr_auc": average_precision_score(test.label, p_test),
        "hour_precision_at_thr": precision_score(test.label, pred, zero_division=0),
        "hour_recall_at_thr": recall_score(test.label, pred, zero_division=0),
        "event_recall_all_modes": float(ev.caught.mean()),
        "event_recall_predictable_modes": float(predictable.caught.mean()),
        "event_recall_tool_changer": float(ev[ev.failure_mode == "TOOL_CHANGER"].caught.mean()) if (ev.failure_mode == "TOOL_CHANGER").any() else float("nan"),
        "median_lead_time_hours": float(ev[ev.caught].lead_time_hours.median()) if ev.caught.any() else 0.0,
        "false_alert_machine_days_per_week_fleet": len(fp_days) / weeks,
        "false_alerts_per_machine_per_month": len(fp_days) / 60 / (weeks / 4.345),
        "test_failures": float(len(ev)),
    }
    mlflow.log_params({**params, "alert_threshold": ALERT_THRESHOLD, "horizon_h": HORIZON_H, "n_features": len(FEATURES),
                       "split_ts": str(split_ts), "train_rows": len(train), "test_rows": len(test), "alert_persistence_h": 2})
    mlflow.log_metrics(metrics)
    mlflow.log_table(grid, "threshold_tuning_validation.json")
    mlflow.log_table(ev.assign(failure_ts=ev.failure_ts.astype(str)), "event_level_backtest.json")
    sig = mlflow.models.infer_signature(test[FEATURES].head(50), p_test[:50])
    info = mlflow.sklearn.log_model(model, name="model", signature=sig, input_example=test[FEATURES].head(3),
                                    registered_model_name=MODEL_NAME, pyfunc_predict_fn="predict_proba")
    RUN_ID = run.info.run_id

print(json.dumps({k: round(v, 4) for k, v in metrics.items()}, indent=2))
print("\nEvent-level backtest (test window):")
print(ev.to_string(index=False))

# COMMAND ----------

from mlflow import MlflowClient
client = MlflowClient()
version = max(int(v.version) for v in client.search_model_versions(f"name='{MODEL_NAME}'"))
client.set_registered_model_alias(MODEL_NAME, "champion", version)
client.update_registered_model(MODEL_NAME, description="P(unplanned failure within 48h) per CNC machine-hour. Features: rolling sensor stats + drift vs own baseline. Owner: Reliability Engineering.")
print(f"Registered {MODEL_NAME} v{version} -> @champion (run {RUN_ID})")

# COMMAND ----------

# MAGIC %md ## 5 · Business backtest — what would this have saved in the test window?
# MAGIC Assumptions (documented, conservative): a caught failure becomes a planned repair that takes
# MAGIC **45 %** of the unplanned downtime and is scheduled into a changeover; contribution margin
# MAGIC **$1,450 / machine-hour**; each false alert costs a **1.5 h** inspection.

# COMMAND ----------

caught = ev[ev.caught]
hours_avoided = (caught.downtime_hours * (1 - PLANNED_FRACTION)).sum()
test_days = (test.ts.max() - test.ts.min()).days
backtest = pd.DataFrame([dict(test_window_days=test_days, failures=len(ev), caught=int(ev.caught.sum()),
                              downtime_hours_unplanned=float(ev.downtime_hours.sum()), downtime_hours_avoided=float(hours_avoided),
                              false_alert_machine_days=len(fp_days), net_value_usd=float(net_value),
                              annualized_net_value_usd=float(net_value * 365 / test_days),
                              model_version=str(version), run_id=RUN_ID)])
spark.createDataFrame(backtest).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{C}.{S}.gold_model_backtest")
with mlflow.start_run(run_id=RUN_ID):
    mlflow.log_metrics({"backtest_downtime_hours_avoided": float(hours_avoided), "backtest_net_value_usd": float(net_value)})
print(backtest.T.to_string(header=False))
dbutils.jobs.taskValues.set("model_version", str(version)) if hasattr(dbutils, "jobs") else None
