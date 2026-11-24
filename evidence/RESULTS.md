# Results — headline numbers (all from committed run output)

Final end-to-end job run `266743852559069`: **SUCCESS** on all 7 tasks (Gen AI + Lakebase tasks repair-run twice after evaluation findings; see [`iterations/README.md`](iterations/README.md)).

## Data & pipeline — [`lakeflow_pipeline.md`](lakeflow_pipeline.md), [`land_raw_files.md`](land_raw_files.md)
- 172,800 hourly sensor rows (60 machines × 120 days), 106 historical failures, 346 work orders, production orders, roster, manuals → UC Volume
- Lakeflow expectations dropped exactly the 20 null-machine and 20 impossible-temperature rows seeded as dirt (172,760 rows in silver)

## Governance — [`uc_governance.md`](uc_governance.md)
- `mask_pii` on `silver_technicians.phone` / `.email`; `plant_filter` row filter on `plant_id`, both attached in pipeline DDL
- Lineage bronze → silver → gold captured in `system.access.table_lineage`
- UC model `machine_failure_48h`, versions 1-4, alias `champion` → v4
- App service principal: `USE SCHEMA, SELECT` in UC; in Lakebase SELECT on serving tables and INSERT/UPDATE **only** on `work_orders`; no PII columns published

## ML — [`train_failure_model.md`](train_failure_model.md)
Held-out last 46 days (time-based split at 2026-10-07; threshold 0.45 chosen on an inner validation window):

| Metric | Value |
|---|---|
| Failures in test window | 43 |
| Caught 2-48 h ahead — predictable modes (bearing, coolant, servo) | **30 / 30 (100 %)** |
| Caught — tool-changer jams (no precursor, by design) | 0 / 13 |
| Median lead time | **31.5 h** |
| False alerts | 9 machine-days fleet-wide = **0.10 / machine / month** |
| Hour-level precision / recall at threshold | 0.94 / 0.51 |
| ROC-AUC / PR-AUC (hour level) | 0.80 / 0.60 |
| Downtime hours avoided (planned repair = 45 % of unplanned) | 336 of 676 h |
| Net value in window (CM $1,450/h − $2,175/false alert) | **$467,698 → ≈ $3.7 M / year** |

## Prioritization — [`score_and_prioritize.md`](score_and_prioritize.md)
3 machines ACT NOW, $479,793 total 48-h exposure:
1. **M-SAL-07** · spindle bearing · Orion Motors brake caliper bracket · 12 h buffer · **$441,995**
2. M-QRO-03 · servo axis · Vanta EV · 35 h buffer · $20,916
3. M-MTY-12 · coolant pump · Kestrel Trucks · 16 h buffer · $16,882 (higher probability than #2, lower exposure)

## GenAI — [`genai_repair_plans.md`](genai_repair_plans.md)
Claude (`databricks-claude-sonnet-5-5`) repair plans for the 3 ACT NOW machines, MLflow-traced and evaluated:
`safety_first` 1.0 · `grounded` 1.0 · `business_aware` 1.0 · `cites_manual_part_number` 1.0

## Genie — [`genie_answers.md`](genie_answers.md)
6 real Conversation API questions answered with generated SQL and result rows. Includes a refusal on the technician-PII question, because that table isn't in the space.

## Lakebase + App — [`publish_lakebase.md`](publish_lakebase.md), [`lakebase_and_app.md`](lakebase_and_app.md)
Lakebase `meridian-db`: machine_risk 60 · repair_plans 3 · telemetry_recent ~10.1K · technicians 12 · work_orders (app write-back). App `meridian-uptime-planner` RUNNING.
