# Meridian Uptime — predict CNC failures before they stop your OEM's line

**Industry:** Automotive Tier-1 supplier / discrete manufacturing · **Use case:** predictive maintenance
prioritized by OEM line-down exposure · **Data:** 100 % synthetic (seeded generator in this repo)

> Environment-specific identifiers (workspace URL, workspace/warehouse/Genie/service-principal IDs, Lakebase host, user names) are replaced with placeholders such as `<your-workspace>`; the catalog is shown as `main`. Set your own values in `databricks.yml` and the notebook widgets to redeploy. Run outputs in `evidence/` are otherwise unedited.

> Meridian Precision (fictional) runs 60 CNC machining centers in 3 plants and ships JIT to OEMs whose
> contracts charge **$9K–$22K per hour** of line stoppage once the finished-goods buffer runs out.
> Unplanned failures are visible in the sensors days in advance, but nobody connects the sensor drift
> to the OEM schedule. Meridian Uptime does: every shift the planner gets the machines most likely to
> fail in the next 48 h **ranked by dollars at risk**, a grounded AI repair plan, and one-click dispatch.

**Results (from the committed run evidence):** see [`evidence/RESULTS.md`](evidence/RESULTS.md).

## End-to-end data journey (one Asset Bundle, one job)

| # | Stage | What's built | Where |
|---|---|---|---|
| 1 | **Lakeflow** | Spark Declarative Pipeline: Auto Loader from a UC Volume (historian JSON, CMMS/MES/registry/HR CSV, manuals) → bronze → silver → gold, with data-quality expectations that drop null ids / impossible readings | [`src/pipeline/meridian_ingest.sql`](src/pipeline/meridian_ingest.sql) |
| 2 | **Unity Catalog** | Governed schema + Volume; `mask_pii` column masks (technician phone/e-mail) and `plant_filter` row filter attached in the pipeline DDL; table comments; model registered in UC; lineage raw → gold → model → app | [`00_setup_governance.py`](src/notebooks/00_setup_governance.py) |
| 3 | **Lakebase** | Managed Postgres `meridian-db`: serving tables for the app (ms reads) + `work_orders` transactional write-back; least-privilege grants for the app SP (no PII); registered in UC as catalog `meridian_lakebase` so app write-backs are queryable/joinable from the lakehouse | [`05_load_lakebase.py`](src/notebooks/05_load_lakebase.py) |
| 4a | **ML** | 48-h failure classifier (gradient-boosted trees) on drift-vs-own-baseline features; time-based split; threshold tuned on validation for net $ value; event-level recall + lead time; MLflow tracking; UC model `machine_failure_48h@champion` | [`02_train_failure_model.py`](src/notebooks/02_train_failure_model.py) |
| 4b | **Risk → $** | Batch scoring + explainability (drifting signal in σ) + **exposure = P(fail) × (lost contribution + parts + OEM penalty beyond buffer)** | [`03_score_and_prioritize.py`](src/notebooks/03_score_and_prioritize.py) |
| 4c | **GenAI** | Claude (Foundation Model API) writes repair plans grounded on the OEM manual + the machine's CMMS history; MLflow Tracing + MLflow GenAI evaluation (3 LLM judges + deterministic part-number check) | [`04_genai_repair_plans.py`](src/notebooks/04_genai_repair_plans.py) |
| 5 | **Genie Agent** | "Meridian Reliability" over 7 governed gold/silver tables with instructions, joins, example SQL and benchmarks | [`genie/`](genie/) |
| 6 | **Databricks App** | Streamlit planner console: action list → drill-down (sensor charts, reason, OEM exposure, AI plan) → create work order in Lakebase → Ask Genie | [`app/`](app/) |
| — | **Orchestration** | `databricks.yml`: pipeline + Lakebase instance + 7-task job + app | [`databricks.yml`](databricks.yml) |

Design rationale and trade-offs: [`docs/DESIGN.md`](docs/DESIGN.md) · Demo script for business & technical audiences: [`docs/DEMO_SCRIPT.md`](docs/DEMO_SCRIPT.md) · Business deck: [`deck/`](deck/)

## Execution evidence (text, committed)

| File | What it proves |
|---|---|
| [`evidence/job_run_summary.md`](evidence/job_run_summary.md) | The 7-task end-to-end job ran green (task results + durations) |
| [`evidence/lakeflow_pipeline.md`](evidence/lakeflow_pipeline.md) | Pipeline update + data-quality expectation pass/fail counts |
| `evidence/<task>.md` | Each executed notebook, cell by cell, **with its printed output** |
| [`evidence/uc_governance.md`](evidence/uc_governance.md) | Masks / row filter attached, grants, model version + alias, lineage |
| [`evidence/genie_answers.md`](evidence/genie_answers.md) | Real Genie Conversation API answers + generated SQL |
| [`evidence/lakebase_and_app.md`](evidence/lakebase_and_app.md) | Lakebase row counts, a work order written by the app, app status |
| [`evidence/RESULTS.md`](evidence/RESULTS.md) | Headline numbers pulled from the above |

## Reproduce

```bash
databricks bundle deploy --profile <profile>
databricks bundle run meridian_end_to_end --profile <profile>     # ~25 min, serverless
python3 genie/create_genie_space.py --profile <profile>
databricks bundle run meridian_planner --profile <profile>        # deploys the app source
python3 tools/collect_evidence.py <job_run_id> --profile <profile> # regenerates evidence/
python3 tools/uc_evidence.py; python3 tools/genie_evidence.py; python3 tools/lakebase_app_evidence.py
```
Defaults (catalog, warehouse, LLM endpoint) are in `databricks.yml` / notebook widgets.
