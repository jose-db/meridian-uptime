# Meridian Uptime — demo script (FE Bar roleplay prep)

Two audiences, same build. Lead with *their* outcome, show the product only as proof.

---

## A. Business stakeholder — VP Operations (exec sponsor) + Head of Reliability (domain owner) · 10 min

**1. Open with their problem (1 min, no screen)**
> "Last quarter, how many hours did Orion's line stop waiting on your parts — and what did that cost in chargebacks?
> In our model of your plants, one seized spindle on a Class-A mill with 12 h of buffer is a ~$300K event. The
> sensors saw it coming two days earlier; nobody connected that to the OEM schedule."

**2. Action list (2 min)** — App → *Action list* tab
- "This is what your planner sees at 6 a.m.: every machine ranked by **dollars at risk in the next 48 h**, not by alarm count."
- Point at **M-SAL-07**: ACT NOW, spindle bearing, feeding Orion Motors' brake caliper bracket, 12 h buffer.
- Contrast M-MTY-12 (98 % risk, but 16 h buffer on a cheaper order, $17K) with M-QRO-03 (79 % risk, $21K). Within ACT NOW, **dollars set the order, not the alarm**.

**3. Why should I trust it (2 min)** — *Machine drill-down*
- Vibration and spindle temperature drifting for ~2.5 days vs. the machine's own baseline (chart).
- "On the 46 days of history the model never saw, it caught all 30 predictable failures, with a median of 31.5 h warning. It raised 0.1 false alerts per machine per month." (numbers from `evidence/train_failure_model.md`)
- Be honest: "Tool-changer jams have no sensor signature — we don't claim to predict them; PM covers those."

**4. From insight to action (2 min)** — AI repair plan + *Schedule planned repair*
- Plan cites LOTO first, the exact bearing kit P/N, preload and run-in spec from *their* manual and the last repair note on this machine.
- Pick the skill-matched technician, create the work order → it's in Lakebase instantly; exposure KPI drops.

**5. Ask anything (1 min)** — *Ask Genie*: "Which plant lost the most hours to unplanned downtime in the last 90 days?"

**6. Close on value + next step (2 min)**
- Back-test: $468K net in 46 days, about **$3.7M a year** on 60 machines. That counts lost margin only; OEM penalty avoidance is on top (M-SAL-07 alone is a $442K exposure).
- Proposal: 2-week POC on Saltillo with 6 months of historian + CMMS export; success = recall ≥ 70 % on predictable modes at ≤ 1 false alert/machine/month, and a planner using the list daily.

**Likely objections**
| Objection | Answer |
|---|---|
| "We already have vibration alarms in the historian." | Alarms fire on thresholds, per machine, without the OEM schedule. This ranks by business exposure and tells you what to do. |
| "My planners won't trust a black box." | Every score shows the drifting signal and size in σ; the plan cites the manual; planners decide, the app records it. |
| "Our data is messy." | The pipeline drops bad readings with declared expectations and reports how many — see the DQ log. |
| "What does it cost?" | Serverless pipeline + hourly scoring is a few DBUs a day; the hero event alone is ~100× a year of run cost. |

---

## B. Technical stakeholder — Plant IT / Data Platform lead · 10 min

1. **Bundle** (`databricks.yml`): one deployable unit — pipeline, Lakebase instance, 7-task job, app. Show the job DAG + green run.
2. **Lakeflow** (`src/pipeline/meridian_ingest.sql`): Auto Loader from UC Volume, bronze → silver → gold, expectations (`DROP ROW` on null ids / 999 °C), event log shows dropped counts.
3. **Unity Catalog**: `mask_pii` on technician phone/e-mail and `plant_filter` row filter defined *in the table DDL*; lineage raw → gold → model; model `machine_failure_48h@champion` with signature.
4. **ML**: time-based split; why event-level recall and lead time beat ROC-AUC for this decision; features = drift vs own baseline.
5. **GenAI**: Claude via Foundation Model API; grounded by key lookup (why not vector search here); MLflow traces + judges (`safety_first`, `grounded`, `business_aware`) + deterministic part-number scorer.
6. **Lakebase**: why OLTP for the app (ms reads, transactional write-back with CHECK constraints); app SP gets no PII.
7. **App**: Streamlit on Databricks Apps; OAuth as the app service principal; Genie Conversation API.

**Likely objections**
| Objection | Answer |
|---|---|
| "Why not stream sensor data in real time?" | The decision window is 48 h; hourly is enough. For sub-minute, Zerobus Ingest straight to a Delta table — same pipeline downstream. |
| "How do you retrain / avoid drift?" | Same job re-trains; new version registered in UC; promote via alias after back-test gates; planners' outcomes become labels. |
| "Why Lakebase and not just the warehouse?" | Concurrent small reads + transactional writes from the app; the warehouse is for analytics (Genie). |
| "Synced tables?" | Normally yes (gold → Lakebase); this workspace's metastore has no storage root, so I publish idempotently with psycopg — same schema. |
