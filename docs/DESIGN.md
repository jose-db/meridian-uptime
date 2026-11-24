# Meridian Uptime — design notes & trade-offs

## 1. The customer problem (Industry: Automotive / Discrete Manufacturing)

**Meridian Precision** (fictional) is an automotive Tier-1 supplier: 3 plants in northern/central
Mexico, 60 CNC machining centers, shipping safety-critical brackets, knuckles, hubs and e-axle
housings **just-in-time** to OEM assembly lines (Orion Motors, Vanta EV, Kestrel Trucks).

The pain is not "maintenance is expensive". It is very specific:

1. **Unplanned CNC failures cascade into OEM line-down penalties.** JIT contracts carry
   $9K-$22K per hour of OEM line stoppage once the supplier's finished-goods buffer runs out.
   A seized spindle bearing takes 18-40 h to repair; a planned swap takes 10-14 h.
2. **The warning signs exist but sit in three disconnected systems** — the plant historian
   (sensors), the CMMS (work orders + repair history), the MES (which OEM order is on which
   machine, and how much buffer there is). Nobody joins them before the failure, only in the
   post-mortem.
3. **Maintenance prioritizes by alarm, not by business exposure.** A noisy Class-C lathe with
   40 h of buffer gets attention before the Class-A mill feeding Orion's brake line with 12 h.

**Decision we enable:** every shift, the reliability planner sees *which machines will likely
fail in the next 48 h, ranked by $ exposure to OEM penalties*, with a grounded repair plan and a
one-click work order assigned to a skill-matched technician.

Personas: **Executive sponsor** — VP Operations / Plant GM (owns OEM scorecard, OEE, penalties).
**Domain owner** — Head of Reliability & Maintenance (owns planner workflow, technicians, CMMS).

## 2. Architecture (one integrated flow)

```
Plant historian (JSON/day) ┐
CMMS work orders (CSV)     ├─► UC Volume raw/ ─► Lakeflow SDP (Auto Loader) ─► bronze ─► silver ─► gold
MES production orders (CSV)│                       expectations, PII masks, plant row filter (UC)
Asset registry, HR roster  ┘                                                     │
Maintenance manuals (JSON) ┘                                                     ▼
                         02 features + HistGradientBoosting  ──MLflow──► UC model machine_failure_48h@champion
                         03 batch score + $ exposure ranking ──────────► gold_machine_risk
                         04 Claude repair plans (grounded, MLflow traced + judged) ─► gold_repair_plans
                         05 publish ─────────────────────────────────────► Lakebase (Postgres)
                                                                              ▲   │ work_orders write-back
   Genie Agent "Meridian Reliability" over gold/silver UC tables              │   ▼
                         └──────────── Databricks App (Streamlit) "Maintenance Planner Console"
```

Everything above is deployed by **one Databricks Asset Bundle** (`databricks.yml`): pipeline,
Lakebase instance, a 7-task job (`meridian-uptime-end-to-end`) that runs the whole chain, and the app.

## 3. Key design choices and trade-offs

| Decision | Why | Trade-off / what I'd do at production scale |
|---|---|---|
| Hourly rollups, not raw 1 kHz vibration | Failure precursors (bearing wear, pump decay) develop over 1-4 days; hourly is enough and cheap | Add Zerobus / streaming ingest of edge-computed features for sub-hour alerts |
| Predict "fail within 48 h" (binary) | Matches the planner's decision window: enough time to move an OEM order or build buffer | Survival model (time-to-failure) would give better scheduling; harder to explain |
| Features = drift vs the machine's **own** 14-day baseline | Machines differ (age, model); absolute thresholds create false alarms | Needs 2 weeks of history for a new machine — cold start uses fleet baseline |
| **Time-based** train/test split + event-level metrics | Random split leaks future degradation into training and overstates performance | — |
| Rank by **$ exposure**, not probability | The business question is "what do I fix first", which depends on OEM penalties and buffer | Exposure inherits uncertainty from average repair durations per mode |
| Tool-changer jams left un-modeled | No sensor precursor exists; claiming to predict them would be dishonest | Covered by preventive maintenance; reported as a known miss in metrics |
| LLM grounded by key lookup (machine_type × failure_mode) instead of vector search | Manuals are small and structured; exact retrieval beats semantic retrieval here | Switch to Vector Search when the manual corpus grows to thousands of pages |
| LLM quality gate with MLflow GenAI evaluation (judges + deterministic part-number check) | A hallucinated part number wastes a technician shift | Add human feedback from planners in the app as labels |
| Lakebase loaded via idempotent psycopg publish | This workspace's metastore has no managed storage root, so UC synced tables can't initialise | In a standard workspace: synced tables (triggered) from gold → Lakebase |
| Lakebase DB registered as UC catalog `meridian_lakebase` | Planner decisions written in the app are immediately joinable with gold tables (e.g. exposure covered vs uncovered) without an export job | Read-only from the lakehouse side; writes stay transactional in Postgres |
| Masks on technician PII + plant row filter in the pipeline DDL | Governance travels with the table; the app SP never sees PII (Lakebase roster excludes those columns) | Map to real IdP groups (`meridian_plant_sal`, `meridian_hr`) |

## 4. What I'd do next with the customer
1. Replace synthetic data with 6 months of historian + CMMS export from one plant (Saltillo) — 2-week POC.
2. Calibrate the exposure model with the customer's real OEM penalty clauses and buffer policy.
3. Close the loop: technicians' findings in the app become labels for the next model version (champion/challenger in UC).
4. Expand to OEE (performance + quality losses), not only availability.
