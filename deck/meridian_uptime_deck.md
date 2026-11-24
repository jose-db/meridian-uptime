# Meridian Uptime
## Stop CNC failures before they stop your OEM's line

**Meridian Precision · Operations & Reliability leadership review**
Prepared by Databricks Field Engineering · November 2026

*All data in this proposal is synthetic and modelled on a 60-machine, 3-plant Tier-1 operation.*

---

## 1 · The outcome, up front

**Halve unplanned CNC downtime and protect OEM deliveries, using data you already collect.**

| KPI (owner) | Today (modelled baseline) | With Meridian Uptime (back-tested) |
|---|---|---|
| Unplanned downtime, 60 machines (VP Ops) | ~5,400 h / year | **~2,700 h / year avoided (−50 %)** |
| Predictable failures caught in advance (Reliability) | 0 %, found when the machine stops | **100 % (30 / 30), median 31 h warning** |
| False alerts (planner workload) | n/a | **0.1 per machine per month** |
| Net operating value, contribution margin only | — | **≈ $3.7 M / year** |
| OEM line-down penalty exposure (VP Ops) | Unknown until it happens | **Visible 48 h ahead: $480 K at risk today, on 3 machines** |

> The $3.7 M counts **only lost contribution margin** avoided, minus the cost of inspecting false alerts.
> OEM chargebacks avoided come **on top**. A single seized spindle on the Orion Motors line is a ~$440 K event.

---

## 2 · Why this matters now

- JIT contracts charge **$9 K–$22 K per hour** of OEM line stoppage once your finished-goods buffer runs out.
- A **seized spindle bearing takes 18–40 h** to fix. A **planned swap takes 10–14 h**. The difference is whether you pay a penalty.
- The warning signs are already in your data. Vibration and temperature drift **2–4 days ahead**, but they sit in three systems nobody joins in time:
  - **Historian:** the sensors
  - **CMMS:** repair history and parts
  - **MES:** which OEM order is on which machine, and how much buffer is left
- Maintenance today reacts to **alarms**, not **business exposure**. A noisy Class-C lathe with 40 h of buffer gets attention before the Class-A mill feeding Orion's brake line with 12 h.

---

## 3 · What your teams get on Monday morning

**One ranked list per shift: which machines will likely fail in the next 48 h, ordered by dollars at risk.**

| # | Machine | Risk (48 h) | Likely failure | Feeds | Buffer | $ at risk |
|---|---|---|---|---|---|---|
| 1 | M-SAL-07 · Saltillo L2 | 100 % ACT NOW | Spindle bearing | Orion Motors, brake caliper bracket | 12 h | **$442 K** |
| 2 | M-QRO-03 · Querétaro L1 | 79 % ACT NOW | Servo axis | Vanta EV, output shaft | 35 h | $21 K |
| 3 | M-MTY-12 · Monterrey L3 | 98 % ACT NOW | Coolant pump | Kestrel Trucks, output shaft | 16 h | $17 K |

Each row comes with:
- **Why:** e.g. "spindle vibration +2.2 mm/s vs its own 14-day baseline (12σ)"
- **An AI-drafted repair plan:** LOTO first, the exact bearing kit P/N, preload and run-in spec from your manual, and this machine's last repair note
- **One-click dispatch** to a skill-matched technician on the right shift

---

## 4 · For the VP Operations (executive sponsor)

| You care about | What changes |
|---|---|
| **OEM delivery performance & scorecard** | Failures on OEM-critical machines are seen 1–2 shifts ahead. Planners can move the order to a sister machine or build buffer. |
| **Penalties / chargebacks** | Exposure is quantified per machine, per order, so you protect the expensive hours first. |
| **OEE (availability)** | Back-tested: half of unplanned downtime hours become short planned repairs. |
| **EBITDA** | ≈ $3.7 M/yr in contribution margin on 60 machines, before penalty avoidance. |
| **Risk** | Starts on one plant, with data you already have. No new sensors, no rip-and-replace of CMMS or MES. |

---

## 5 · For the Head of Reliability & Maintenance (domain owner)

| You care about | What changes |
|---|---|
| **Emergency vs planned work mix** | Caught failures become planned 10–14 h jobs, not 18–40 h emergencies with collateral damage. |
| **Planner time** | One list replaces stitching together historian trends, CMMS history and the MES schedule. |
| **Technician productivity & safety** | Plans start with LOTO, cite the manual's part numbers and specs, and are matched to technician skills. |
| **Parts expediting** | The plan says what to stage (e.g. bearing set SB-7014-P4 ×2) one to two shifts before the job. |
| **Trust** | Every score shows the drifting signal. Every plan passes automated checks for safety and grounding. Planners stay in control. |

---

## 6 · Proof: back-test on held-out history

Trained on late Jul – 7 Oct; tested on the **last 46 days the model never saw**.

| Metric | Result |
|---|---|
| Failures in the test window | 43 |
| **Caught 2–48 h ahead (predictable modes)** | **30 / 30 (100 %)** |
| Tool-changer jams (no sensor warning sign) | 0 / 13. **We don't claim these**; preventive maintenance covers them. |
| Median warning time | **31.5 h** |
| False alerts | 9 machine-days in 46 days, fleet-wide (**0.1 / machine / month**) |
| Downtime hours avoided (planned repair ≈ 45 % of unplanned duration) | **336 of 676 h** |
| Net value in the window (contribution $1,450/h − $2,175 per false alert) | **$468 K → ≈ $3.7 M / year** |

**Assumptions to validate with you:** contribution margin per machine-hour, planned-repair ratio, inspection cost per alert, and OEM penalty clauses.

---

## 7 · How it works (one platform, one governed flow)

1. **Ingest (Lakeflow):** historian, CMMS, MES, asset registry, HR roster and manuals land automatically. Bad readings are dropped and counted.
2. **Govern (Unity Catalog):** technician PII is masked, rows are scoped by plant, and lineage runs from raw file → model → app.
3. **Predict (ML):** a 48 h failure model per machine, versioned and approved in Unity Catalog.
4. **Prioritize ($):** risk × (lost margin + parts + OEM penalty hours beyond the buffer).
5. **Plan (Gen AI):** Claude drafts the repair plan from your manual and the machine's history. Every plan is traced and evaluated.
6. **Serve & act (Lakebase + App):** the planner console reads in milliseconds, and work orders are written back transactionally.
7. **Ask anything (Genie):** e.g. "Which plant lost the most hours last quarter?", answered in plain language with the SQL shown.

---

## 8 · Plan: 6 weeks to value on one plant

| Weeks | What | Exit criteria |
|---|---|---|
| 1–2 | Connect the Saltillo historian (6 months of hourly data) plus the CMMS and MES exports; confirm penalty and buffer terms | Data quality report; baseline downtime and penalty hours agreed with Finance |
| 3–4 | Train on Saltillo history and back-test with the Reliability team | ≥ 70 % recall on predictable modes at ≤ 1 false alert per machine per month |
| 5–6 | Planners use the console every shift in parallel with the current process | Planners act on the list daily; first avoided emergency documented |
| Next | Roll out to Monterrey and Querétaro; add OEE performance and quality losses | VP Ops sign-off on the value case |

**What we need from you:** a Reliability lead (4 h/week), historian and CMMS export access, and Finance's confirmation of margin and penalty figures.

---

## 9 · The ask

**Approve a 6-week Saltillo pilot**, sponsored by the VP Operations and owned by the Head of Reliability.

- Success is measured in your KPIs: unplanned downtime hours, OEM penalty hours, and the emergency-work ratio.
- If the pilot hits its exit criteria, we bring the multi-plant business case to the next QBR.
