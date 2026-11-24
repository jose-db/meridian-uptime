# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Uptime — 01 · Generate raw plant data (synthetic)
# MAGIC
# MAGIC **Customer (fictional):** Meridian Precision — automotive Tier-1 supplier, 3 plants
# MAGIC (Saltillo, Monterrey, Querétaro), 60 CNC machining centers that ship brackets, knuckles
# MAGIC and housings **just-in-time** to OEM assembly lines.
# MAGIC
# MAGIC This notebook lands *raw* files into a Unity Catalog Volume exactly as the plant
# MAGIC systems would drop them:
# MAGIC
# MAGIC | Source system | Volume folder | Format |
# MAGIC |---|---|---|
# MAGIC | Historian / IIoT gateway (hourly sensor rollups) | `raw/telemetry/` | JSON lines, one file per day |
# MAGIC | CMMS (work orders) | `raw/work_orders/` | CSV |
# MAGIC | MES (production orders, next 7 days) | `raw/production_orders/` | CSV |
# MAGIC | Asset registry | `raw/machines/` | CSV |
# MAGIC | HR / shift roster (contains PII) | `raw/technicians/` | CSV |
# MAGIC | Maintenance manuals | `raw/manuals/` | JSON lines |
# MAGIC
# MAGIC Deterministic (seeded). Failure precursors are physically motivated: spindle bearing wear
# MAGIC → rising vibration + temperature; coolant pump wear → falling pressure; servo axis faults →
# MAGIC load spikes. Tool-changer jams have **no precursor** on purpose (an honest model must miss them).

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "meridian")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'Meridian Precision — predictive maintenance (FE Bar build)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw COMMENT 'Landing zone for plant source-system drops'")
RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
print("Landing zone:", RAW)

# COMMAND ----------

import json, os, shutil
import numpy as np
import pandas as pd

rng = np.random.default_rng(20261124)
AS_OF = pd.Timestamp("2026-11-24 06:00:00")          # "now" for the demo story
DAYS = 120
hours = pd.date_range(AS_OF - pd.Timedelta(days=DAYS), AS_OF - pd.Timedelta(hours=1), freq="h")
H = len(hours)

for sub in ["telemetry", "work_orders", "production_orders", "machines", "technicians", "manuals"]:
    p = f"{RAW}/{sub}"
    if os.path.exists(p):
        shutil.rmtree(p)
    os.makedirs(p, exist_ok=True)

# COMMAND ----------

# MAGIC %md ## Asset registry

# COMMAND ----------

PLANTS = {"SAL": ("Saltillo", "North"), "MTY": ("Monterrey", "North"), "QRO": ("Querétaro", "Bajío")}
TYPES = [("5AX-MILL", "5-axis vertical mill VX-850"), ("HMC", "Horizontal machining center H-630"),
         ("LATHE", "CNC turning center T-400")]
machines = []
for plant in PLANTS:
    for i in range(1, 21):
        mtype, model = TYPES[(i - 1) % 3]
        machines.append(dict(
            machine_id=f"M-{plant}-{i:02d}", plant_id=plant, plant_name=PLANTS[plant][0],
            region=PLANTS[plant][1], line=f"{plant}-L{(i - 1) // 5 + 1}", machine_type=mtype, model=model,
            install_year=int(rng.integers(2014, 2024)),
            criticality=("A" if i % 4 == 3 else "B" if i % 2 == 0 else "C"),
        ))
machines_df = pd.DataFrame(machines)
machines_df.loc[machines_df.machine_id == "M-SAL-07", "criticality"] = "A"
machines_df.loc[machines_df.machine_id == "M-MTY-12", "criticality"] = "A"
machines_df.to_csv(f"{RAW}/machines/machines.csv", index=False)
print(len(machines_df), "machines")
display(machines_df.head())

# COMMAND ----------

# MAGIC %md ## Hourly telemetry with seeded failure precursors

# COMMAND ----------

MODES = {  # mode: (probability share, precursor lead hours range, downtime hours range, parts cost range)
    "SPINDLE_BEARING": (0.35, (48, 96), (18, 40), (9000, 16000)),
    "COOLANT_PUMP":    (0.25, (36, 72), (6, 14), (1800, 4200)),
    "SERVO_AXIS":      (0.20, (24, 60), (8, 20), (4000, 9000)),
    "TOOL_CHANGER":    (0.20, (0, 0),   (3, 8),  (600, 2500)),
}
mode_names = list(MODES)
mode_p = np.array([MODES[m][0] for m in mode_names])

# In-flight degradations at AS_OF (the story): failure happens AFTER the data ends
IN_FLIGHT = {
    "M-SAL-07": ("SPINDLE_BEARING", 58, 30),   # ramp started 58h ago, fails ~30h from now
    "M-MTY-12": ("COOLANT_PUMP", 30, 40),
    "M-QRO-03": ("SERVO_AXIS", 40, 26),
}

tele_frames, failures = [], []
for m in machines:
    mid = m["machine_id"]
    base_vib = rng.normal(2.2, 0.25); base_temp = rng.normal(41, 2); base_load = rng.normal(58, 5)
    base_press = rng.normal(68, 3); base_power = rng.normal(22, 3)
    hod = hours.hour.values
    shift = np.where((hod >= 22) | (hod < 6), 0.85, 1.0)          # lighter night shift
    vib = base_vib + rng.normal(0, 0.18, H)
    temp = base_temp + 2.5 * np.sin(2 * np.pi * hod / 24) + rng.normal(0, 0.7, H)
    load = base_load * shift + rng.normal(0, 4, H)
    press = base_press + rng.normal(0, 1.2, H)
    power = base_power * shift + rng.normal(0, 1.0, H)
    status = np.array(["RUNNING"] * H, dtype=object)

    # hard negatives: transient spikes with no failure afterwards
    for _ in range(rng.poisson(3)):
        t = int(rng.integers(0, H - 6)); vib[t:t + int(rng.integers(1, 4))] += rng.uniform(1.5, 3.0)

    # historical failures
    # failure times spread uniformly over the history, >= 300 h apart on the same machine
    starts = []
    last_ok = H - 220 if mid in IN_FLIGHT else H - 60     # keep the story machines' recent history clean
    for cand in sorted(rng.integers(150, last_ok, rng.poisson(2.0))):
        if not starts or cand - starts[-1] >= 300:
            starts.append(int(cand))
    for t_cursor in starts:
        mode = rng.choice(mode_names, p=mode_p)
        lo, hi = MODES[mode][1]; lead = int(rng.integers(lo, hi + 1)) if hi else 0
        d_lo, d_hi = MODES[mode][2]; down = int(rng.integers(d_lo, d_hi + 1))
        tf = t_cursor
        if lead and tf - lead < 0:
            lead = tf
        if lead:
            ramp = np.linspace(0, 1, lead) ** 1.6
            s = slice(tf - lead, tf)
            if mode == "SPINDLE_BEARING":
                vib[s] += ramp * rng.uniform(4.0, 6.0); temp[s] += ramp * rng.uniform(9, 14)
            elif mode == "COOLANT_PUMP":
                press[s] -= ramp * rng.uniform(18, 26); temp[s] += ramp * rng.uniform(4, 7)
            elif mode == "SERVO_AXIS":
                load[s] += ramp * rng.uniform(14, 22) + ramp * rng.normal(0, 6, lead); power[s] += ramp * 4
        e = min(tf + down, H)
        status[tf:e] = "DOWN"; vib[tf:e] = 0; load[tf:e] = 0; power[tf:e] = 0.4; press[tf:e] = 0
        failures.append(dict(machine_id=mid, failure_ts=hours[tf], failure_mode=mode, downtime_hours=down,
                             parts_cost=float(rng.integers(*MODES[mode][3]))))

    if mid in IN_FLIGHT:
        mode, started, _ = IN_FLIGHT[mid]
        total_lead = started + IN_FLIGHT[mid][2]
        ramp = (np.arange(started) / total_lead) ** 1.6 * 1.25
        s = slice(H - started, H)
        if mode == "SPINDLE_BEARING":
            vib[s] += ramp * 5.0; temp[s] += ramp * 12
        elif mode == "COOLANT_PUMP":
            press[s] -= ramp * 22; temp[s] += ramp * 5
        elif mode == "SERVO_AXIS":
            load[s] += ramp * 18 + ramp * rng.normal(0, 5, started); power[s] += ramp * 4

    tele_frames.append(pd.DataFrame(dict(
        machine_id=mid, ts=hours, spindle_vibration_mm_s=vib.round(3), spindle_temp_c=temp.round(2),
        spindle_load_pct=np.clip(load, 0, 100).round(2), coolant_pressure_bar=np.clip(press, 0, None).round(2),
        power_kw=np.clip(power, 0, None).round(2),
        parts_produced=np.where(status == "RUNNING", rng.poisson(14, H), 0), status=status)))

tele = pd.concat(tele_frames, ignore_index=True)
# realistic dirt for the DQ expectations: a few null machine ids + impossible temps
dirty = rng.choice(len(tele), 40, replace=False)
tele.loc[dirty[:20], "machine_id"] = None
tele.loc[dirty[20:], "spindle_temp_c"] = 999.0

tele["ts"] = tele["ts"].dt.strftime("%Y-%m-%dT%H:%M:%S")
for day, g in tele.groupby(tele["ts"].str[:10]):
    g.to_json(f"{RAW}/telemetry/telemetry_{day}.json", orient="records", lines=True)
fail_df = pd.DataFrame(failures)
print(f"{len(tele):,} telemetry rows in {tele['ts'].str[:10].nunique()} daily files; {len(fail_df)} historical failures")
print(fail_df.failure_mode.value_counts().to_string())

# COMMAND ----------

# MAGIC %md ## CMMS work orders (corrective history + preventive)

# COMMAND ----------

TECHS = []
first = ["Luis", "Ana", "Jorge", "Marisol", "Diego", "Paola", "Ricardo", "Fernanda", "Héctor", "Sofía", "Iván", "Carla"]
last = ["Treviño", "Garza", "Ramírez", "Villarreal", "Cantú", "Salinas", "Ortega", "Medina", "Rivas", "Leal", "Ibarra", "Núñez"]
skills_pool = ["SPINDLE_BEARING", "COOLANT_PUMP", "SERVO_AXIS", "TOOL_CHANGER"]
k = 0
for plant in PLANTS:
    for j in range(4):
        sk = sorted(set([skills_pool[(k + j) % 4], skills_pool[(k + j + 1) % 4]]))
        TECHS.append(dict(technician_id=f"T-{plant}-{j + 1}", full_name=f"{first[k]} {last[k]}", plant_id=plant,
                          shift=["A", "A", "B", "C"][j], skills="|".join(sk),
                          phone=f"+52 81 {rng.integers(1000, 9999)} {rng.integers(1000, 9999)}",
                          email=f"{first[k].lower()}.{last[k].lower()}@meridian-precision.example"))
        k += 1
techs_df = pd.DataFrame(TECHS)
techs_df.to_csv(f"{RAW}/technicians/technicians.csv", index=False)

NOTE_TPL = {
    "SPINDLE_BEARING": ["Spindle seized mid-cycle, heavy chatter reported by operator for 2 shifts before. Replaced front bearing set, re-preloaded, ran-in 45 min.",
                        "High vibration alarm ignored as sensor noise. Rear bearing spalled. Replaced cartridge + drawbar check."],
    "COOLANT_PUMP":    ["Low coolant pressure, tool burn on part. Pump impeller worn, replaced pump and filter.",
                        "Coolant through-spindle lost; pressure decayed over ~2 days. Replaced pump seal kit."],
    "SERVO_AXIS":      ["Y-axis following error alarm. Servo motor encoder cable chafed + ballscrew drag. Replaced cable, re-lubed.",
                        "Z-axis overload trips under heavy cut. Servo amplifier replaced."],
    "TOOL_CHANGER":    ["ATC arm jam, pot misaligned. Re-taught tool change position.",
                        "Tool drop during change, gripper spring broken. Replaced gripper."],
}
wos = []
for i, f in enumerate(fail_df.itertuples(), 1):
    plant = f.machine_id.split("-")[1]
    cand = [t for t in TECHS if t["plant_id"] == plant and f.failure_mode in t["skills"]] or [t for t in TECHS if t["plant_id"] == plant]
    wos.append(dict(work_order_id=f"WO-{24000 + i}", machine_id=f.machine_id, wo_type="CORRECTIVE",
                    failure_mode=f.failure_mode, opened_ts=f.failure_ts.strftime("%Y-%m-%dT%H:%M:%S"),
                    closed_ts=(f.failure_ts + pd.Timedelta(hours=f.downtime_hours)).strftime("%Y-%m-%dT%H:%M:%S"),
                    downtime_hours=f.downtime_hours, labor_hours=round(f.downtime_hours * rng.uniform(0.5, 0.9), 1),
                    parts_cost_usd=f.parts_cost, technician_id=cand[int(rng.integers(len(cand)))]["technician_id"],
                    notes=NOTE_TPL[f.failure_mode][int(rng.integers(2))]))
n = len(wos)
for m in machines:                    # preventive maintenance every ~30 days
    for d in range(10, DAYS, 30):
        n += 1
        ts = AS_OF - pd.Timedelta(days=DAYS - d) + pd.Timedelta(hours=int(rng.integers(0, 12)))
        wos.append(dict(work_order_id=f"WO-{24000 + n}", machine_id=m["machine_id"], wo_type="PREVENTIVE",
                        failure_mode=None, opened_ts=ts.strftime("%Y-%m-%dT%H:%M:%S"),
                        closed_ts=(ts + pd.Timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S"),
                        downtime_hours=2, labor_hours=2.0, parts_cost_usd=float(rng.integers(80, 400)),
                        technician_id=[t for t in TECHS if t["plant_id"] == m["plant_id"]][d % 4]["technician_id"],
                        notes="Scheduled PM: lube, way-cover inspection, coolant concentration check."))
wo_df = pd.DataFrame(wos)
wo_df.to_csv(f"{RAW}/work_orders/work_orders.csv", index=False)
print(len(wo_df), "work orders", wo_df.wo_type.value_counts().to_dict())

# COMMAND ----------

# MAGIC %md ## MES production orders (next 7 days) — where downtime turns into OEM penalties

# COMMAND ----------

OEMS = [("Orion Motors", 22000), ("Vanta EV", 15000), ("Kestrel Trucks", 9000)]
PARTS = {"5AX-MILL": [("BRK-4471", "Brake caliper bracket"), ("KNK-2210", "Steering knuckle")],
         "HMC": [("HSG-9902", "E-axle gearbox housing"), ("MNT-3310", "Engine mount")],
         "LATHE": [("HUB-1180", "Wheel hub"), ("SFT-5520", "Output shaft")]}
pos, n = [], 0
for m in machines:
    for d in range(0, 7, 2):
        n += 1
        oem, pen = OEMS[(n + d) % 3]
        pn, pdesc = PARTS[m["machine_type"]][n % 2]
        pos.append(dict(production_order_id=f"PO-{88000 + n}", machine_id=m["machine_id"], oem_customer=oem,
                        part_number=pn, part_description=pdesc, quantity=int(rng.integers(300, 1200)),
                        start_ts=(AS_OF + pd.Timedelta(days=d)).strftime("%Y-%m-%dT%H:%M:%S"),
                        due_ts=(AS_OF + pd.Timedelta(days=d + 2)).strftime("%Y-%m-%dT%H:%M:%S"),
                        oem_line_down_penalty_usd_per_hour=pen,
                        finished_goods_buffer_hours=int(rng.integers(8, 48))))
po_df = pd.DataFrame(pos)
hero = po_df.machine_id == "M-SAL-07"
po_df.loc[hero, ["oem_customer", "part_number", "part_description", "oem_line_down_penalty_usd_per_hour", "finished_goods_buffer_hours"]] = \
    ["Orion Motors", "BRK-4471", "Brake caliper bracket", 22000, 12]
po_df.to_csv(f"{RAW}/production_orders/production_orders.csv", index=False)
print(len(po_df), "production orders")
display(po_df[hero])

# COMMAND ----------

# MAGIC %md ## Maintenance manuals (grounding for the GenAI repair plan)

# COMMAND ----------

MANUALS = {
    "SPINDLE_BEARING": ("Spindle bearing replacement", "Parts: angular-contact bearing set (P/N SB-7014-P4, x2), spacer kit, grease GBU-Y. "
        "Steps: 1) LOTO and verify zero energy. 2) Remove drawbar and spindle cartridge. 3) Press out bearings, inspect housing bore (max 0.005 mm runout). "
        "4) Install new set, preload 1,200 N. 5) Run-in: 15 min at 2,000 rpm, 15 at 6,000, 15 at 10,000; vibration must be < 2.8 mm/s. "
        "Estimated time: 10-14 h planned vs 18-40 h after a seizure (collateral damage to drawbar and housing). Early signs: vibration > 4 mm/s and temperature +8 °C over baseline."),
    "COOLANT_PUMP": ("Coolant pump service", "Parts: pump assembly CP-220 or seal kit CP-220-SK, 25 µm filter. Steps: 1) LOTO. 2) Drain sump to pump inlet. "
        "3) Replace seal kit if pressure > 45 bar, else replace pump assembly. 4) Bleed air, verify 65-70 bar through-spindle. Estimated time: 3-5 h planned. "
        "Early signs: pressure drop > 10 bar over 24 h, spindle temperature +4 °C, burn marks on parts."),
    "SERVO_AXIS": ("Servo axis diagnosis", "Parts: encoder cable EC-15, servo amplifier SA-30 (if fault code persists), way lube. "
        "Steps: 1) Read following-error history. 2) Inspect cable chain for chafing. 3) Check ballscrew drag torque (< 2.5 Nm). 4) Replace cable or amplifier. "
        "Estimated time: 4-8 h planned. Early signs: load spikes > 20% over baseline under the same program."),
    "TOOL_CHANGER": ("ATC alignment", "Parts: gripper spring kit GS-4. Steps: re-teach tool change position, replace gripper springs every 18 months. "
        "Estimated time: 2-3 h. No reliable sensor precursor; prevent with PM."),
}
with open(f"{RAW}/manuals/manuals.json", "w") as fh:
    for i, (mode, (title, body)) in enumerate(MANUALS.items(), 1):
        for mtype, model in TYPES:
            fh.write(json.dumps(dict(doc_id=f"MAN-{i}-{mtype}", machine_type=mtype, model=model, failure_mode=mode,
                                     title=f"{title} — {model}", content=body)) + "\n")
print("Manual docs written")

# COMMAND ----------

summary = {sub: sorted(os.listdir(f"{RAW}/{sub}"))[:2] + ([f"... {len(os.listdir(f'{RAW}/{sub}'))} files"]) for sub in os.listdir(RAW)}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps({"telemetry_rows": int(len(tele)), "historical_failures": int(len(fail_df)),
                                  "work_orders": int(len(wo_df)), "production_orders": int(len(po_df))}))
