"""Meridian Uptime — Maintenance Planner Console (Databricks App, Streamlit).

Flow: ranked action list (Lakebase) -> machine drill-down (sensor drift + OEM exposure +
GenAI repair plan) -> schedule work order (Lakebase write-back) -> ask Genie anything.
"""
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

import db

st.set_page_config(page_title="Meridian Uptime", page_icon="⚙️", layout="wide")
BAND_COLOR = {"ACT NOW": "#D92D20", "WATCH": "#F79009", "LOW": "#12B76A"}
user = st.context.headers.get("X-Forwarded-Email", "planner@meridian-precision.example")

st.markdown("## ⚙️ Meridian Uptime — Maintenance Planner Console")
st.caption("Predict → prioritize by OEM exposure → dispatch. Scores from UC model `machine_failure_48h@champion`; "
           "served from Lakebase; questions answered by a Genie Agent over Unity Catalog.")

plant = st.sidebar.selectbox("Plant", ["All plants", "Saltillo", "Monterrey", "Querétaro"])
st.sidebar.markdown(f"Signed in as **{user}**")

try:
    risk = db.machine_risk(plant)
except Exception as e:  # surface config problems clearly in the demo
    st.error(f"Lakebase read failed: {e}")
    st.stop()

act = risk[risk.risk_band == "ACT NOW"]
watch = risk[risk.risk_band == "WATCH"]
wos = db.work_orders()
open_wo = wos[wos.status.isin(["SCHEDULED", "IN_PROGRESS"])] if len(wos) else wos
covered = set(open_wo.machine_id) if len(open_wo) else set()
uncovered_exposure = risk[~risk.machine_id.isin(covered) & risk.risk_band.isin(["ACT NOW", "WATCH"])].exposure_usd.sum()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Machines — ACT NOW", len(act), help="P(failure in 48 h) ≥ 60 %")
k2.metric("Machines — WATCH", len(watch), help="30-60 %")
k3.metric("48 h $ exposure not yet covered", f"${uncovered_exposure:,.0f}",
          help="Risk-weighted cost of unplanned failure (lost contribution + parts + OEM line-down penalties) on machines without a scheduled work order")
k4.metric("Open planned work orders", len(open_wo))
st.caption(f"Lakebase read: {risk.attrs.get('ms', '?')} ms · as of {pd.to_datetime(risk.as_of_ts).max():%Y-%m-%d %H:%M}")

tab_list, tab_machine, tab_wo, tab_genie = st.tabs(["📋 Action list", "🔍 Machine drill-down", "🛠 Work orders", "💬 Ask Genie"])

# ------------------------------------------------------------------ action list
with tab_list:
    view = risk[["priority_rank", "machine_id", "plant_name", "line", "criticality", "failure_prob_48h", "risk_band",
                 "likely_failure_mode", "reason", "oem_customer", "finished_goods_buffer_hours", "exposure_usd"]].copy()
    view["covered"] = view.machine_id.isin(covered).map({True: "✅ scheduled", False: ""})
    st.dataframe(view, hide_index=True, use_container_width=True, height=520, column_config={
        "priority_rank": st.column_config.NumberColumn("#", width="small"),
        "failure_prob_48h": st.column_config.ProgressColumn("P(fail 48h)", format="%.0f%%", min_value=0, max_value=1),
        "exposure_usd": st.column_config.NumberColumn("Exposure $", format="$%d"),
        "finished_goods_buffer_hours": st.column_config.NumberColumn("Buffer h", format="%d"),
    })

# ------------------------------------------------------------------ drill-down
with tab_machine:
    default = risk.machine_id.iloc[0] if len(risk) else None
    mid = st.selectbox("Machine", risk.machine_id.tolist(), index=0)
    m = risk[risk.machine_id == mid].iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("P(failure in 48 h)", f"{m.failure_prob_48h:.0%}", m.risk_band)
    c2.metric("Likely failure mode", m.likely_failure_mode.replace("_", " ").title())
    c3.metric("Next OEM order", m.oem_customer, f"{m.part_description}", delta_color="off")
    c4.metric("Risk-weighted exposure", f"${m.exposure_usd:,.0f}",
              f"${m.cost_if_fails_usd:,.0f} if it fails; buffer {m.finished_goods_buffer_hours:.0f} h", delta_color="off")
    st.info(f"**Why:** {m.reason}. **Recommended:** {m.recommended_action}.")

    t = db.telemetry(mid)
    fig = make_subplots(rows=2, cols=2, shared_xaxes=True, subplot_titles=(
        "Spindle vibration (mm/s)", "Spindle temperature (°C)", "Coolant pressure (bar)", "Spindle load (%)"))
    for i, (col, color) in enumerate([("spindle_vibration_mm_s", "#D92D20"), ("spindle_temp_c", "#F79009"),
                                       ("coolant_pressure_bar", "#2E90FA"), ("spindle_load_pct", "#7A5AF8")]):
        fig.add_trace(go.Scatter(x=t.ts, y=t[col], mode="lines", line=dict(color=color, width=1.6), showlegend=False),
                      row=i // 2 + 1, col=i % 2 + 1)
    fig.update_layout(height=420, margin=dict(l=10, r=10, t=40, b=10))
    st.plotly_chart(fig, use_container_width=True)

    left, right = st.columns([3, 2])
    with left:
        st.markdown("#### 🤖 AI repair plan")
        plan = db.repair_plan(mid)
        if plan:
            st.markdown(plan["repair_plan_md"])
            st.caption(f"Generated by `{plan['llm_endpoint']}` · grounded on OEM manual + this machine's CMMS history · "
                       f"traced & evaluated with MLflow · {plan['generated_at']}")
        else:
            st.write("Low risk — no plan generated. Follow the normal PM cadence.")
    with right:
        st.markdown("#### 📅 Schedule planned repair")
        techs = db.technicians(m.plant_id)
        techs["label"] = techs.full_name + " · shift " + techs.shift + " · " + techs.skills.str.replace("|", ", ")
        skilled = techs[techs.skills.str.contains(m.likely_failure_mode)]
        options = (skilled if len(skilled) else techs)
        with st.form(f"wo_{mid}"):
            tech = st.selectbox("Technician (skill-matched)", options.label.tolist())
            start = st.date_input("Start date", datetime.now().date() + timedelta(days=1))
            hour = st.slider("Start hour", 0, 23, 6)
            est = st.number_input("Estimated hours", 1.0, 48.0, float(round(m.exp_downtime_h * 0.45, 1)))
            note = st.text_area("Planner note", f"Planned {m.likely_failure_mode.lower().replace('_', ' ')} repair before "
                                                 f"{m.oem_customer} {m.part_number} run; build {int(est)} h buffer on sister machine.")
            if st.form_submit_button("Create work order", type="primary"):
                tid = options[options.label == tech].technician_id.iloc[0]
                wo_id = db.create_work_order(mid, m.likely_failure_mode, tid, datetime.combine(start, datetime.min.time()) + timedelta(hours=hour),
                                             est, m.exposure_usd, note, user)
                st.success(f"Work order **{wo_id}** written to Lakebase — ${m.exposure_usd:,.0f} exposure now covered.")
                st.balloons()

# ------------------------------------------------------------------ work orders
with tab_wo:
    wos = db.work_orders()
    st.dataframe(wos, hide_index=True, use_container_width=True)
    if len(wos):
        c1, c2, c3 = st.columns([2, 2, 1])
        sel = c1.selectbox("Work order", wos.work_order_id.tolist())
        new = c2.selectbox("New status", ["IN_PROGRESS", "DONE", "CANCELLED"])
        if c3.button("Update"):
            db.update_status(sel, new)
            st.rerun()

# ------------------------------------------------------------------ Genie
with tab_genie:
    st.markdown("Ask about downtime, failures, costs, OEM exposure or model performance — answered by the "
                "**Meridian Reliability Genie Agent** over governed Unity Catalog tables.")
    examples = ["Which plant lost the most hours to unplanned downtime in the last 90 days?",
                "What is the total cost of spindle bearing failures by plant?",
                "Which machines are ACT NOW and which OEM customers do they feed?",
                "How much net value did the model deliver in the backtest?"]
    q = st.selectbox("Try an example", [""] + examples)
    q = st.text_input("Your question", q)
    if st.button("Ask Genie", type="primary") and q:
        with st.spinner("Genie is thinking…"):
            try:
                ans = db.ask_genie(q, st.session_state.get("genie_conv"))
                st.session_state["genie_conv"] = ans["conversation_id"]
                if ans["text"]:
                    st.markdown(ans["text"])
                if ans["table"] is not None:
                    st.dataframe(ans["table"], hide_index=True, use_container_width=True)
                if ans["sql"]:
                    with st.expander("SQL generated by Genie"):
                        st.code(ans["sql"], language="sql")
            except Exception as e:
                st.error(f"Genie call failed: {e}")
