"""Meridian Uptime — data layer.

Operational reads/writes  -> Lakebase Postgres (psycopg, short-lived OAuth DB credential)
Natural-language analytics -> Genie Agent (Conversation API) over the governed UC tables
"""
import os
import time
import uuid
from contextlib import contextmanager

import pandas as pd
from databricks.sdk import WorkspaceClient

LAKEBASE_INSTANCE = os.environ.get("MU_LAKEBASE_INSTANCE", "meridian-db")
LAKEBASE_DB = os.environ.get("MU_LAKEBASE_DB", "meridian")
GENIE_SPACE_ID = os.environ.get("MU_GENIE_SPACE_ID", "")

_w = None
_cred = {"token": None, "exp": 0.0, "host": None, "user": None}


def w() -> WorkspaceClient:
    global _w
    if _w is None:
        _w = WorkspaceClient()  # app service principal when deployed
    return _w


def _credential():
    if _cred["token"] is None or time.time() > _cred["exp"]:
        inst = w().api_client.do("GET", f"/api/2.0/database/instances/{LAKEBASE_INSTANCE}")
        cred = w().api_client.do("POST", "/api/2.0/database/credentials",
                                 body={"request_id": str(uuid.uuid4()), "instance_names": [LAKEBASE_INSTANCE]})
        _cred.update(token=cred["token"], exp=time.time() + 45 * 60, host=inst["read_write_dns"],
                     user=os.environ.get("DATABRICKS_CLIENT_ID") or w().current_user.me().user_name)
    return _cred


@contextmanager
def pg():
    import psycopg
    c = _credential()
    conn = psycopg.connect(host=c["host"], dbname=LAKEBASE_DB, user=c["user"], password=c["token"],
                           sslmode="require", port=5432, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


def df(sql: str, params=None) -> pd.DataFrame:
    with pg() as conn, conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
        out = pd.DataFrame(rows, columns=cols)
        out.attrs["ms"] = round((time.perf_counter() - t0) * 1000, 1)
        return out


# ---------------------------------------------------------------- reads
def machine_risk(plant=None) -> pd.DataFrame:
    sql = "SELECT * FROM machine_risk"
    if plant and plant != "All plants":
        return df(sql + " WHERE plant_name = %s ORDER BY priority_rank", (plant,))
    return df(sql + " ORDER BY priority_rank")


def telemetry(machine_id: str) -> pd.DataFrame:
    return df("SELECT * FROM telemetry_recent WHERE machine_id = %s ORDER BY ts", (machine_id,))


def repair_plan(machine_id: str):
    d = df("SELECT repair_plan_md, llm_endpoint, generated_at FROM repair_plans WHERE machine_id = %s", (machine_id,))
    return d.iloc[0].to_dict() if len(d) else None


def technicians(plant_id: str) -> pd.DataFrame:
    return df("SELECT technician_id, full_name, shift, skills FROM technicians WHERE plant_id = %s ORDER BY technician_id", (plant_id,))


def work_orders() -> pd.DataFrame:
    return df("""SELECT w.work_order_id, w.machine_id, w.failure_mode, w.technician_id, t.full_name AS technician,
                        w.scheduled_start, w.est_hours, w.exposure_usd, w.status, w.planner_note, w.created_by, w.created_at
                 FROM work_orders w LEFT JOIN technicians t USING (technician_id) ORDER BY w.created_at DESC""")


# ---------------------------------------------------------------- write-back
def create_work_order(machine_id, failure_mode, technician_id, scheduled_start, est_hours, exposure_usd, note, user):
    with pg() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO work_orders (machine_id, failure_mode, technician_id, scheduled_start, est_hours,
                                                exposure_usd, planner_note, created_by)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING work_order_id""",
                    (machine_id, failure_mode, technician_id, scheduled_start, float(est_hours), float(exposure_usd), note, user))
        return cur.fetchone()[0]


def update_status(work_order_id, status):
    with pg() as conn, conn.cursor() as cur:
        cur.execute("UPDATE work_orders SET status = %s, updated_at = now() WHERE work_order_id = %s", (status, work_order_id))


# ---------------------------------------------------------------- Genie
def ask_genie(question: str, conversation_id: str | None = None) -> dict:
    g = w().genie
    msg = (g.create_message_and_wait(GENIE_SPACE_ID, conversation_id, question) if conversation_id
           else g.start_conversation_and_wait(GENIE_SPACE_ID, question))
    out = {"conversation_id": msg.conversation_id, "text": None, "sql": None, "table": None}
    for a in msg.attachments or []:
        if a.text and a.text.content:
            out["text"] = a.text.content
        if a.query:
            out["sql"] = a.query.query
            out["text"] = out["text"] or a.query.description
            res = g.get_message_attachment_query_result(GENIE_SPACE_ID, msg.conversation_id, msg.id, a.attachment_id)
            sr = res.statement_response
            if sr and sr.manifest and sr.result:
                out["table"] = pd.DataFrame(sr.result.data_array or [], columns=[c.name for c in sr.manifest.schema.columns])
    return out
