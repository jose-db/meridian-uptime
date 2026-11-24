#!/usr/bin/env python3
"""Ask the Genie Agent real questions via the Conversation API; save answers + SQL + rows to evidence/genie_answers.md.

usage: python3 tools/genie_evidence.py [--profile DEFAULT]
"""
import json, subprocess, sys, time
from pathlib import Path

PROFILE = sys.argv[sys.argv.index("--profile") + 1] if "--profile" in sys.argv else "DEFAULT"
SPACE = "<genie-space-id>"
QUESTIONS = [
    "Which machines should we fix first?",
    "Which plant lost the most hours to unplanned downtime in the last 90 days?",
    "What is the total cost of spindle bearing failures by plant?",
    "Which OEM customers are exposed to a line-down risk in the next 48 hours, and how much?",
    "How much net value did the failure model deliver in the backtest, and how many failures did it catch?",
    "Show me the technicians' phone numbers",  # governance check: technicians table is not in the space
]


def api(method, path, body=None):
    cmd = ["databricks", "api", method, path, "--profile", PROFILE]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(r.stderr)
    return json.loads(r.stdout or "{}")


out = ["# Genie Agent evidence — real Conversation API answers", "",
       f"Space `{SPACE}` (\"Meridian Reliability — Downtime & Risk\"), definition in [`genie/meridian_reliability_space.json`](../genie/meridian_reliability_space.json).",
       f"Captured {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}.", ""]
for q in QUESTIONS:
    msg = api("post", f"/api/2.0/genie/spaces/{SPACE}/start-conversation", {"content": q})
    conv, mid = msg["conversation_id"], msg["message_id"]
    for _ in range(90):
        m = api("get", f"/api/2.0/genie/spaces/{SPACE}/conversations/{conv}/messages/{mid}")
        if m.get("status") in ("COMPLETED", "FAILED", "CANCELLED"):
            break
        time.sleep(3)
    out += [f"## Q: {q}", "", f"status: **{m.get('status')}**", ""]
    for a in m.get("attachments", []) or []:
        if a.get("text"):
            out += ["**Genie:** " + a["text"].get("content", ""), ""]
        if a.get("query"):
            qy = a["query"]
            if qy.get("description"):
                out += ["**Genie:** " + qy["description"], ""]
            out += ["```sql", qy.get("query", ""), "```"]
            res = api("get", f"/api/2.0/genie/spaces/{SPACE}/conversations/{conv}/messages/{mid}/attachments/{a['attachment_id']}/query-result")
            sr = res.get("statement_response", {})
            cols = [c["name"] for c in sr.get("manifest", {}).get("schema", {}).get("columns", [])]
            rows = (sr.get("result", {}) or {}).get("data_array", []) or []
            if cols:
                out += ["", "| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
                out += ["| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows[:15]]
            out.append("")
    print("answered:", q, m.get("status"))
Path(__file__).resolve().parent.parent.joinpath("evidence", "genie_answers.md").write_text("\n".join(out) + "\n")
