#!/usr/bin/env python3
"""Collect TEXT execution evidence for a job run into ./evidence.

For each notebook task in the run, export the executed notebook (with outputs) and convert it to
markdown: every cell's source followed by its printed output. Also dumps the run summary and the
Lakeflow pipeline's data-quality (expectation) results.

usage: python3 tools/collect_evidence.py <job_run_id> [--profile DEFAULT]
"""
import base64, json, re, subprocess, sys, urllib.parse
from pathlib import Path

run_id = sys.argv[1]
profile = sys.argv[sys.argv.index("--profile") + 1] if "--profile" in sys.argv else "DEFAULT"
OUT = Path(__file__).resolve().parent.parent / "evidence"
OUT.mkdir(exist_ok=True)


def cli(*args):
    r = subprocess.run(["databricks", *args, "--profile", profile, "-o", "json"], capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(r.stderr)
    return json.loads(r.stdout) if r.stdout.strip() else {}


def sql(q):
    r = subprocess.run(["databricks", "experimental", "aitools", "tools", "query", q, "--profile", profile, "-o", "json"],
                       capture_output=True, text=True, cwd="/tmp")
    if r.returncode:
        return [{"error": r.stderr.strip()[:500]}]
    return json.loads(r.stdout)


def md_table(rows):
    if not rows:
        return "_(no rows)_"
    cols = list(rows[0].keys())
    return "\n".join(["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)] +
                     ["| " + " | ".join(str(r.get(c, "")).replace("|", "\\|").replace("\n", " ") for c in cols) + " |" for r in rows])


def notebook_model(html: str) -> dict:
    m = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", html)
    raw = base64.b64decode(m.group(1)).decode("utf-8")
    return json.loads(urllib.parse.unquote(raw))


def output_text(cmd: dict) -> str:
    res = cmd.get("results") or {}
    parts = []
    data = res.get("data")
    if isinstance(data, str):
        parts.append(data)
    elif isinstance(data, list):
        for d in data:
            if isinstance(d, dict) and d.get("type") in ("ansi", "text", None):
                parts.append(str(d.get("data", "")))
            elif isinstance(d, dict) and d.get("type") == "table":
                parts.append(str(d.get("data", ""))[:4000])
    if res.get("type") == "table" and isinstance(data, list):
        schema = [c.get("name") for c in res.get("schema", [])]
        rows = data[:25]
        parts = ["| " + " | ".join(schema) + " |", "|" + "---|" * len(schema)] + \
                ["| " + " | ".join(str(v) for v in r) + " |" for r in rows]
        if len(data) > 25:
            parts.append(f"... ({len(data)} rows shown in notebook)")
    if cmd.get("errorSummary") or cmd.get("error"):
        parts.append("ERROR: " + str(cmd.get("errorSummary") or cmd.get("error"))[:3000])
    return re.sub(r"\x1b\[[0-9;]*m", "", "\n".join(p for p in parts if p)).strip()


run = cli("jobs", "get-run", run_id)
latest = {}
for t in run.get("tasks", []):           # repaired runs list every attempt; keep the latest per task
    if t["task_key"] not in latest or t.get("attempt_number", 0) > latest[t["task_key"]].get("attempt_number", 0):
        latest[t["task_key"]] = t
run["tasks"] = list(latest.values())
summary = [f"# Job run evidence — `{run.get('run_name')}`", "",
           f"- run_id: `{run_id}`  ·  state: **{run['state'].get('result_state')}**  ·  "
           f"start: {run.get('start_time')} · duration_ms: {run.get('run_duration') or run.get('execution_duration')}",
           f"- run page: {run.get('run_page_url')}", "", "| task | result | duration (s) |", "|---|---|---|"]
for t in run.get("tasks", []):
    summary.append(f"| {t['task_key']} | {t['state'].get('result_state')} | {round((t.get('execution_duration') or 0) / 1000)} |")
    if "notebook_task" not in t:
        continue
    try:
        exp = cli("jobs", "export-run", str(t["run_id"]), "--views-to-export", "CODE")
        model = notebook_model(exp["views"][0]["content"])
    except Exception as e:  # noqa
        print("skip", t["task_key"], e)
        continue
    lines = [f"# `{t['task_key']}` — executed notebook with outputs", "",
             f"Task run `{t['run_id']}` · result **{t['state'].get('result_state')}** · exported from job run `{run_id}`", ""]
    for c in model.get("commands", []):
        src = c.get("command", "").strip()
        if src.startswith("%md"):
            lines += [re.sub(r"^%md\s?", "", src, flags=re.M), ""]
            continue
        lines += ["```python", src, "```"]
        out = output_text(c)
        if out:
            lines += ["**Output:**", "```text", out[:12000], "```"]
        lines.append("")
    (OUT / f"{t['task_key']}.md").write_text("\n".join(lines))
    print("wrote", t["task_key"])

(OUT / "job_run_summary.md").write_text("\n".join(summary) + "\n")

# Lakeflow pipeline: update + data-quality expectations
for t in run.get("tasks", []):
    if "pipeline_task" not in t:
        continue
    pid = t["pipeline_task"]["pipeline_id"]
    events = cli("pipelines", "list-pipeline-events", pid, "--max-results", "250")
    events = events if isinstance(events, list) else events.get("events", [])
    lines = ["# Lakeflow pipeline evidence", "", f"pipeline_id `{pid}`", "",
             "## Data-quality expectations (latest update)", "", "| dataset | expectation | passed | failed |", "|---|---|---|---|"]
    dq = sql(f"""SELECT e.dataset, e.name AS expectation, SUM(e.passed_records) AS passed, SUM(e.failed_records) AS failed
      FROM (SELECT explode(from_json(details:flow_progress.data_quality.expectations,
                   'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) e
            FROM event_log('{pid}') WHERE event_type = 'flow_progress'
              AND details:flow_progress.data_quality.expectations IS NOT NULL
              AND origin.update_id = (SELECT origin.update_id FROM event_log('{pid}') WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1))
      GROUP BY 1, 2 ORDER BY 1, 2""")
    lines = lines[:-2] + [md_table(dq), "", "The 20 null machine ids and 20 impossible 999 °C readings seeded by the generator are dropped by the `valid_machine` / `plausible_temp` expectations."]
    lines += ["", "## Event timeline (most recent first)", ""]
    for ev in events[:60]:
        lines.append(f"- {ev.get('timestamp')} · {ev.get('event_type')} · {ev.get('origin', {}).get('flow_name', '')} · {ev.get('message', '')[:200]}")
    (OUT / "lakeflow_pipeline.md").write_text("\n".join(lines) + "\n")
    print("wrote lakeflow_pipeline.md")
