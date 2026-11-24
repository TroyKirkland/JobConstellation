"""
Job Constellation — Databricks Jobs & Notebook control centre.

Local mode : reads profiles from ~/.databrickscfg, lets you pick an environment.
App mode   : when running as a Databricks App (DATABRICKS_APP_NAME set) it uses the
             app's own workspace credentials and exposes a single environment.

Run locally:  .venv/bin/uvicorn server:app --port 8765
"""
from __future__ import annotations

import base64
import configparser
import os
import posixpath
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
import json
from datetime import datetime
from fastapi.staticfiles import StaticFiles

HERE = Path(__file__).parent
APP_MODE = bool(os.getenv("DATABRICKS_APP_NAME"))
MAX_NOTEBOOK_SCANS = int(os.getenv("MAX_NOTEBOOK_SCANS", "400"))
RUNS_PER_JOB = int(os.getenv("RUNS_PER_JOB", "10"))

app = FastAPI(title="Job Constellation")
_clients: dict[str, WorkspaceClient] = {}
_cache: dict[tuple, tuple[float, dict]] = {}
CACHE_TTL = 120


# --------------------------------------------------------------------------- #
# Environments
# --------------------------------------------------------------------------- #
def list_profiles() -> list[dict]:
    if APP_MODE:
        return [{"name": "this-workspace", "host": os.getenv("DATABRICKS_HOST", "")}]
    cfg = configparser.ConfigParser()
    cfg.read(Path.home() / ".databrickscfg")
    out = []
    for section in cfg.sections():
        host = cfg[section].get("host", "")
        if not host or section.startswith("__"):
            continue
        out.append({"name": section, "host": host, "fevm": "fevm" in host})
    # FEVM workspaces first
    out.sort(key=lambda p: (not p["fevm"], p["name"]))
    return out


def client(profile: str) -> WorkspaceClient:
    if profile not in _clients:
        _clients[profile] = WorkspaceClient() if APP_MODE else WorkspaceClient(profile=profile)
    return _clients[profile]


def api_get(w: WorkspaceClient, path: str, query: dict | None = None) -> dict:
    return w.api_client.do("GET", path, query=query or {}) or {}


def paginate(w: WorkspaceClient, path: str, key: str, query: dict) -> list[dict]:
    items, token = [], None
    while True:
        q = dict(query)
        if token:
            q["page_token"] = token
        resp = api_get(w, path, q)
        items.extend(resp.get(key, []))
        token = resp.get("next_page_token")
        if not token or not resp.get("has_more", True):
            break
    return items


# --------------------------------------------------------------------------- #
# Notebook source scanning (%run and dbutils.notebook.run)
# --------------------------------------------------------------------------- #
RUN_MAGIC = re.compile(r"^\s*(?:#\s*MAGIC\s*)?%run\s+[\"']?([^\s\"']+)", re.M)
NB_RUN = re.compile(r"dbutils\.notebook\.run\(\s*[fr]?[\"']([^\"']+)[\"']")
TABLE_WRITE = re.compile(
    r"(?:saveAsTable|insertInto|toTable)\(\s*[fr]?[\"']([\w`.\-{}]+)[\"']"
    r"|(?:INSERT\s+(?:INTO|OVERWRITE)(?:\s+TABLE)?|MERGE\s+INTO|CREATE\s+(?:OR\s+REPLACE\s+)?TABLE(?:\s+IF\s+NOT\s+EXISTS)?)\s+([\w`.\-{}]+)",
    re.I,
)
TABLE_READ = re.compile(
    r"(?:spark\.table|spark\.read\.table|readStream\.table)\(\s*[fr]?[\"']([\w`.\-{}]+)[\"']"
    r"|\b(?:FROM|JOIN)\s+([a-zA-Z_`][\w`\-{}]*\.[\w`\-{}]+(?:\.[\w`\-{}]+)?)(?![\w.]*\s+import\b)",
    re.I,
)


NON_TABLE_ROOTS = {"pyspark", "databricks", "delta", "dbruntime", "os", "typing", "datetime",
                   "concurrent", "collections", "json", "sklearn", "mlflow", "pandas", "numpy"}
FILE_EXTS = {"cs", "py", "sql", "csv", "json", "txt", "parquet", "dtsx", "yml", "yaml", "xml", "scala"}


def _norm_table(t: str) -> str | None:
    t = t.strip("`").replace("`", "").lower()
    if "{" in t or t.count(".") < 1 or t.startswith(("system.information_schema",)):
        return None
    if t.split(".")[0] in NON_TABLE_ROOTS or t.rsplit(".", 1)[-1] in FILE_EXTS:
        return None
    return t


def resolve_path(base: str, ref: str) -> str:
    ref = ref.strip()
    if ref.startswith("/"):
        p = ref
    else:
        p = posixpath.normpath(posixpath.join(posixpath.dirname(base), ref))
    if p.startswith("/Workspace/"):
        p = p[len("/Workspace"):]
    return p


def norm_nb(path: str) -> str:
    return path[len("/Workspace"):] if path.startswith("/Workspace/") else path


def scan_notebook(w: WorkspaceClient, path: str) -> dict:
    info = {"path": path, "calls": [], "writes": [], "reads": [], "language": None,
            "object_id": None, "error": None}
    try:
        st = api_get(w, "/api/2.0/workspace/get-status", {"path": path})
        info["object_id"] = st.get("object_id")
        info["language"] = st.get("language")
        try:
            exp = api_get(w, "/api/2.0/workspace/export", {"path": path, "format": "SOURCE"})
        except Exception:  # noqa: BLE001  workspace files need RAW/AUTO
            exp = api_get(w, "/api/2.0/workspace/export", {"path": path, "format": "AUTO"})
        src = base64.b64decode(exp.get("content", "")).decode("utf-8", "replace")
        calls = {resolve_path(path, m) for m in RUN_MAGIC.findall(src)}
        calls |= {resolve_path(path, m) for m in NB_RUN.findall(src)}
        info["calls"] = sorted(calls)
        writes = {_norm_table(a or b) for a, b in TABLE_WRITE.findall(src)}
        reads = {_norm_table(a or b) for a, b in TABLE_READ.findall(src)}
        writes.discard(None)
        reads.discard(None)
        info["writes"] = sorted(writes)
        info["reads"] = sorted(reads - writes)
    except Exception as e:  # noqa: BLE001
        info["error"] = str(e)[:200]
    return info


# --------------------------------------------------------------------------- #
# Graph builder
# --------------------------------------------------------------------------- #
def task_kind(t: dict) -> tuple[str, str | None]:
    for k, ref in (
        ("notebook_task", "notebook_path"),
        ("run_job_task", "job_id"),
        ("pipeline_task", "pipeline_id"),
        ("spark_python_task", "python_file"),
        ("python_wheel_task", "package_name"),
        ("sql_task", None),
        ("dbt_task", None),
        ("condition_task", None),
        ("for_each_task", None),
        ("spark_jar_task", "main_class_name"),
        ("dashboard_task", "dashboard_id"),
        ("clean_rooms_notebook_task", "notebook_name"),
    ):
        if k in t:
            v = t[k].get(ref) if ref else None
            if k == "sql_task":
                s = t[k]
                v = (s.get("file") or {}).get("path") or (s.get("query") or {}).get("query_id") \
                    or (s.get("alert") or {}).get("alert_id") or (s.get("dashboard") or {}).get("dashboard_id")
            return k.replace("_task", ""), str(v) if v is not None else None
    return "other", None


def derive_job_dependencies(nodes: dict, edges: dict) -> None:
    """Roll notebook/file-level table I/O up to jobs and add job→job dependency edges.

    A job "owns" every notebook/file its tasks run, plus notebooks reached via %run / notebook.run.
    If job A writes table T and job B reads T (or is triggered by an update to T), then A → B.
    """
    out_by_src: dict[str, list[dict]] = {}
    for e in edges.values():
        out_by_src.setdefault(e["source"], []).append(e)

    job_assets: dict[str, set] = {}
    for jid, n in nodes.items():
        if n["type"] != "job":
            continue
        assets, stack = set(), [e["target"] for e in out_by_src.get(jid, [])
                                if e["kind"] in ("runs_notebook", "runs_file")]
        while stack:
            a = stack.pop()
            if a in assets:
                continue
            assets.add(a)
            stack += [e["target"] for e in out_by_src.get(a, []) if e["kind"] == "calls_notebook"]
        job_assets[jid] = assets

    writers: dict[str, set] = {}
    readers: dict[str, set] = {}
    for jid, assets in job_assets.items():
        for e in edges.values():
            if e["kind"] == "writes_table" and e["source"] in assets:
                writers.setdefault(e["target"], set()).add(jid)
            elif e["kind"] == "reads_table" and e["target"] in assets:
                readers.setdefault(e["source"], set()).add(jid)
    for e in edges.values():
        if e["kind"] == "table_trigger":
            readers.setdefault(e["source"], set()).add(e["target"])

    for jid, n in nodes.items():
        if n["type"] == "job":
            n["writes"] = sorted(t[4:] for t, js in writers.items() if jid in js)
            n["reads"] = sorted(t[4:] for t, js in readers.items() if jid in js)

    deps: dict[tuple, set] = {}
    for tbl, ws in writers.items():
        for wj in ws:
            for rj in readers.get(tbl, ()):
                if wj != rj:
                    deps.setdefault((wj, rj), set()).add(tbl[4:])
    for (src, dst), tables in deps.items():
        tl = sorted(tables)
        label = tl[0] if len(tl) == 1 else f"{tl[0]} +{len(tl) - 1}"
        edges[f"{src}->{dst}:data_dependency"] = {
            "id": f"{src}->{dst}:data_dependency", "source": src, "target": dst,
            "kind": "data_dependency", "label": label, "tables": tl}


def build_graph(profile: str, scan: bool) -> dict:
    w = client(profile)
    host = w.config.host.rstrip("/")
    t0 = time.time()

    jobs_summary = paginate(w, "/api/2.2/jobs/list", "jobs", {"limit": 100, "expand_tasks": "true"})

    def get_job(j):
        try:
            return api_get(w, "/api/2.2/jobs/get", {"job_id": j["job_id"]})
        except Exception:  # noqa: BLE001
            return j

    def get_runs(job_id):
        try:
            return api_get(w, "/api/2.2/jobs/runs/list",
                           {"job_id": job_id, "limit": RUNS_PER_JOB}).get("runs", [])
        except Exception:  # noqa: BLE001
            return []

    with ThreadPoolExecutor(16) as ex:
        jobs = list(ex.map(get_job, jobs_summary))
        runs_by_job = dict(zip([j["job_id"] for j in jobs],
                               ex.map(get_runs, [j["job_id"] for j in jobs])))

    pipelines = {}
    try:
        for p in paginate(w, "/api/2.0/pipelines", "statuses", {"max_results": 100}):
            pipelines[p["pipeline_id"]] = p
    except Exception:  # noqa: BLE001
        pass

    nodes: dict[str, dict] = {}
    edges: dict[str, dict] = {}

    def add_node(nid, **data):
        if nid not in nodes:
            nodes[nid] = {"id": nid, **data}
        else:
            nodes[nid].update({k: v for k, v in data.items() if v is not None})
        return nodes[nid]

    def add_edge(src, dst, kind, label=None):
        eid = f"{src}->{dst}:{kind}"
        if eid not in edges and src != dst:
            edges[eid] = {"id": eid, "source": src, "target": dst, "kind": kind, "label": label}

    notebook_users: dict[str, set] = {}
    job_detail = {}

    for j in jobs:
        jid = j["job_id"]
        s = j.get("settings", {})
        runs = runs_by_job.get(jid, [])
        run_states = []
        for r in runs:
            st = r.get("state", {}) or r.get("status", {})
            run_states.append({
                "run_id": r.get("run_id"),
                "life": st.get("life_cycle_state") or (r.get("status", {}) or {}).get("state"),
                "result": st.get("result_state") or ((r.get("status", {}) or {}).get("termination_details") or {}).get("code"),
                "start": r.get("start_time"),
                "end": r.get("end_time"),
                "duration": r.get("run_duration") or r.get("execution_duration"),
                "trigger": r.get("trigger"),
                "url": r.get("run_page_url"),
            })
        last = run_states[0] if run_states else None
        health = "idle"
        if last:
            if last["life"] in ("RUNNING", "PENDING", "QUEUED", "BLOCKED", "WAITING_FOR_RETRY"):
                health = "running"
            elif last["result"] == "SUCCESS":
                health = "success"
            elif last["result"] in ("FAILED", "TIMEDOUT", "INTERNAL_ERROR", "UPSTREAM_FAILED", "MAXIMUM_CONCURRENT_RUNS_REACHED"):
                health = "failed"
            elif last["result"] in ("CANCELED", "SKIPPED", "EXCLUDED", "UPSTREAM_CANCELED", "SUCCESS_WITH_FAILURES"):
                health = "cancelled"

        schedule = None
        if s.get("schedule"):
            schedule = {"type": "cron", "expr": s["schedule"].get("quartz_cron_expression"),
                        "tz": s["schedule"].get("timezone_id"), "paused": s["schedule"].get("pause_status")}
        trig = s.get("trigger") or {}
        if trig:
            if "table_update" in trig:
                schedule = {"type": "table_update", "tables": trig["table_update"].get("table_names", []),
                            "paused": trig.get("pause_status")}
            elif "file_arrival" in trig:
                schedule = {"type": "file_arrival", "url": trig["file_arrival"].get("url"),
                            "paused": trig.get("pause_status")}
            elif "periodic" in trig:
                p = trig["periodic"]
                schedule = {"type": "periodic", "expr": f"every {p.get('interval')} {p.get('unit', '').lower()}",
                            "paused": trig.get("pause_status")}
        if s.get("continuous"):
            schedule = {"type": "continuous", "paused": s["continuous"].get("pause_status")}

        tasks_out = []
        all_tasks = list(s.get("tasks", []))
        # unwrap for_each inner tasks
        for t in list(all_tasks):
            inner = (t.get("for_each_task") or {}).get("task")
            if inner:
                inner = dict(inner)
                inner["task_key"] = f"{t['task_key']} ⟳ {inner.get('task_key', 'iteration')}"
                inner["depends_on"] = [{"task_key": t["task_key"]}]
                all_tasks.append(inner)

        for t in all_tasks:
            kind, ref = task_kind(t)
            tasks_out.append({
                "task_key": t.get("task_key"),
                "kind": kind,
                "ref": ref,
                "depends_on": [{"task_key": d.get("task_key"), "outcome": d.get("outcome")}
                               for d in t.get("depends_on", [])],
                "run_if": t.get("run_if"),
                "description": t.get("description"),
                "condition": t.get("condition_task"),
            })
            job_node = f"job:{jid}"
            if kind == "notebook" and ref:
                nb = norm_nb(ref)
                notebook_users.setdefault(nb, set()).add(jid)
                add_node(f"nb:{nb}", type="notebook", label=posixpath.basename(nb), path=nb)
                add_edge(job_node, f"nb:{nb}", "runs_notebook", t.get("task_key"))
            elif kind == "run_job" and ref:
                add_edge(job_node, f"job:{ref}", "triggers_job", t.get("task_key"))
            elif kind == "pipeline" and ref:
                p = pipelines.get(ref, {})
                add_node(f"pl:{ref}", type="pipeline", label=p.get("name", ref), pipeline_id=ref,
                         state=p.get("state"), health=p.get("health"),
                         url=f"{host}/pipelines/{ref}")
                add_edge(job_node, f"pl:{ref}", "runs_pipeline", t.get("task_key"))
            elif kind in ("spark_python", "sql") and ref and "/" in ref:
                fid = f"file:{ref}"
                add_node(fid, type="file", label=posixpath.basename(ref), path=ref, lang=kind)
                add_edge(job_node, fid, "runs_file", t.get("task_key"))

        if schedule and schedule.get("type") == "table_update":
            for tbl in schedule["tables"]:
                add_node(f"tbl:{tbl.lower()}", type="table", label=tbl.lower(), full_name=tbl.lower())
                add_edge(f"tbl:{tbl.lower()}", f"job:{jid}", "table_trigger", "on update")

        creator = j.get("creator_user_name")
        tags = s.get("tags", {})
        job_detail[jid] = True
        add_node(f"job:{jid}", type="job", label=s.get("name", str(jid)), job_id=jid,
                 creator=creator, run_as=j.get("run_as_user_name"), tags=tags,
                 schedule=schedule, health=health, runs=run_states, tasks=tasks_out,
                 task_count=len(tasks_out), url=f"{host}/jobs/{jid}",
                 max_concurrent=s.get("max_concurrent_runs"),
                 bundle="deployment" in s and s["deployment"].get("kind") == "BUNDLE",
                 created=j.get("created_time"))

    # Jobs referenced by run_job but not visible (permissions / deleted)
    for e in list(edges.values()):
        if e["target"] not in nodes and e["target"].startswith("job:"):
            add_node(e["target"], type="job", label=f"job {e['target'][4:]} (not visible)",
                     job_id=e["target"][4:], health="unknown", ghost=True, tasks=[], runs=[])

    # Notebook source scanning — discovers notebook→notebook calls and table I/O
    scanned = 0
    if scan:
        frontier = [n["path"] for n in nodes.values() if n["type"] == "notebook"]
        seen = set()
        with ThreadPoolExecutor(12) as ex:
            while frontier and scanned < MAX_NOTEBOOK_SCANS:
                batch = [p for p in dict.fromkeys(frontier) if p not in seen][: MAX_NOTEBOOK_SCANS - scanned]
                frontier = []
                seen.update(batch)
                for info in ex.map(lambda p: scan_notebook(w, p), batch):
                    scanned += 1
                    nid = f"nb:{info['path']}"
                    add_node(nid, type="notebook", label=posixpath.basename(info["path"]),
                             path=info["path"], language=info["language"],
                             scan_error=info["error"],
                             url=f"{host}/#notebook/{info['object_id']}" if info["object_id"] else None)
                    for c in info["calls"]:
                        add_node(f"nb:{c}", type="notebook", label=posixpath.basename(c), path=c,
                                 called_only=True)
                        add_edge(nid, f"nb:{c}", "calls_notebook", "%run / notebook.run")
                        if c not in seen:
                            frontier.append(c)
                    for tbl in info["writes"]:
                        add_node(f"tbl:{tbl}", type="table", label=tbl, full_name=tbl)
                        add_edge(nid, f"tbl:{tbl}", "writes_table", "writes")
                    for tbl in info["reads"]:
                        add_node(f"tbl:{tbl}", type="table", label=tbl, full_name=tbl)
                        add_edge(f"tbl:{tbl}", nid, "reads_table", "reads")

        # Python / SQL files run directly by tasks
        files = [n for n in nodes.values() if n["type"] == "file" and n["path"].startswith("/")]
        with ThreadPoolExecutor(12) as ex2:
            file_infos = list(ex2.map(lambda f: scan_notebook(w, norm_nb(f["path"])), files))
        for n, info in zip(files, file_infos):
            scanned += 1
            n["scan_error"] = info["error"]
            for tbl in info["writes"]:
                add_node(f"tbl:{tbl}", type="table", label=tbl, full_name=tbl)
                add_edge(n["id"], f"tbl:{tbl}", "writes_table", "writes")
            for tbl in info["reads"]:
                add_node(f"tbl:{tbl}", type="table", label=tbl, full_name=tbl)
                add_edge(f"tbl:{tbl}", n["id"], "reads_table", "reads")

    derive_job_dependencies(nodes, edges)

    for nb, users in notebook_users.items():
        n = nodes.get(f"nb:{nb}")
        if n:
            n["used_by_jobs"] = sorted(users)
            n["shared"] = len(users) > 1

    # Stats
    now_ms = time.time() * 1000
    all_runs = [r for j in jobs for r in runs_by_job.get(j["job_id"], [])]
    recent = [r for r in all_runs if r.get("start_time", 0) > now_ms - 24 * 3600 * 1000
              and (r.get("state") or {}).get("life_cycle_state") in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED")]

    def res(r):
        return (r.get("state") or {}).get("result_state")

    def life(r):
        return (r.get("state") or {}).get("life_cycle_state")

    stats = {
        "jobs": sum(1 for n in nodes.values() if n["type"] == "job" and not n.get("ghost")),
        "tasks": sum(n.get("task_count", 0) for n in nodes.values() if n["type"] == "job"),
        "notebooks": sum(1 for n in nodes.values() if n["type"] == "notebook"),
        "pipelines": sum(1 for n in nodes.values() if n["type"] == "pipeline"),
        "tables": sum(1 for n in nodes.values() if n["type"] == "table"),
        "job_links": sum(1 for e in edges.values() if e["kind"] == "triggers_job"),
        "notebook_links": sum(1 for e in edges.values() if e["kind"] == "calls_notebook"),
        "data_deps": sum(1 for e in edges.values() if e["kind"] == "data_dependency"),
        "shared_notebooks": sum(1 for n in nodes.values() if n.get("shared")),
        "runs_24h": len(recent),
        "success_24h": sum(1 for r in recent if res(r) == "SUCCESS"),
        "failed_24h": sum(1 for r in recent if res(r) in ("FAILED", "TIMEDOUT", "INTERNAL_ERROR")),
        "running": sum(1 for r in all_runs if life(r) in ("RUNNING", "PENDING", "QUEUED", "BLOCKED")),
        "scheduled": sum(1 for n in nodes.values() if n["type"] == "job" and n.get("schedule")),
        "scanned_notebooks": scanned,
        "build_seconds": round(time.time() - t0, 2),
    }

    feed = sorted(
        [{"job_id": j["job_id"], "job": j.get("settings", {}).get("name"),
          "run_id": r.get("run_id"), "start": r.get("start_time"), "end": r.get("end_time"),
          "life": life(r), "result": res(r), "url": r.get("run_page_url"),
          "trigger": r.get("trigger"), "duration": r.get("run_duration")}
         for j in jobs for r in runs_by_job.get(j["job_id"], [])],
        key=lambda r: r["start"] or 0, reverse=True)[:60]

    return {"profile": profile, "host": host, "generated_at": int(now_ms),
            "nodes": list(nodes.values()), "edges": list(edges.values()),
            "stats": stats, "feed": feed}


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #
@app.get("/api/environments")
def environments():
    return {"app_mode": APP_MODE, "environments": list_profiles()}


@app.get("/api/check")
def check(profile: str = Query(...)):
    try:
        me = client(profile).current_user.me()
        return {"ok": True, "user": me.user_name, "host": client(profile).config.host}
    except Exception as e:  # noqa: BLE001
        _clients.pop(profile, None)
        return JSONResponse({"ok": False, "error": str(e)[:400]}, status_code=200)


@app.get("/api/graph")
def graph(profile: str = Query(...), scan: bool = True, refresh: bool = False):
    key = (profile, scan)
    if not refresh and key in _cache and time.time() - _cache[key][0] < CACHE_TTL:
        return _cache[key][1]
    try:
        data = build_graph(profile, scan)
    except Exception as e:  # noqa: BLE001
        _clients.pop(profile, None)
        raise HTTPException(status_code=502, detail=str(e)[:500])
    _cache[key] = (time.time(), data)
    return data


def _run_summary(r: dict) -> dict:
    st = r.get("state", {}) or {}
    tasks = r.get("tasks", []) or []
    def tstate(t):
        ts = t.get("state", {}) or {}
        return ts.get("life_cycle_state"), ts.get("result_state")
    running = [t.get("task_key") for t in tasks if tstate(t)[0] in ("RUNNING", "TERMINATING", "QUEUED")]
    waiting = [t.get("task_key") for t in tasks if tstate(t)[0] in ("PENDING", "BLOCKED", "WAITING_FOR_RETRY")]
    done = [t for t in tasks if tstate(t)[0] in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")]
    failed = [t.get("task_key") for t in tasks if tstate(t)[1] in ("FAILED", "TIMEDOUT", "INTERNAL_ERROR")]
    return {
        "run_id": r.get("run_id"), "job_id": r.get("job_id"), "run_name": r.get("run_name"),
        "life": st.get("life_cycle_state"), "result": st.get("result_state"), "message": st.get("state_message"),
        "start": r.get("start_time"), "end": r.get("end_time"), "trigger": r.get("trigger"),
        "url": r.get("run_page_url"), "parent_run_id": (r.get("trigger_info") or {}).get("run_id"),
        "tasks_total": len(tasks), "tasks_done": len(done), "running_tasks": running, "waiting_tasks": waiting, "failed_tasks": failed,
    }


@app.get("/api/active")
def active(profile: str = Query(...), recent_minutes: int = 120):
    """Workspace-wide view of what is running now, plus runs that finished recently."""
    w = client(profile)
    try:
        act = paginate(w, "/api/2.2/jobs/runs/list", "runs", {"active_only": "true", "limit": 25, "expand_tasks": "true"})
        since = int((time.time() - recent_minutes * 60) * 1000)
        rec = api_get(w, "/api/2.2/jobs/runs/list",
                      {"completed_only": "true", "limit": 25, "start_time_from": since, "expand_tasks": "true"}).get("runs", [])
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=str(e)[:500])
    return {"generated_at": int(time.time() * 1000),
            "active": [_run_summary(r) for r in act],
            "recent": [_run_summary(r) for r in rec]}


@app.get("/api/run")
def run_detail(profile: str = Query(...), run_id: int = Query(...)):
    """Per-task state for a single job run (used to colour the task DAG)."""
    try:
        r = api_get(client(profile), "/api/2.2/jobs/runs/get", {"run_id": run_id})
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=str(e)[:500])
    tasks = {}
    for t in r.get("tasks", []):
        st = t.get("state", {})
        tasks[t.get("task_key")] = {
            "life": st.get("life_cycle_state"), "result": st.get("result_state"),
            "message": st.get("state_message"), "duration": t.get("execution_duration") or t.get("run_duration"),
            "url": t.get("run_page_url"), "start": t.get("start_time"),
        }
    st = r.get("state", {}) or {}
    return {"run_id": run_id, "url": r.get("run_page_url"), "tasks": tasks,
            "life": st.get("life_cycle_state"), "result": st.get("result_state"), "start": r.get("start_time")}


# --------------------------------------------------------------------------- #
# Portable snapshot — one self-contained HTML file, no server needed
# --------------------------------------------------------------------------- #
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")


def build_snapshot(profile: str, redact: bool) -> str:
    g = graph(profile=profile, scan=True, refresh=False)
    try:
        live = active(profile=profile)
    except HTTPException:
        live = {"generated_at": g["generated_at"], "active": [], "recent": []}
    run_ids = {j["runs"][0]["run_id"] for j in g["nodes"] if j["type"] == "job" and j.get("runs")}
    run_ids |= {r["run_id"] for r in g["feed"] if (r.get("start") or 0) > g["generated_at"] - 24 * 3600 * 1000}
    run_ids |= {r["run_id"] for r in live["active"]}

    def fetch(rid):
        try:
            return str(rid), run_detail(profile=profile, run_id=rid)
        except Exception:  # noqa: BLE001
            return str(rid), {"run_id": rid, "tasks": {}}

    with ThreadPoolExecutor(12) as ex:
        runs = dict(ex.map(fetch, [r for r in run_ids if r]))
    try:
        user = client(profile).current_user.me().user_name
    except Exception:  # noqa: BLE001
        user = ""
    snap = {
        "taken_at": int(time.time() * 1000), "profile": profile,
        "environments": {"app_mode": True, "environments": [{"name": profile, "host": g["host"]}]},
        "check": {"ok": True, "user": user, "host": g["host"]},
        "graph": g, "active": live, "runs": runs,
    }
    payload = json.dumps(snap, separators=(",", ":"), default=str)
    if redact:
        mapping: dict[str, str] = {}
        payload = EMAIL_RE.sub(lambda m: mapping.setdefault(m.group(0).lower(), f"user{len(mapping) + 1}@redacted"), payload)
        # also hide the username part where it appears on its own (e.g. "[dev troy_kirkland]" bundle prefixes)
        for email, alias in mapping.items():
            local = email.split("@")[0]
            for variant in {local, local.replace(".", "_"), local.replace(".", "-")}:
                if len(variant) >= 4:
                    payload = re.sub(re.escape(variant), alias.split("@")[0], payload, flags=re.I)
    payload = payload.replace("</", "<\\/")  # never close the <script> early

    html = (HERE / "static" / "index.html").read_text()
    for lib in ("cytoscape.min.js", "dagre.min.js", "cytoscape-dagre.js"):
        src = (HERE / "static" / "vendor" / lib).read_text().replace("</script", "<\\/script")
        html = html.replace(f'<script src="/static/vendor/{lib}"></script>', f"<script>{src}</script>")
    if "<!--SNAPSHOT-DATA-->" not in html:
        raise HTTPException(status_code=500, detail="index.html is missing the <!--SNAPSHOT-DATA--> marker")
    html = html.replace("<!--SNAPSHOT-DATA-->", f"<script>window.SNAPSHOT={payload};</script>", 1)
    return html


@app.get("/api/snapshot")
def snapshot(profile: str = Query(...), redact: bool = True):
    html = build_snapshot(profile, redact)
    stamp = datetime.now().strftime("%Y%m%d-%H%M")
    safe = re.sub(r"[^A-Za-z0-9_-]+", "-", profile)
    return HTMLResponse(html, headers={"Content-Disposition": f'attachment; filename="job-constellation-{safe}-{stamp}.html"'})


@app.get("/")
def index():
    return FileResponse(HERE / "static" / "index.html")


app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")
