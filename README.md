# Job Constellation — Databricks Jobs Control Centre

A colourful map of every Databricks Job in a workspace, and the links between jobs, tasks, notebooks, files, pipelines and tables.

## Sharing

| You want to… | Do this |
|---|---|
| **Send someone a view** (customer, exec, Slack) | Click **⇪ Share → Download .html**. One self-contained file (about 850 KB) that opens by double-click with no login, server or internet. Every view is clickable, frozen at the time of export. "Hide email addresses" is on by default and also masks usernames in job names and paths. |
| **Let a colleague run it on their own workspaces** | Send `job-constellation-tool.zip`. They unzip it and run `./run.sh` (macOS/Linux) or `run.bat` (Windows). Needs Python 3.10+ and the Databricks CLI logged in (`databricks auth login --profile <name>`). |
| **Give a team a permanent link** | Deploy it as a Databricks App (see below). |

## Run it

```bash
./run.sh            # macOS / Linux → http://localhost:8765
run.bat             # Windows
```

Pick an environment (any profile in `~/.databrickscfg`; FEVM workspaces are starred and listed first). The page remembers the last one you used.

## What it finds

| Connection | Source |
|---|---|
| Job → Job | `run_job_task` |
| Job → Notebook / File / Pipeline | `notebook_task`, `spark_python_task`, `sql_task.file`, `pipeline_task` |
| Notebook → Notebook | `%run` and `dbutils.notebook.run(...)` found by parsing notebook source (deep scan) |
| Notebook → Table (writes / reads) | `saveAsTable`, `INSERT INTO`, `MERGE INTO`, `CREATE TABLE`, `spark.table`, `FROM/JOIN a.b` (regex, best-effort) |
| Table → Job | Table-update triggers |
| Shared notebook | Same notebook used by more than one job (gold ring) |

## Three views (tabs above the graph)

- **✦ Constellation**: everything (jobs, notebooks, files, tables, pipelines) and how they connect.
- **⇄ Dependencies**: jobs only. Pick a job to see everything it depends on (left, teal) and everything that depends on it (right, pink), with "impact if it fails". Click any job to re-centre on it. The breadcrumb trail and `Esc` step back. Click a link to see *why* it exists:
  - **▶ Direct**: a `run_job` task starts the other job.
  - **⛁ Via table**: one job writes a table the other reads, or the other is triggered by that table updating. Detected by deep scan.
- **● Live**: every run currently in progress across the workspace (`runs/list?active_only`), refreshed every 10 seconds. Each card shows elapsed time, task progress, the steps running now, failed steps and the downstream jobs waiting on it. **Live task DAG** colours each task by its status and refreshes every 8 seconds until the run finishes.

## Using the UI

- **Top KPIs**: jobs, tasks, notebooks, links, 24-hour success rate, running now.
- **Left panel**: job list with the last 10 runs as a coloured bar, filterable by health; press `/` to search.
- **Centre**: the constellation graph. Hover to light up the full upstream and downstream lineage, click to inspect, double-click a job to open its **Task DAG** (each task coloured by its status in the latest run, condition branches labelled ✓/✗).
- **Right panel**: fleet-health donut, most-connected jobs and the live run feed, or the details of the selected item, with deep links into Databricks.
- **Layers**: toggle Jobs, Notebooks, Tables, Files and Pipelines; switch between Flow, Organic and Hub layouts; turn on Auto-refresh every 60 seconds.

## Moving to a Databricks App

The code is already shaped for this. `server.py` detects `DATABRICKS_APP_NAME` and switches to the app's own credentials (single environment, dropdown locked).

```bash
databricks sync . /Workspace/Users/<you>/job-constellation -p <profile>
databricks apps create job-constellation -p <profile>
databricks apps deploy job-constellation --source-code-path /Workspace/Users/<you>/job-constellation -p <profile>
```

Give the app's service principal `CAN_VIEW` on the jobs and read access to the notebooks. Otherwise switch to on-behalf-of-user auth so each viewer sees what they are allowed to see.

## Ideas for v2

- Table lineage from `system.access.table_lineage` (exact, not regex) through a SQL warehouse.
- Cost per job from `system.billing.usage`.
- Run timeline (Gantt view) across all jobs for the last 24 hours.
