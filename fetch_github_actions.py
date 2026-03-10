#!/usr/bin/env python3
"""
Fetch GitHub Actions workflow runs from a public repo and save as CSV.

Usage:

    python fetch_github_actions.py --repo prometheus/prometheus --max-runs 500 --created "2024-10-01..2025-01-01"
"""

import argparse
import csv
import json
import os
import sys
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from urllib.parse import quote
from datetime import datetime

def fetch_json(url, token=None):
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    req = Request(url, headers=headers)
    try:
        with urlopen(req) as resp:
            remaining = resp.headers.get("X-RateLimit-Remaining", "?")
            reset = resp.headers.get("X-RateLimit-Reset", "")
            if remaining != "?" and int(remaining) < 5:
                reset_time = datetime.fromtimestamp(int(reset))
                print(f"Rate limit almost exhausted. Resets at {reset_time}")
            return json.loads(resp.read().decode())
    except HTTPError as e:
        if e.code == 403:
            reset = e.headers.get("X-RateLimit-Reset", "")
            if reset:
                reset_time = datetime.fromtimestamp(int(reset))
                print(f"\nRate limited! Resets at {reset_time}")
                print("   Tip: pass --token YOUR_GITHUB_TOKEN for higher limits")
            else:
                print(f"\n403 Forbidden: {e.read().decode()}")
            sys.exit(1)
        raise


def fetch_all_runs(repo, max_runs, token=None, created_filter=None):
    runs = []
    page = 1
    per_page = 100

    while len(runs) < max_runs:
        url = f"https://api.github.com/repos/{repo}/actions/runs?per_page={per_page}&page={page}"
        if created_filter:
            url += f"&created={quote(created_filter)}"
        print(f"  Fetching page {page} (have {len(runs)} runs so far)...")
        data = fetch_json(url, token)

        batch = data.get("workflow_runs", [])
        if not batch:
            break

        runs.extend(batch)
        page += 1
        time.sleep(0.5)

    return runs[:max_runs]


def fetch_workflows(repo, token=None):
    url = f"https://api.github.com/repos/{repo}/actions/workflows?per_page=100"
    print("  Fetching workflow definitions...")
    data = fetch_json(url, token)
    return {w["id"]: w for w in data.get("workflows", [])}


def fetch_jobs_for_run(repo, run_id, token=None):
    url = f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/jobs?per_page=100"
    data = fetch_json(url, token)
    return data.get("jobs", [])


def save_runs_csv(runs, workflows, output_path):
    fieldnames = [
        "run_id", "run_number", "workflow_id", "workflow_name",
        "event", "status", "conclusion", "head_branch", "head_sha",
        "actor_login", "created_at", "updated_at", "run_started_at",
        "run_attempt", "url"
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in runs:
            wf = workflows.get(r.get("workflow_id"), {})
            writer.writerow({
                "run_id": r["id"],
                "run_number": r["run_number"],
                "workflow_id": r.get("workflow_id", ""),
                "workflow_name": r.get("name", wf.get("name", "")),
                "event": r["event"],
                "status": r["status"],
                "conclusion": r.get("conclusion", ""),
                "head_branch": r.get("head_branch", ""),
                "head_sha": r.get("head_sha", ""),
                "actor_login": r.get("actor", {}).get("login", ""),
                "created_at": r.get("created_at", ""),
                "updated_at": r.get("updated_at", ""),
                "run_started_at": r.get("run_started_at", ""),
                "run_attempt": r.get("run_attempt", 1),
                "url": r.get("html_url", ""),
            })

    print(f"   Saved {len(runs)} runs to {output_path}")


def save_jobs_csv(all_jobs, output_path):
    fieldnames = [
        "job_id", "run_id", "job_name", "status", "conclusion",
        "started_at", "completed_at", "runner_name", "runner_os",
        "run_attempt", "workflow_name"
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for j in all_jobs:
            writer.writerow({
                "job_id": j["id"],
                "run_id": j["run_id"],
                "job_name": j["name"],
                "status": j["status"],
                "conclusion": j.get("conclusion", ""),
                "started_at": j.get("started_at", ""),
                "completed_at": j.get("completed_at", ""),
                "runner_name": j.get("runner_name", ""),
                "runner_os": (j.get("labels") or [""])[0] if j.get("labels") else "",
                "run_attempt": j.get("run_attempt", 1),
                "workflow_name": j.get("workflow_name", ""),
            })

    print(f"    Saved {len(all_jobs)} jobs to {output_path}")


def save_workflows_csv(workflows, output_path):
    """Save workflow definitions to CSV."""
    fieldnames = ["workflow_id", "name", "path", "state", "created_at", "updated_at"]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for wid, w in workflows.items():
            writer.writerow({
                "workflow_id": wid,
                "name": w["name"],
                "path": w["path"],
                "state": w["state"],
                "created_at": w.get("created_at", ""),
                "updated_at": w.get("updated_at", ""),
            })

    print(f"    Saved {len(workflows)} workflows to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Fetch GitHub Actions data as CSV")
    parser.add_argument("--repo", default="grafana/grafana",
                        help="GitHub repo (default: grafana/grafana)")
    parser.add_argument("--max-runs", type=int, default=500,
                        help="Max workflow runs to fetch (default: 500)")
    parser.add_argument("--token", default=None,
                        help="GitHub token (or set GITHUB_TOKEN env var)")
    parser.add_argument("--fetch-jobs", action="store_true",
                        help="Also fetch individual jobs per run (SLOW - many API calls)")
    parser.add_argument("--jobs-sample", type=int, default=50,
                        help="If --fetch-jobs, only fetch jobs for this many runs (default: 50)")
    parser.add_argument("--output-dir", default=".",
                        help="Output directory for CSVs")
    parser.add_argument("--created", default=None,
                        help="Date filter for runs, e.g. '2024-10-01..2025-01-01' or '>=2024-06-01'")
    args = parser.parse_args()

    token = args.token or os.environ.get("GITHUB_TOKEN")
    repo = args.repo
    safe_repo = repo.replace("/", "_")

    print(f"  Fetching data from {repo}")
    if not token:
        print("   (No auth token - rate limit is ~60 requests/hour)")
        print("   Tip: set GITHUB_TOKEN env var or pass --token for 5000 req/hr\n")

    workflows = fetch_workflows(repo, token)
    wf_path = os.path.join(args.output_dir, f"{safe_repo}_workflows.csv")
    save_workflows_csv(workflows, wf_path)

    print(f"\n  Fetching up to {args.max_runs} workflow runs...")
    runs = fetch_all_runs(repo, args.max_runs, token, args.created)
    runs_path = os.path.join(args.output_dir, f"{safe_repo}_runs.csv")
    save_runs_csv(runs, workflows, runs_path)

    if args.fetch_jobs:
        sample = runs[:args.jobs_sample]
        print(f"\n   Fetching jobs for {len(sample)} runs (this may take a while)...")
        all_jobs = []
        for i, r in enumerate(sample):
            print(f"  [{i+1}/{len(sample)}] Run {r['id']}...")
            jobs = fetch_jobs_for_run(repo, r["id"], token)
            for j in jobs:
                j["workflow_name"] = r.get("name", "")
            all_jobs.extend(jobs)
            time.sleep(0.5)

        jobs_path = os.path.join(args.output_dir, f"{safe_repo}_jobs.csv")
        save_jobs_csv(all_jobs, jobs_path)

    print(f"\n{'='*60}")
    print(f"  Summary for {repo}:")
    print(f"   Workflows: {len(workflows)}")
    print(f"   Runs fetched: {len(runs)}")
    if runs:
        dates = [r["created_at"][:10] for r in runs if r.get("created_at")]
        print(f"   Date range: {min(dates)} to {max(dates)}")
        events = {}
        for r in runs:
            e = r.get("event", "unknown")
            events[e] = events.get(e, 0) + 1
        print(f"   Event types: {events}")
        conclusions = {}
        for r in runs:
            c = r.get("conclusion") or r.get("status", "unknown")
            conclusions[c] = conclusions.get(c, 0) + 1
        print(f"   Conclusions: {conclusions}")


if __name__ == "__main__":
    main()