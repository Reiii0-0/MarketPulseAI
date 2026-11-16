from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import platform
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, int(round((pct / 100.0) * (len(sorted_vals) - 1)))))
    return sorted_vals[idx]


def _hardware_info() -> dict[str, Any]:
    mem_bytes = 0
    if Path("/proc/meminfo").exists():
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                kb = int(line.split()[1])
                mem_bytes = kb * 1024
                break
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count() or 1,
        "mem_total_gib": round(mem_bytes / (1024**3), 2) if mem_bytes else None,
        "python_version": sys.version.split()[0],
        "load_average": list(os.getloadavg()) if hasattr(os, "getloadavg") else None,
    }


def _collect_docker_stats(project_name: str | None) -> list[dict[str, str]]:
    cmd = [
        "docker",
        "stats",
        "--no-stream",
        "--format",
        "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=30)
    if proc.returncode != 0:
        return []
    rows: list[dict[str, str]] = []
    prefix = f"{project_name}-" if project_name else None
    for line in proc.stdout.strip().splitlines():
        parts = line.split("\t")
        if len(parts) != 4:
            continue
        name, cpu_pct, mem_usage, mem_pct = parts
        if prefix and not name.startswith(prefix):
            continue
        rows.append(
            {
                "container": name,
                "cpu_pct": cpu_pct,
                "mem_usage": mem_usage,
                "mem_pct": mem_pct,
            }
        )
    return rows


def validate_response(endpoint: str, body: bytes, run_id: str, ticker: str) -> None:
    payload = json.loads(body)
    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object")
    if endpoint == "/health/ready":
        valid = payload.get("ready") is True
    elif endpoint.startswith("/api/v1/runs?"):
        valid = any(row.get("run_id") == run_id for row in payload.get("runs", []))
    elif endpoint.startswith("/api/v1/tickers?"):
        valid = ticker in payload.get("tickers", [])
    elif endpoint == "/api/v1/evidence":
        valid = bool((payload.get("active_bundle_manifest") or {}).get("bundle_id"))
    else:
        key = endpoint.split("/")[-1].split("?")[0]
        rows = payload.get(key, [])
        valid = bool(rows) and all(row.get("ticker") == ticker for row in rows)
        if key != "bars":
            valid = valid and all(row.get("run_id") == run_id for row in rows)
    if not valid:
        raise ValueError("Response does not contain the requested benchmark workload")


def run_benchmark(
    base_url: str,
    run_id: str,
    ticker: str,
    duration_seconds: float,
    target_rps: float,
    concurrency: int,
    compose_project: str | None = None,
) -> dict[str, Any]:
    """Measure read API latency, payload size, errors, and container resource usage."""
    if (
        not all(math.isfinite(x) and x > 0 for x in (duration_seconds, target_rps))
        or concurrency < 1
    ):
        raise ValueError("duration, target RPS and concurrency must be positive and finite")
    base = base_url.rstrip("/")
    run_query = urlencode({"run_id": run_id})
    query = urlencode({"run_id": run_id, "ticker": ticker})
    endpoints = [
        "/health/ready",
        "/api/v1/runs?limit=10",
        f"/api/v1/tickers?{run_query}",
        f"/api/v1/bars?{query}&limit=60",
        f"/api/v1/predictions?{query}&limit=50",
        f"/api/v1/outcomes?{query}&limit=50",
        "/api/v1/evidence",
    ]

    # Warm-up request per endpoint
    for ep in endpoints:
        with urllib.request.urlopen(f"{base}{ep}", timeout=5) as resp:
            validate_response(ep, resp.read(), run_id, ticker)

    total_requests = max(1, int(round(duration_seconds * target_rps)))
    spacing_s = 1.0 / max(target_rps, 0.1)
    per_endpoint: dict[str, list[float]] = {ep: [] for ep in endpoints}
    per_endpoint_bytes: dict[str, list[int]] = {ep: [] for ep in endpoints}
    all_latencies_ms: list[float] = []
    errors: list[str] = []
    scheduled_latencies: list[float] = []
    scheduling_delays: list[float] = []

    def _hit(idx: int, start_anchor: float) -> tuple[str, float, int, str | None, float, float]:
        ep = endpoints[idx % len(endpoints)]
        due = start_anchor + idx * spacing_s
        now = time.perf_counter()
        if due > now:
            time.sleep(due - now)
        t0 = time.perf_counter()
        delay_ms = max(0.0, (t0 - due) * 1000)
        try:
            with urllib.request.urlopen(f"{base}{ep}", timeout=10) as resp:
                body = resp.read()
                validate_response(ep, body, run_id, ticker)
                dt_ms = (time.perf_counter() - t0) * 1000.0
                if resp.status >= 400:
                    return (
                        ep,
                        dt_ms,
                        len(body),
                        f"{ep}:HTTP_{resp.status}",
                        dt_ms + delay_ms,
                        delay_ms,
                    )
                return ep, dt_ms, len(body), None, dt_ms + delay_ms, delay_ms
        except urllib.error.HTTPError as exc:
            dt_ms = (time.perf_counter() - t0) * 1000.0
            return ep, dt_ms, 0, f"{ep}:HTTP_{exc.code}", dt_ms + delay_ms, delay_ms
        except Exception as exc:
            dt_ms = (time.perf_counter() - t0) * 1000.0
            return ep, dt_ms, 0, f"{ep}:{type(exc).__name__}", dt_ms + delay_ms, delay_ms

    wall_t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(_hit, i, wall_t0) for i in range(total_requests)]
        for fut in concurrent.futures.as_completed(futures):
            ep, dt_ms, n_bytes, err, scheduled_ms, delay_ms = fut.result()
            scheduled_latencies.append(scheduled_ms)
            scheduling_delays.append(delay_ms)
            all_latencies_ms.append(dt_ms)
            per_endpoint[ep].append(dt_ms)
            per_endpoint_bytes[ep].append(n_bytes)
            if err is not None:
                errors.append(err)
    elapsed_s = max(time.perf_counter() - wall_t0, 1e-6)

    all_sorted = sorted(all_latencies_ms)
    endpoint_summary: dict[str, dict[str, Any]] = {}
    for ep in endpoints:
        vals = sorted(per_endpoint[ep])
        bvals = per_endpoint_bytes[ep]
        endpoint_summary[ep] = {
            "count": len(vals),
            "p50_ms": round(_percentile(vals, 50), 2),
            "p95_ms": round(_percentile(vals, 95), 2),
            "p99_ms": round(_percentile(vals, 99), 2),
            "max_ms": round(max(vals) if vals else 0.0, 2),
            "avg_payload_bytes": int(round(sum(bvals) / len(bvals))) if bvals else 0,
        }

    slowest_ep = max(
        endpoint_summary.items(),
        key=lambda item: float(item[1]["p95_ms"]),
    )[0]

    return {
        "measured_at": datetime.now(UTC).isoformat(),
        "base_url": base,
        "workload": {
            "run_id": run_id,
            "ticker": ticker,
            "target_rps": target_rps,
            "concurrency": concurrency,
            "configured_duration_seconds": duration_seconds,
            "actual_duration_seconds": round(elapsed_s, 3),
            "total_requests": len(all_sorted),
            "achieved_rps": round(len(all_sorted) / elapsed_s, 2),
            "warm_state": True,
        },
        "hardware": _hardware_info(),
        "scheduled_latency_ms": {
            "p95": round(_percentile(sorted(scheduled_latencies), 95), 2),
            "max": round(max(scheduled_latencies), 2),
        },
        "scheduling_delay_ms": {
            "p95": round(_percentile(sorted(scheduling_delays), 95), 2),
            "max": round(max(scheduling_delays), 2),
        },
        "response_validation": "Every warmup/timed response parsed; readiness, bundle, run, ticker and nonempty data checked.",
        "latency_ms": {
            "p50": round(_percentile(all_sorted, 50), 2),
            "p95": round(_percentile(all_sorted, 95), 2),
            "p99": round(_percentile(all_sorted, 99), 2),
            "max": round(max(all_sorted) if all_sorted else 0.0, 2),
        },
        "error_count": len(errors),
        "sample_errors": errors[:10],
        "slowest_endpoint_p95": slowest_ep,
        "endpoints": endpoint_summary,
        "container_resources": _collect_docker_stats(compose_project),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure read API latency and Compose resources.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--run-id", default="replay-smoke-1")
    parser.add_argument("--ticker", default="AAPL")
    parser.add_argument("--duration-seconds", type=float, default=12.0)
    parser.add_argument("--target-rps", type=float, default=10.0)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--compose-project", default=None)
    parser.add_argument("--output", default="reports/benchmarks/read_api_profile.json")
    args = parser.parse_args()

    report = run_benchmark(
        base_url=args.base_url,
        run_id=args.run_id,
        ticker=args.ticker,
        duration_seconds=args.duration_seconds,
        target_rps=args.target_rps,
        concurrency=args.concurrency,
        compose_project=args.compose_project,
    )
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0 if report["error_count"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
