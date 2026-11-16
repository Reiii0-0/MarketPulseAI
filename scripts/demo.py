"""Local Compose bootstrap with preserved overrides and measured readiness phases."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import socket
import subprocess
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PORTS = {
    "postgres": ("POSTGRES_HOST_PORT", 5432),
    "kafka": ("KAFKA_HOST_PORT", 9092),
    "api": ("API_HOST_PORT", 8000),
    "grafana": ("GRAFANA_HOST_PORT", 3000),
}


def free_port(preferred: int, reserved: set[int]) -> int:
    for candidate in range(preferred, min(preferred + 500, 65536)):
        if candidate in reserved:
            continue
        with socket.socket() as sock:
            try:
                sock.bind(("127.0.0.1", candidate))
            except OSError:
                continue
        return candidate
    raise RuntimeError(f"No available loopback port near {preferred}")


def get_json(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=5) as response:
        result: dict[str, Any] = json.load(response)
        return result


def has_ready_predictions(ready: dict[str, Any], run_detail: dict[str, Any]) -> bool:
    return (
        ready.get("ready") is True
        and int(run_detail.get("run", {}).get("predictions_written", 0)) > 0
    )


def run_demo(
    *, build: bool = True, output: Path = Path("reports/verification/demo_bootstrap.json")
) -> dict[str, Any]:
    started = time.perf_counter()
    env = dict(os.environ)
    if not Path(".env").exists():
        shutil.copyfile(".env.example", ".env")
        logging.info("[make demo] Created .env from .env.example")
    if not shutil.which("docker"):
        raise RuntimeError("Install Docker Engine/Desktop and Docker Compose, then retry.")

    def compose(
        *args: str, capture: bool = False, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        proc = subprocess.run(
            ["docker", "compose", *args],
            env=env,
            text=True,
            capture_output=capture,
            timeout=900,
            check=False,
        )
        if check and proc.returncode:
            # Never print captured Compose config: it contains resolved credentials.
            raise RuntimeError(
                f"docker compose {args[0]} failed (exit {proc.returncode}); inspect service logs."
            )
        return proc

    compose("version", capture=True)
    cfg = json.loads(compose("config", "--format", "json", capture=True).stdout)
    if "caddy-tls" in cfg["services"]:
        raise RuntimeError(
            "make demo is a local bootstrap; use the TLS deployment overlay separately."
        )
    reserved: set[int] = set()
    selected: dict[str, int] = {}
    for service, (var, container_port) in PORTS.items():
        entries = cfg["services"][service].get("ports", [])
        preferred = int(next(x["published"] for x in entries if x["target"] == container_port))
        current = compose("port", service, str(container_port), capture=True, check=False)
        actual = current.stdout.strip().rsplit(":", 1)[-1]
        port = (
            int(actual)
            if current.returncode == 0 and actual.isdigit()
            else free_port(preferred, reserved)
        )
        reserved.add(port)
        selected[service] = port
        env[var] = str(port)
        if port != preferred:
            logging.info(f"[make demo] {var}={port} (requested {preferred})")
    # Compose resolves shell > .env itself. Do not execute or source a dotenv file.
    producer_env = cfg["services"]["producer"]["environment"]
    run_id = str(producer_env["REPLAY_RUN_ID"])
    speed = str(producer_env["REPLAY_SPEED_MULTIPLIER"])
    api_url = f"http://127.0.0.1:{selected['api']}"
    phases: dict[str, float] = {}

    def phase(name: str, action: Any) -> None:
        t0 = time.perf_counter()
        logging.info(f"[make demo] {name}")
        action()
        phases[name] = round(time.perf_counter() - t0, 3)

    def ensure_bundle() -> None:
        valid = subprocess.run(
            [
                "uv",
                "run",
                "--locked",
                "python",
                "-c",
                "from model.predictor import Predictor; Predictor.load('model/artefacts', enforce_contract=True)",
            ],
            capture_output=True,
            timeout=120,
            check=False,
        )
        if (
            valid.returncode
            or not Path("reports/evaluation/fixture_smoke_v1_evaluation.json").exists()
        ):
            subprocess.run(
                [
                    "uv",
                    "run",
                    "--locked",
                    "python",
                    "-m",
                    "model.evaluate",
                    "--csv",
                    "data/fixtures/smoke_1m_v1.csv",
                    "--manifest",
                    "data/manifests/fixture_smoke_v1.json",
                    "--output-dir",
                    "reports/evaluation",
                    "--bundle-dir",
                    "model/artefacts",
                ],
                check=True,
            )

    if not shutil.which("uv"):
        raise RuntimeError("Install uv to verify/generate the demo model bundle, then retry.")
    phase("bundle_prepare", ensure_bundle)
    if build:
        phase(
            "image_build", lambda: compose("build", "api", "bar-consumer", "producer", "dbt-runner")
        )
    runtime_start = time.perf_counter()
    phase(
        "infrastructure_ready",
        lambda: compose(
            "up", "-d", "--no-build", "--wait", "--wait-timeout", "120", "postgres", "kafka"
        ),
    )
    phase("topic_init", lambda: compose("run", "--rm", "--no-deps", "init-kafka"))
    phase(
        "migration",
        lambda: compose(
            "exec",
            "-T",
            "postgres",
            "sh",
            "-ec",
            'for f in /migrations/*.sql; do [ -f "$f" ] || continue; psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$f" >/dev/null; done',
        ),
    )
    phase(
        "services_start",
        lambda: compose(
            "up",
            "-d",
            "--no-build",
            "bar-consumer",
            "dbt-runner",
            "api",
            "prediction-worker",
            "grafana",
        ),
    )
    phase("replay_start", lambda: compose("up", "-d", "--no-build", "producer"))

    def await_predictions() -> None:
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            try:
                ready = get_json(f"{api_url}/health/ready")
                run = get_json(f"{api_url}/api/v1/runs/{run_id}")
                if has_ready_predictions(ready, run):
                    return
            except (OSError, ValueError):
                pass
            time.sleep(2)
        raise RuntimeError(
            f"No ready predictions for run {run_id}; inspect producer, bar-consumer, prediction-worker and api logs."
        )

    phase("predictions_ready", await_predictions)
    ready_seconds = time.perf_counter() - runtime_start
    phase(
        "dbt_verify",
        lambda: compose(
            "exec",
            "-T",
            "dbt-runner",
            "sh",
            "-ec",
            "cd /usr/app/dbt_project; dbt run --profiles-dir .; dbt test --profiles-dir .",
        ),
    )
    report = {
        "measured_at": datetime.now(UTC).isoformat(),
        "project": cfg["name"],
        "run_id": run_id,
        "replay_speed_multiplier": float(speed),
        "build_requested": build,
        "phases_seconds": phases,
        "runtime_to_predictions_seconds": round(ready_seconds, 3),
        "total_seconds": round(time.perf_counter() - started, 3),
        "ports": selected,
        "dashboard_url": api_url,
        "model_bundle": Path("model/artefacts/current").resolve().name,
        "images": compose("images", "--format", "json", capture=True).stdout.strip(),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n")
    logging.info(f"[make demo] Ready: {api_url}/ (run {run_id}); evidence: {output}")
    return report


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--no-build", action="store_true", help="Reuse existing images for measured warm startup"
    )
    parser.add_argument(
        "--output", type=Path, default=Path("reports/verification/demo_bootstrap.json")
    )
    args = parser.parse_args()
    try:
        run_demo(build=not args.no_build, output=args.output)
    except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
        logging.info(f"[make demo] ERROR: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
