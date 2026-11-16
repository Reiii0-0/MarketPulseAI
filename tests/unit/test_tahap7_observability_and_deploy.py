from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]
from fastapi.testclient import TestClient

from api import main as api_main


def test_grafana_datasource_alerts_and_dashboard_uids_align() -> None:
    ds_path = Path("monitoring/grafana/provisioning/datasources/postgres.yml")
    alerts_path = Path("monitoring/grafana/provisioning/alerting/alert_rules.yml")
    dash_path = Path("monitoring/grafana/provisioning/dashboards/marketpulse.json")

    ds_cfg = yaml.safe_load(ds_path.read_text(encoding="utf-8"))
    ds = ds_cfg["datasources"][0]
    uid = ds["uid"]
    assert uid == "marketpulse_postgres_db"
    assert "$__env{POSTGRES_PASSWORD}" in ds["secureJsonData"]["password"]

    alerts_cfg = yaml.safe_load(alerts_path.read_text(encoding="utf-8"))
    rules = alerts_cfg["groups"][0]["rules"]
    assert len(rules) >= 5

    rule_uids = {r["uid"] for r in rules}
    assert {
        "live_source_freshness_stale",
        "worker_backlog_or_error",
        "ingestion_quarantine_spike",
        "worker_compute_latency_high",
        "low_signaled_accuracy_real_live",
    }.issubset(rule_uids)

    for rule in rules:
        assert rule["condition"] == "B"
        sql_items = [item for item in rule["data"] if item["refId"] == "A"]
        expr_items = [item for item in rule["data"] if item["refId"] == "B"]
        assert len(sql_items) == 1
        assert len(expr_items) == 1
        assert sql_items[0]["datasourceUid"] == uid
        assert expr_items[0]["datasourceUid"] == "__expr__"
        assert expr_items[0]["model"]["type"] == "threshold"
        # Replay event clock must never be compared to NOW() inside alert rules
        assert "event_timestamp" not in sql_items[0]["model"]["rawSql"]

    dash = json.loads(dash_path.read_text(encoding="utf-8"))
    for var in dash["templating"]["list"]:
        assert var["datasource"]["uid"] == uid
    for panel in dash["panels"]:
        assert panel["datasource"]["uid"] == uid
        for target in panel["targets"]:
            assert target["datasource"]["uid"] == uid
            raw_sql = target["rawSql"]
            assert "window_start" not in raw_sql
            assert "avg_sentiment_score" not in raw_sql
            assert "marts.predictions" not in raw_sql


def test_compose_network_isolation_and_public_tls_overlay() -> None:
    class _ComposeLoader(yaml.SafeLoader):  # type: ignore[misc]
        pass

    _ComposeLoader.add_constructor(
        "!reset",
        lambda loader, node: (
            loader.construct_sequence(node) if isinstance(node, yaml.SequenceNode) else None
        ),
    )

    base = yaml.safe_load(Path("docker-compose.yml").read_text(encoding="utf-8"))
    tls = yaml.load(
        Path("deploy/docker-compose.demo-tls.yml").read_text(encoding="utf-8"),
        Loader=_ComposeLoader,
    )

    assert base["networks"]["internal_net"]["internal"] is True
    for internal_service in ("postgres", "kafka", "bar-consumer", "prediction-worker"):
        assert base["services"][internal_service]["networks"] == ["internal_net"]
    assert "internal_net" in base["services"]["api"]["networks"]
    assert "edge_net" in base["services"]["api"]["networks"]

    assert tls["services"]["api"]["environment"]["API_READONLY_DEMO"] == "true"
    assert "edge_net" in tls["services"]["caddy-tls"]["networks"]
    assert tls["services"]["grafana"]["networks"] == ["internal_net"]

    caddyfile = Path("deploy/Caddyfile").read_text(encoding="utf-8")
    caddy_tokens = set(caddyfile.split())
    assert "not method GET HEAD OPTIONS" in caddyfile
    assert "/api/v1/predict" not in caddy_tokens
    assert "/api/v1/predict*" not in caddy_tokens
    assert "/api/v1/predict/batch" not in caddy_tokens
    assert "/api/v1/predictions*" in caddy_tokens


def test_migrations_are_additive_and_idempotent() -> None:
    migrations = sorted(Path("scripts/migrations").glob("*.sql"))
    assert len(migrations) >= 3
    for path in migrations:
        sql = path.read_text(encoding="utf-8")
        assert not re.search(r"\bDROP\s+TABLE\b", sql, re.IGNORECASE)
        assert not re.search(r"\bTRUNCATE\b", sql, re.IGNORECASE)
        for stmt in re.findall(r"CREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS)", sql, re.IGNORECASE):
            raise AssertionError(f"Non-idempotent CREATE TABLE in {path}: {stmt}")


def test_readonly_demo_middleware_blocks_mutations_and_allows_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(api_main.config, "api_readonly_demo", True)
    client = TestClient(api_main.app)

    post_resp = client.post("/api/v1/predict", json={"ticker": "AAPL"})
    assert post_resp.status_code == 403
    assert post_resp.json()["error"] == "READONLY_DEMO_MODE"

    batch_resp = client.post("/api/v1/predict/batch", json={"requests": []})
    assert batch_resp.status_code == 403
    assert batch_resp.json()["error"] == "READONLY_DEMO_MODE"

    health_resp = client.get("/health")
    assert health_resp.status_code == 200
    root_resp = client.get("/")
    assert root_resp.status_code == 200
