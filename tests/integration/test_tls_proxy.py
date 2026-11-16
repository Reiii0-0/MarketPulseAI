"""Opt-in verification against a running TLS overlay, including certificate trust."""

import json
import os
import ssl
import urllib.error
import urllib.request

import pytest


@pytest.mark.compose
@pytest.mark.skipif(os.getenv("RUN_TLS_E2E") != "1", reason="Requires a running TLS demo overlay")
@pytest.mark.parametrize(
    "method,path,expected",
    [
        ("GET", "/health/ready", 200),
        ("GET", "/", 200),
        ("GET", "/api/v1/runs?limit=10", 200),
        ("POST", "/api/v1/predict", 403),
        ("POST", "/api/v1/predictions", 403),
        ("DELETE", "/api/v1/runs/unused", 403),
        ("GET", "/docs", 404),
    ],
)
def test_tls_demo_routes(method: str, path: str, expected: int) -> None:
    base = os.getenv("MARKETPULSE_TLS_BASE_URL", "https://localhost:18443")
    context = ssl.create_default_context(cafile=os.getenv("MARKETPULSE_TLS_CA"))
    request = urllib.request.Request(
        base + path, method=method, data=b"{}" if method == "POST" else None
    )
    try:
        with urllib.request.urlopen(request, context=context, timeout=10) as response:
            status, body = response.status, response.read()
    except urllib.error.HTTPError as exc:
        status, body = exc.code, exc.read()
    assert status == expected
    if path == "/health/ready":
        assert json.loads(body)["ready"] is True
