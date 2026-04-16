"""Tests for rate limiting on API routes."""

import importlib

import pytest


@pytest.fixture
def tight_client(monkeypatch):
    monkeypatch.setenv("RATE_LIMIT_DEFAULT", "3/minute")
    monkeypatch.delenv("CRYPTO_API_KEY_HASH", raising=False)
    import src.serving.config as cfg
    importlib.reload(cfg)
    import src.serving.api.ratelimit as rl
    importlib.reload(rl)
    import src.serving.api.routes.health as health_mod
    importlib.reload(health_mod)
    import src.serving.api.routes as routes_pkg
    importlib.reload(routes_pkg)
    import src.serving.api.main as api_main
    importlib.reload(api_main)
    from fastapi.testclient import TestClient
    return TestClient(api_main.app, raise_server_exceptions=False)


def test_health_live_not_rate_limited(tight_client):
    # probes must not be throttled
    for _ in range(10):
        resp = tight_client.get("/api/v1/health/live")
        assert resp.status_code == 200


def test_rate_limit_blocks_after_budget_exhausted(tight_client):
    path = "/api/v1/prices/symbols"
    # First 3 may succeed or error (depends on delta availability), but not 429
    codes = []
    for _ in range(3):
        codes.append(tight_client.get(path).status_code)
    # At least one of the first 3 should NOT be 429
    assert 429 not in codes, f"rate limit triggered too early: {codes}"
    # 4th request must be rate-limited
    resp = tight_client.get(path)
    assert resp.status_code == 429
