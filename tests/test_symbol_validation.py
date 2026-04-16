"""Tests for symbol regex validation on API routes (prevents injection/weird input)."""

import importlib

import pytest


@pytest.fixture
def client(monkeypatch):
    monkeypatch.delenv("CRYPTO_API_KEY_HASH", raising=False)
    monkeypatch.setenv("RATE_LIMIT_DEFAULT", "10000/minute")
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


@pytest.mark.parametrize("bad", [
    "'; DROP TABLE x;--",
    "../../../etc/passwd",
    "BTC USD",           # space
    "<script>",
    "btc/usd",           # lowercase
    "A/B",               # too short
    "TOOLONG/USDTEXTRA", # too long
    "\x00",              # null byte
])
def test_invalid_query_symbol_rejected(client, bad):
    resp = client.get("/api/v1/prices", params={"symbol": bad})
    assert resp.status_code == 422, f"expected 422 for {bad!r}, got {resp.status_code}"


@pytest.mark.parametrize("bad", [
    "../../../etc",
    "not a symbol",
    "btc/usd",  # lowercase
    ";",
])
def test_invalid_path_symbol_rejected(client, bad):
    # URL-encode so it actually reaches the path param
    from urllib.parse import quote
    resp = client.get(f"/api/v1/prices/{quote(bad, safe='')}")
    assert resp.status_code in (404, 422), f"expected 404/422 for {bad!r}, got {resp.status_code}"


@pytest.mark.parametrize("good", ["BTC/USD", "ETH/USDT", "BNB/USD", "XRP/USD"])
def test_valid_query_symbol_accepted(client, good):
    resp = client.get("/api/v1/prices", params={"symbol": good})
    # Auth off, rate limit high → any non-422 status is acceptable
    assert resp.status_code != 422, f"valid {good!r} was rejected"
