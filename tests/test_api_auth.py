"""Tests for bearer-token auth gating on FastAPI routes."""

import hashlib
import importlib

import pytest


@pytest.fixture
def reload_app(monkeypatch):
    def _build(enable_auth: bool):
        if enable_auth:
            token = "test-token-xyz"
            monkeypatch.setenv("CRYPTO_API_KEY_HASH", hashlib.sha256(token.encode()).hexdigest())
        else:
            monkeypatch.delenv("CRYPTO_API_KEY_HASH", raising=False)
        # force clean reload for module-level env reads
        import src.serving.api.auth  # ensure imported
        importlib.reload(src.serving.api.auth)
        import src.serving.api.main as api_main
        importlib.reload(api_main)
        from fastapi.testclient import TestClient
        return TestClient(api_main.app, raise_server_exceptions=False), "test-token-xyz"
    return _build


def test_auth_disabled_without_env_key(reload_app):
    client, _ = reload_app(enable_auth=False)
    # Public paths work without any token
    resp = client.get("/api/v1/health/live")
    assert resp.status_code == 200


def test_health_live_public_even_with_auth_enabled(reload_app):
    client, _ = reload_app(enable_auth=True)
    resp = client.get("/api/v1/health/live")
    assert resp.status_code == 200


def test_protected_route_rejects_missing_token(reload_app):
    client, _ = reload_app(enable_auth=True)
    resp = client.get("/api/v1/prices")
    assert resp.status_code == 401
    assert "detail" in resp.json()


def test_protected_route_rejects_invalid_token(reload_app):
    client, _ = reload_app(enable_auth=True)
    resp = client.get(
        "/api/v1/prices",
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert resp.status_code == 401


def test_protected_route_rejects_non_bearer_scheme(reload_app):
    client, token = reload_app(enable_auth=True)
    resp = client.get(
        "/api/v1/prices",
        headers={"Authorization": f"Basic {token}"},
    )
    assert resp.status_code == 401


def test_protected_route_accepts_valid_token(reload_app):
    client, token = reload_app(enable_auth=True)
    # Use /api/v1/symbols — simpler endpoint that doesn't need live data
    resp = client.get(
        "/api/v1/prices/symbols",
        headers={"Authorization": f"Bearer {token}"},
    )
    # Auth must not return 401; any other status is acceptable
    # (the endpoint may 500 if Delta unavailable in test env — that's
    # orthogonal to auth)
    assert resp.status_code != 401
