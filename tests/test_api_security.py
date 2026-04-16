"""Security-behavior tests for FastAPI app: CORS allowlist, error sanitization, host binding."""

import pytest


@pytest.fixture
def client(monkeypatch, tmp_path):
    # Keep ServingConfig defaults stable regardless of env
    monkeypatch.delenv("API_HOST", raising=False)
    monkeypatch.delenv("DASHBOARD_HOST", raising=False)
    monkeypatch.delenv("CORS_ALLOWED_ORIGINS", raising=False)
    # Reload modules so class-level defaults pick up env changes
    import importlib
    import src.serving.config as cfg

    importlib.reload(cfg)
    import src.serving.api.main as api_main

    importlib.reload(api_main)
    from fastapi.testclient import TestClient

    # raise_server_exceptions=False so we see the real HTTP response
    # for unhandled exceptions (matches production behavior).
    return TestClient(api_main.app, raise_server_exceptions=False)


def test_api_host_defaults_to_loopback(monkeypatch):
    monkeypatch.delenv("API_HOST", raising=False)
    import importlib
    import src.serving.config as cfg

    importlib.reload(cfg)
    assert cfg.ServingConfig.API_HOST == "127.0.0.1"


def test_dashboard_host_defaults_to_loopback(monkeypatch):
    monkeypatch.delenv("DASHBOARD_HOST", raising=False)
    import importlib
    import src.serving.config as cfg

    importlib.reload(cfg)
    assert cfg.ServingConfig.DASHBOARD_HOST == "127.0.0.1"


def test_cors_reflects_allowed_origin(client):
    resp = client.options(
        "/api/v1/health/live",
        headers={
            "Origin": "http://localhost:8501",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert resp.headers.get("access-control-allow-origin") == "http://localhost:8501"


def test_cors_does_not_reflect_disallowed_origin(client):
    resp = client.options(
        "/api/v1/health/live",
        headers={
            "Origin": "http://evil.example.com",
            "Access-Control-Request-Method": "GET",
        },
    )
    # Either absent or not the attacker's origin
    allow = resp.headers.get("access-control-allow-origin")
    assert allow != "http://evil.example.com"
    assert allow != "*"


def test_unhandled_exception_returns_generic_message(client):
    # Register a test route that raises a revealing error
    from src.serving.api import main as api_main

    @api_main.app.get("/__boom_test__")
    async def boom():
        raise ValueError("secret internal path: /etc/passwd")

    resp = client.get("/__boom_test__")
    assert resp.status_code == 500
    body = resp.json()
    # Must NOT leak the original exception text
    assert "secret internal path" not in (body.get("detail") or "")
    assert "/etc/passwd" not in (body.get("detail") or "")
    # Should have a generic message
    assert body.get("detail", "").lower() in {"internal server error", "internal error"}


def test_reload_flag_not_in_entrypoint():
    """Production entrypoint must not unconditionally pass reload=True."""
    from pathlib import Path
    import re

    main_py = Path("src/serving/api/main.py").read_text()
    # Find uvicorn.run(...) block
    match = re.search(r"uvicorn\.run\s*\([^)]*\)", main_py, re.DOTALL)
    assert match, "expected uvicorn.run(...) in main.py"
    block = match.group(0)
    # reload=True is only allowed if gated on an env var
    if "reload=True" in block:
        pytest.fail("uvicorn.run uses unconditional reload=True")
