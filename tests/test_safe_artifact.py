"""Tests for HMAC-signed artifact loading."""

import os
import pickle
import pytest
from pathlib import Path


@pytest.fixture
def hmac_env(monkeypatch):
    monkeypatch.setenv("CRYPTO_MODEL_HMAC_KEY", "a" * 64)


@pytest.fixture
def tmp_pickle(tmp_path):
    obj = {"hello": "world", "n": 42}
    path = tmp_path / "artifact.pkl"
    with open(path, "wb") as f:
        pickle.dump(obj, f)
    return path, obj


def test_sign_artifact_writes_sig_file(hmac_env, tmp_pickle):
    from ml.utils.safe_artifact import sign_artifact

    path, _ = tmp_pickle
    sig_path = sign_artifact(path)

    assert sig_path == path.with_suffix(path.suffix + ".sig")
    assert sig_path.exists()
    content = sig_path.read_text().strip()
    assert len(content) == 64  # sha256 hex
    assert all(c in "0123456789abcdef" for c in content)


def test_safe_load_pickle_accepts_valid_signature(hmac_env, tmp_pickle):
    from ml.utils.safe_artifact import sign_artifact, safe_load_pickle

    path, obj = tmp_pickle
    sign_artifact(path)

    loaded = safe_load_pickle(path)
    assert loaded == obj


def test_safe_load_pickle_rejects_missing_signature(hmac_env, tmp_pickle):
    from ml.utils.safe_artifact import safe_load_pickle, ArtifactIntegrityError

    path, _ = tmp_pickle
    # No .sig written

    with pytest.raises(ArtifactIntegrityError, match="no signature"):
        safe_load_pickle(path)


def test_safe_load_pickle_rejects_tampered_file(hmac_env, tmp_pickle):
    from ml.utils.safe_artifact import sign_artifact, safe_load_pickle, ArtifactIntegrityError

    path, _ = tmp_pickle
    sign_artifact(path)

    # Tamper with the file after signing
    with open(path, "ab") as f:
        f.write(b"\x00evil")

    with pytest.raises(ArtifactIntegrityError, match="signature mismatch"):
        safe_load_pickle(path)


def test_safe_load_pickle_rejects_tampered_signature(hmac_env, tmp_pickle):
    from ml.utils.safe_artifact import sign_artifact, safe_load_pickle, ArtifactIntegrityError

    path, _ = tmp_pickle
    sig_path = sign_artifact(path)
    sig_path.write_text("0" * 64)  # replace with bogus sig

    with pytest.raises(ArtifactIntegrityError, match="signature mismatch"):
        safe_load_pickle(path)


def test_safe_load_pickle_warns_without_key_in_dev(tmp_pickle, monkeypatch, caplog):
    # CRYPTO_MODEL_HMAC_KEY unset → dev mode: warn and load
    monkeypatch.delenv("CRYPTO_MODEL_HMAC_KEY", raising=False)
    from ml.utils.safe_artifact import safe_load_pickle

    path, obj = tmp_pickle
    with caplog.at_level("WARNING"):
        loaded = safe_load_pickle(path)
    assert loaded == obj
    assert any("unsigned" in rec.message.lower() or "dev mode" in rec.message.lower()
               for rec in caplog.records)


def test_safe_load_torch_passes_weights_only(hmac_env, tmp_path, monkeypatch):
    """safe_load_torch must call torch.load with weights_only=True."""
    from ml.utils import safe_artifact

    # Create a fake .pt file and sign it
    pt = tmp_path / "model.pt"
    pt.write_bytes(b"fake torch bytes")
    safe_artifact.sign_artifact(pt)

    captured = {}

    def fake_load(path, **kwargs):
        captured["path"] = str(path)
        captured["kwargs"] = kwargs
        return {"state": "ok"}

    monkeypatch.setattr(safe_artifact, "_torch_load", fake_load)
    result = safe_artifact.safe_load_torch(pt, map_location="cpu")

    assert result == {"state": "ok"}
    assert captured["kwargs"].get("weights_only") is True
    assert captured["kwargs"].get("map_location") == "cpu"


def test_safe_load_pickle_missing_file_returns_none(hmac_env, tmp_path):
    """Non-existent file returns None (caller decides handling)."""
    from ml.utils.safe_artifact import safe_load_pickle

    result = safe_load_pickle(tmp_path / "nonexistent.pkl")
    assert result is None
