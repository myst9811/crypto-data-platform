"""HMAC-signed artifact loading to prevent RCE via tampered pickle/torch files.

Usage:
    from ml.utils.safe_artifact import sign_artifact, safe_load_pickle, safe_load_torch

    # At training time, after saving:
    with open(path, "wb") as f:
        pickle.dump(model, f)
    sign_artifact(path)            # writes path.sig

    # At serving time:
    model = safe_load_pickle(path)           # raises on tamper/missing sig
    state = safe_load_torch(path, map_location="cpu")

Env vars:
    CRYPTO_MODEL_HMAC_KEY   Hex-encoded signing key. Required in prod.
                            Absent → dev mode (load unsigned with warning).
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import pickle
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

SIG_SUFFIX = ".sig"
ENV_KEY = "CRYPTO_MODEL_HMAC_KEY"


class ArtifactIntegrityError(Exception):
    """Raised when an artifact's signature is missing or does not match."""


def _get_key() -> Optional[bytes]:
    raw = os.getenv(ENV_KEY)
    if not raw:
        return None
    try:
        return bytes.fromhex(raw)
    except ValueError:
        return raw.encode("utf-8")


def _compute_hmac(path: Path, key: bytes) -> str:
    h = hmac.new(key, digestmod=hashlib.sha256)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _sig_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + SIG_SUFFIX)


def sign_artifact(path: Path) -> Path:
    """Sign an artifact, writing path.sig alongside it. Returns the sig path.

    Requires CRYPTO_MODEL_HMAC_KEY env var.
    """
    path = Path(path)
    key = _get_key()
    if key is None:
        raise ArtifactIntegrityError(
            f"{ENV_KEY} not set; cannot sign artifact"
        )
    digest = _compute_hmac(path, key)
    sig = _sig_path(path)
    sig.write_text(digest + "\n")
    return sig


def _verify_or_raise(path: Path) -> None:
    """Verify signature; raises ArtifactIntegrityError if missing/bad.

    If no key is configured, emit a warning and allow loading (dev mode).
    """
    key = _get_key()
    if key is None:
        logger.warning(
            "Loading unsigned artifact %s (dev mode — set %s in production)",
            path,
            ENV_KEY,
        )
        return

    sig = _sig_path(path)
    if not sig.exists():
        raise ArtifactIntegrityError(
            f"no signature file for {path} (expected {sig})"
        )

    expected = sig.read_text().strip()
    actual = _compute_hmac(path, key)
    if not hmac.compare_digest(expected, actual):
        raise ArtifactIntegrityError(
            f"signature mismatch for {path}"
        )


def safe_load_pickle(path: Path) -> Optional[Any]:
    """Load a pickle file after verifying its HMAC signature.

    Returns None if the file does not exist (caller decides handling).
    Raises ArtifactIntegrityError if signature check fails in prod mode.
    """
    path = Path(path)
    if not path.exists():
        return None
    _verify_or_raise(path)
    with open(path, "rb") as f:
        return pickle.load(f)


def _torch_load(path, **kwargs):
    """Indirection so tests can monkeypatch without importing torch."""
    import torch

    return torch.load(path, **kwargs)


def safe_load_torch(path: Path, **kwargs) -> Optional[Any]:
    """Load a torch file after verifying its HMAC signature.

    Always forces weights_only=True to prevent arbitrary object deserialization.
    Returns None if the file does not exist.
    """
    path = Path(path)
    if not path.exists():
        return None
    _verify_or_raise(path)
    kwargs["weights_only"] = True
    return _torch_load(path, **kwargs)


def main() -> None:
    """CLI: python -m ml.utils.safe_artifact sign <file>..."""
    import sys

    if len(sys.argv) < 3 or sys.argv[1] != "sign":
        print("usage: python -m ml.utils.safe_artifact sign <file>...", file=sys.stderr)
        sys.exit(2)
    for p in sys.argv[2:]:
        sig = sign_artifact(Path(p))
        print(f"signed {p} -> {sig}")


if __name__ == "__main__":
    main()
