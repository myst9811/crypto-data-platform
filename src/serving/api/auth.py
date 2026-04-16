"""Bearer-token authentication for API routes.

Enforcement is env-gated. In dev (no CRYPTO_API_KEY_HASH set) auth is
disabled with a warning on each request; in prod, every request must
carry `Authorization: Bearer <token>` whose SHA-256 matches the hash.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
from typing import Optional

from fastapi import Header, HTTPException, status

logger = logging.getLogger(__name__)

ENV_HASH = "CRYPTO_API_KEY_HASH"

_warned_dev_mode = False


def _expected_hash() -> Optional[str]:
    h = os.getenv(ENV_HASH, "").strip().lower()
    return h or None


def _sha256_hex(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


async def verify_api_key(authorization: str = Header(default="")) -> None:
    """FastAPI dependency enforcing bearer-token auth.

    Attach as a router-level dependency on every router that should be
    protected. Leave /health/live unprotected.
    """
    expected = _expected_hash()
    if expected is None:
        global _warned_dev_mode
        if not _warned_dev_mode:
            logger.warning(
                "API auth disabled: %s not set. Do not run in production.",
                ENV_HASH,
            )
            _warned_dev_mode = True
        return

    # Require `Bearer <token>`
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    actual = _sha256_hex(token)
    if not hmac.compare_digest(actual, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
