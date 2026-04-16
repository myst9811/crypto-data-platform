"""Shared rate limiter for FastAPI routes.

Rate: RATE_LIMIT_DEFAULT env (default 100/minute), applied to every
route. Use `@limiter.exempt` on health/probe endpoints.
"""

import os

from slowapi import Limiter
from slowapi.util import get_remote_address

DEFAULT_LIMIT = os.getenv("RATE_LIMIT_DEFAULT", "100/minute")

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[DEFAULT_LIMIT],
    headers_enabled=True,
)
