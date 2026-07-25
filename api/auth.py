"""
auth.py — simple API key authentication.

Deliberately not OAuth/JWT: this is a service-to-service internal API,
not a multi-user or third-party-consumer surface, so a static key
checked via a header is the appropriately-sized solution — OAuth would
add complexity (token issuance, refresh, client registration) without
a real corresponding need here.

If API_KEY is unset, auth is disabled entirely (so local dev / first
run never breaks because of a missing key) — a warning is logged once
at import time so the gap is visible, not silent.
"""
import logging
import os

from fastapi import Header, HTTPException

log = logging.getLogger(__name__)

API_KEY = os.environ.get("API_KEY")

if not API_KEY:
    log.warning("API_KEY is not set — all endpoints are UNAUTHENTICATED. Set API_KEY to enable auth.")


def require_api_key(x_api_key: str = Header(default=None)) -> None:
    if not API_KEY:
        return  # auth disabled — see module docstring
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Missing or invalid X-API-Key header")
