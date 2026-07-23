#!/usr/bin/env python3
"""
http_client.py — unified HTTP client abstraction for announce_scanner

Wraps aiohttp and curl_cffi behind a common interface, allowing backend
switching via HTTP_BACKEND env var (default: aiohttp).

Usage:
    from http_client import create_http_client

    async with create_http_client(timeout=5.0) as client:
        resp = await client.get("https://example.com", headers={...})
        print(resp.status_code, resp.body, resp.headers.get("X-Cache"))
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Unified types
# ---------------------------------------------------------------------------

@dataclass
class HttpResponse:
    """Unified HTTP response carrying pre-parsed JSON body and headers."""

    status_code: int
    headers: dict[str, str]
    body: Any = None
    elapsed_ms: int = 0

    def header(self, name: str, default: str | None = None) -> str | None:
        """Case-insensitive header lookup."""
        for k, v in self.headers.items():
            if k.lower() == name.lower():
                return v
        return default


# ---------------------------------------------------------------------------
# Cache status classifier
# ---------------------------------------------------------------------------

def classify_cache_status(headers: dict[str, str]) -> str:
    """Classify a response's cache freshness for stats purposes.

    CloudFront exposes ``X-Cache: Hit/Miss/RefreshHit/Error from cloudfront``.
    Other front-ends (GCP GLB, raw nginx) do not expose a comparable header,
    so we infer freshness from ``Cache-Control``:

      * ``no-cache`` / ``no-store`` / ``must-revalidate`` set by origin →
        response is NOT served from any cache → equivalent to "Miss" (fresh).
      * otherwise we cannot tell → "Unknown".
    """
    x_cache = headers.get("X-Cache") or headers.get("x-cache")
    if x_cache:
        return x_cache
    cc = (headers.get("Cache-Control") or "").lower()
    if "no-cache" in cc or "no-store" in cc or "must-revalidate" in cc:
        return "Direct (no CDN cache)"
    return "Unknown"


# ---------------------------------------------------------------------------
# Backend: aiohttp
# ---------------------------------------------------------------------------

class _AiohttpClient:
    """HTTP client backed by **aiohttp**."""

    def __init__(self, timeout: float = 5.0) -> None:
        import aiohttp

        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout),
        )

    async def get(self, url: str, headers: dict[str, str] | None = None) -> HttpResponse:
        start = time.time()
        async with self._session.get(url, headers=headers) as resp:
            elapsed_ms = int((time.time() - start) * 1000)
            try:
                body = await resp.json()
            except Exception:
                body = None
            return HttpResponse(
                status_code=resp.status,
                headers=dict(resp.headers),
                body=body,
                elapsed_ms=elapsed_ms,
            )

    async def close(self) -> None:
        await self._session.close()

    async def __aenter__(self) -> _AiohttpClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()


# ---------------------------------------------------------------------------
# Backend: curl_cffi
# ---------------------------------------------------------------------------

class _CurlCffiClient:
    """HTTP client backed by **curl_cffi** (TLS-fingerprint-spoofing capable)."""

    def __init__(self, timeout: float = 5.0, impersonate: str | None = None) -> None:
        from curl_cffi import AsyncSession

        if impersonate is None:
            impersonate = os.environ.get("HTTP_CURL_IMPERSONATE", "chrome146")
        self._session = AsyncSession(timeout=timeout, impersonate=impersonate)

    async def get(self, url: str, headers: dict[str, str] | None = None) -> HttpResponse:
        start = time.time()
        resp = await self._session.get(url, headers=headers)
        elapsed_ms = int((time.time() - start) * 1000)
        try:
            body = resp.json()
        except Exception:
            body = None
        return HttpResponse(
            status_code=resp.status_code,
            headers=dict(resp.headers),
            body=body,
            elapsed_ms=elapsed_ms,
        )

    async def close(self) -> None:
        await self._session.close()

    async def __aenter__(self) -> _CurlCffiClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()


# ---------------------------------------------------------------------------
# Backend registry
# ---------------------------------------------------------------------------

_BACKENDS: dict[str, type] = {
    "aiohttp": _AiohttpClient,
    "curl_cffi": _CurlCffiClient,
}


def create_http_client(
    backend: str | None = None,
    timeout: float = 5.0,
) -> Any:
    """Create an HTTP client for the selected backend.

    Parameters
    ----------
    backend:
        One of ``"aiohttp"`` (default) or ``"curl_cffi"``.
        If *None*, reads the ``HTTP_BACKEND`` environment variable
        (default ``"aiohttp"``).
    timeout:
        Request timeout in seconds (default 5.0).

    Returns
    -------
    An object implementing ``.get(url, headers=None)``,
    ``.close()``, and the async context manager protocol.
    """
    if backend is None:
        backend = os.environ.get("HTTP_BACKEND", "aiohttp")

    cls = _BACKENDS.get(backend)
    if cls is None:
        raise ValueError(
            f"Unknown HTTP backend: {backend!r}. "
            f"Available: {list(_BACKENDS)}",
        )

    return cls(timeout=timeout)
