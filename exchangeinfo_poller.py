#!/usr/bin/env python3
"""
exchangeinfo_poller.py — Binance exchangeInfo poller for announce_scanner

Third independent detection channel. Watches for new trading symbols added
to Binance's SPOT exchangeInfo endpoint. Unlike the CMS channel (which is
behind CloudFront and bounded by its TTL), exchangeInfo is served from the
market-data API and updates within seconds of Binance adding a pair.

This is NOT a replacement for the announcement WS — it detects "pair added
to trading" rather than "announcement published". The two events can happen
in either order:
  - Announcement first, pair added minutes/hours later  (pre-announce listing)
  - Pair added and announcement at the same time         (direct listing)
  - Pair added BEFORE the announcement                   (stealth listing)

Two API hosts are queried in parallel per cycle because they sit behind
different CDNs and have independent caches:
  - https://api.binance.com        (CloudFront / AWS — exposes X-Cache header)
  - https://api-gcp.binance.com    (Google Cloud GLB — no X-Cache, but origin
                                    sends Cache-Control: no-cache,no-store,
                                    must-revalidate, so responses are always
                                    fresh and equivalent to a Miss)

A new symbol seen on either host is emitted.

Cache-status classification (used only for stats output):
  - "Miss from cloudfront"      — CloudFront cache miss (fresh)
  - "Hit from cloudfront"       — CloudFront cache hit (stale)
  - "RefreshHit from cloudfront" — stale + background revalidation
  - "Direct (no CDN cache)"     — no X-Cache header, but origin forbids caching
                                  via Cache-Control (e.g. GCP GLB path)
  - "Unknown"                   — 200 but no cache indicator
  - "Error"                     — request failed

Bootstrap behaviour: on first run, the current symbol set is stored as a
baseline and no symbols are emitted. After that, only symbols not present in
the previous snapshot are emitted. There is no "listed_at" timestamp in the
exchangeInfo response (Binance does not expose it), so latency for this
channel is the time between two consecutive polls (bounded by the poll
interval), not a true publish-to-detect latency.

Usage:
    python exchangeinfo_poller.py             # continuous polling
    python exchangeinfo_poller.py --oneshot   # single check, print + exit
    python exchangeinfo_poller.py --reset     # reset stored state

Config (env, optional):
    EI_POLL_INTERVAL_S           base interval between cycles (default 2.0)
    EI_HOSTS                     comma-separated API hosts (default: api.binance.com,api-gcp.binance.com)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from typing import Any

import aiohttp

# Load .env from the script's directory if python-dotenv is available.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

from notifier import notifier
from log_setup import get_logger

log = get_logger()


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
# Three API hosts polled in parallel — each behind different infrastructure:
#   - api.binance.com      : CloudFront (AWS)  — exposes X-Cache header
#   - api-gcp.binance.com  : Google Cloud GLB  — no X-Cache, origin forbids cache
#   - api4.binance.com     : direct nginx origin (no CloudFront, no GLB)
#                            Discovered in rickstaa/crypto-listings-sniper.
#                            Bypasses CloudFront edge hop and rate-limiter.
# Their caches are independent, so polling all four in parallel raises the
# chance of seeing a fresh response on at least one of them.
DEFAULT_HOSTS = ["api.binance.com", "api-gcp.binance.com", "api4.binance.com", "api2.binance.com"]
HOSTS = [h.strip() for h in os.environ.get("EI_HOSTS", ",".join(DEFAULT_HOSTS)).split(",") if h.strip()]

POLL_INTERVAL_S = float(os.environ.get("EI_POLL_INTERVAL_S", "2.0"))

# exchangeInfo has weight=20 per request. Spot API limit is 6000 weight/min.
# At 2s interval we use 20*30 = 600 weight/min per host — well within limits.
HTTP_TIMEOUT_S = 5.0

# State persistence
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(PROJECT_ROOT, "state")
STATE_FILE = os.path.join(STATE_DIR, "exchangeinfo_state.json")


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------
def load_state() -> dict[str, Any]:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(data: dict[str, Any]) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, STATE_FILE)


def reset_state() -> None:
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        log.info(f"state file removed: {STATE_FILE}")
    else:
        log.info("no state file found")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def now_ms() -> int:
    return time.time_ns() // 1_000_000


def extract_symbols(body: dict[str, Any]) -> set[str]:
    """Return the set of symbol names from an exchangeInfo response."""
    out: set[str] = set()
    for sym in body.get("symbols", []) or []:
        name = sym.get("symbol")
        if name:
            out.add(name)
    return out


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------
async def fetch_one(session: aiohttp.ClientSession, host: str) -> tuple[str, set[str], int, str]:
    """Fetch exchangeInfo from one host.

    Returns (host, symbols, recv_ms, cache_status).
    On error returns (host, set(), 0, "Error").

    cache_status classification:
      - "Miss from cloudfront"  — CloudFront X-Cache: Miss (fresh from origin)
      - "Hit from cloudfront"   — CloudFront X-Cache: Hit (cached)
      - "RefreshHit from cloudfront" — CloudFront X-Cache: RefreshHit (stale+revalidate)
      - "Direct (no CDN cache)" — no X-Cache header AND origin sent
                                  Cache-Control: no-cache/no-store (e.g. GCP GLB
                                  fronting nginx with origin-level no-cache).
                                  Equivalent to a Miss: response is fresh.
      - "Unknown"               — response 200 but no usable cache indicator.
      - "Error"                 — request failed.
    """
    url = f"https://{host}/api/v3/exchangeInfo"
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as resp:
            recv_ms = now_ms()
            if resp.status == 429:
                log.warning(f"[EXCHANGE] 429 rate-limited  host={host}")
                notifier.reconnect("EXCHANGE", f"429 rate-limited (host={host})")
                return host, set(), recv_ms, "RateLimited"
            if resp.status != 200:
                log.warning(f"[EXCHANGE] HTTP {resp.status}  host={host}")
                notifier.reconnect("EXCHANGE", f"HTTP {resp.status} (host={host})")
                return host, set(), recv_ms, "Error"
            body = await resp.json()
            cache_status = _classify_cache_status(resp)
            return host, extract_symbols(body), recv_ms, cache_status
    except Exception:
        return host, set(), 0, "Error"


def _classify_cache_status(resp: aiohttp.ClientResponse) -> str:
    """Classify a response's cache freshness for stats purposes.

    CloudFront exposes `X-Cache: Hit/Miss/RefreshHit/Error from cloudfront`.
    Other front-ends (GCP GLB, raw nginx) do not expose a comparable header,
    so we infer freshness from `Cache-Control`:
      - `no-cache` / `no-store` / `must-revalidate` set by origin → response
        is NOT served from any cache → equivalent to "Miss" (fresh).
      - otherwise we cannot tell → "Unknown".
    """
    # CloudFront — use its explicit X-Cache header verbatim.
    x_cache = resp.headers.get("X-Cache") or resp.headers.get("x-cache")
    if x_cache:
        return x_cache

    # Non-CloudFront (GCP GLB, raw nginx, etc.). Origin's Cache-Control tells
    # us whether the response is allowed to be cached anywhere.
    cc = (resp.headers.get("Cache-Control") or "").lower()
    if "no-cache" in cc or "no-store" in cc or "must-revalidate" in cc:
        return "Direct (no CDN cache)"

    return "Unknown"


async def poll_once(session: aiohttp.ClientSession,
                    known: set[str],
                    stats: dict[str, Any]) -> set[str]:
    """Poll all hosts in parallel, emit new symbols, return set of new symbols.

    A symbol is "new" if it is not in `known` and appeared on at least one host.
    """
    tasks = [fetch_one(session, h) for h in HOSTS]
    results = await asyncio.gather(*tasks)

    # Merge: union of symbols across all hosts.
    merged: set[str] = set()
    for host, symbols, recv_ms, cache_status in results:
        stats.setdefault("total", {}).setdefault(host, 0)
        stats["total"][host] += 1
        stats.setdefault("cache", {}).setdefault(host, {})
        stats["cache"][host][cache_status] = stats["cache"][host].get(cache_status, 0) + 1
        merged.update(symbols)

    # Emit new symbols
    new_symbols: set[str] = merged - known
    recv_ms_now = now_ms()
    for sym in sorted(new_symbols):
        # Determine which host(s) returned this symbol.
        src_hosts = [h for h, syms, _, _ in results if sym in syms]
        src_host = ",".join(src_hosts) if src_hosts else "?"

        log.info(
            f"🚀 NEW [EXCHANGE]  symbol={sym}  host={src_host}"
        )
        notifier.announcement(
            channel="EXCHANGE",
            title=f"New symbol: {sym}",
            latency_ms=None,  # exchangeInfo has no listed_at field
            recv_ts_ms=recv_ms_now,
            extra={"host": src_host} if src_host != "?" else None,
        )

    return new_symbols


async def run_oneshot() -> set[str]:
    """Single poll against an empty known set — emits everything seen."""
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_S)) as session:
        return await poll_once(session, known=set(), stats={})


async def run_continuous() -> None:
    state = load_state()
    known: set[str] = set(state.get("symbols", []))
    stats: dict[str, Any] = {"total": {}, "cache": {}}
    cycle = 0

    log.info(f"[EXCHANGE] announce_scanner  hosts={HOSTS}  interval={POLL_INTERVAL_S}s  "
             f"state={STATE_FILE}")
    notifier.startup(
        "EXCHANGE",
        f"endpoints:\n"
        + "".join(f"  https://{h}/api/v3/exchangeInfo\n" for h in HOSTS)
        + f"interval: {POLL_INTERVAL_S}s",
    )

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_S)) as session:
        # Bootstrap: if state is empty, seed the baseline silently (no emits).
        if not known:
            baseline = set()
            for host, symbols, _, _ in await asyncio.gather(*[fetch_one(session, h) for h in HOSTS]):
                baseline.update(symbols)
            known = baseline
            state["symbols"] = sorted(known)
            save_state(state)
            log.info(f"[EXCHANGE] bootstrap  baseline={len(known)} symbols (no emits)")
        else:
            log.info(f"[EXCHANGE] ready  known={len(known)}")

        while True:
            cycle += 1
            start = now_ms()

            new_syms = await poll_once(session, known, stats)
            if new_syms:
                known.update(new_syms)
                state["symbols"] = sorted(known)
                save_state(state)

            elapsed = now_ms() - start
            sleep_s = max(0, (POLL_INTERVAL_S * 1000) - elapsed) / 1000

            if cycle % int(os.environ.get("EI_STATS_EVERY_CYCLES", "100")) == 0:
                total = stats["total"]
                cache = stats["cache"]
                parts = []
                for h in sorted(total):
                    c = cache.get(h, {})
                    # Show every cache-status bucket seen for this host, sorted.
                    bucket_str = " ".join(
                        f"{k[:1].upper() if k else '?'}:{v}"
                        for k, v in sorted(c.items(), key=lambda kv: -kv[1])
                    )
                    parts.append(f"{h}:{total[h]}({bucket_str})")
                log.info(
                    f"[EXCHANGE] STATS  cycle={cycle}  known={len(known)}  "
                    f"[{', '.join(parts)}]"
                )

            await asyncio.sleep(sleep_s)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="exchangeInfo poller for announce_scanner")
    parser.add_argument("--oneshot", action="store_true", help="single check, print + exit")
    parser.add_argument("--reset", action="store_true", help="reset stored state")
    args = parser.parse_args()

    if args.reset:
        reset_state()
        return

    try:
        if args.oneshot:
            new_syms = asyncio.run(run_oneshot())
            if not new_syms:
                log.info("no symbols")
            else:
                for sym in sorted(new_syms):
                    log.info(f"symbol={sym}")
        else:
            asyncio.run(run_continuous())
    except KeyboardInterrupt:
        log.info("[EXCHANGE] stopped")
        notifier.shutdown("EXCHANGE", "interrupted by user")
        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(asyncio.sleep(1.0))
            loop.run_until_complete(notifier.close())
            loop.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
