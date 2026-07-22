#!/usr/bin/env python3
"""
announce_ws_client.py — minimal Binance Announcements WebSocket client
for the announce_scanner project.

Monitors https://www.binance.com/en/support/announcement/list/48
(New Cryptocurrency Listing, catalogId=48) plus optionally other categories
(49, 50, 51, 93, 128, 157, 161) via the official Binance Announcements
WebSocket API.

Project: announce_scanner

================================================================
DATA SOURCE — OFFICIAL BINANCE ANNOUNCEMENTS WEBSOCKET
================================================================
Base URL:  wss://api.binance.com/sapi/wss
Connection URL (signed):
  wss://api.binance.com/sapi/wss?random=<r>&topic=<t>&recvWindow=<rw>&timestamp=<ts>&signature=<sig>

Topic for English announcements (the only one we need):
  com_announcement_en
  -> when Binance publishes an announcement in English, subscribed clients
     receive a push with payload.data containing:
       {
         "catalogId":   <int>,            # 48 = New Cryptocurrency Listing (== list/48)
         "catalogName": "<string>",       # e.g. "New Cryptocurrency Listing"
         "publishDate": <int epoch ms>,   # == publish_ts_ms (binance publish time)
         "title":       "<string>",       # e.g. "Binance Will List ..."
         "body":        "<string>",       # announcement body (may be HTML)
         "disclaimer":  "<string>"
       }

The `catalogId` field in each message lets us filter for catalogId=48
("New Cryptocurrency Listing") -> exactly the entries shown on list/48.
This is a TRUE real-time push stream (not a poll), so it is the fastest
free source for detecting new listings. Our target: <150 ms, ideal <100 ms
from Binance publish time to our detection.

================================================================
OFFICIAL DOCUMENTATION (verified 2026-07-20)
================================================================
- WebSocket API Basic Information:
    https://developers.binance.com/en/docs/products/announcements/general-info
    Base URL wss://api.binance.com/sapi/wss, PING every 30s, 5 msg/sec limit,
    connection TTL 24h, signature = HMAC-SHA256 over params sorted
    alphabetically (excluding `signature`), API key in X-MBX-APIKEY header.
- Announcements topic + payload schema:
    https://developers.binance.com/en/docs/products/announcements/announcement
    Topic: com_announcement_en
    Payload data fields: catalogId, catalogName, publishDate, title, body, disclaimer
- Changelog (feature added 2025-07-21):
    https://developers.binance.com/en/docs/products/announcements/cms-log

================================================================
PARAMETERS / AUTH
================================================================
- random     : random string (<=32 chars); ensures signature freshness.
- topic      : one or multiple topics separated by `|` (we use com_announcement_en).
- recvWindow : latency tolerance in ms, max 60000.
- timestamp  : client epoch ms when request is built.
- signature  : HMAC-SHA256(secret, "random=...&topic=...&recvWindow=...&timestamp=...")
               NOTE: docs prose says "sorted alphabetically", but the worked
               example only matches when params are concatenated in the
               literal order random, topic, recvWindow, timestamp.
               Verified against the docs' expected HMAC on 2026-07-20.
- X-MBX-APIKEY header carries the API key.

================================================================
FALLBACK / LATENCY-COMPARISON CHANNEL — CMS REST (NOT primary)
================================================================
The list/48 page itself is rendered from a CMS REST endpoint (observed in
the browser network tab on https://www.binance.com/en/support/announcement/list/48).
We keep it documented here as a fallback and as an independent channel for
latency comparison against the WebSocket push above. It is NOT used in this
file's hot path; a separate poller can be built on top of it later.

Endpoint (GET, public, JSON):
  https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query
      ?type=1&pageNo=1&pageSize=N&catalogId=48
  - catalogId=48 -> "New Cryptocurrency Listing" (== list/48)
  - type=1       -> article list query
  - pageSize N   -> number of articles per page (page typically returns the
                    newest N articles sorted by releaseDate desc). Observed
                    that Cloudflare edge-caches by (catalogId,pageNo,pageSize);
                    very small pageSize (e.g. 1) often yields a slower
                    cache-miss response (~700 ms), while pageSize 5/10/50 hit
                    the edge cache (~110 ms from Europe). On the Tokyo VPS
                    these numbers should be lower; a parallel-poll strategy
                    across multiple pageSize/pageNo values can sometimes
                    hit the origin (cache miss) and surface a new article
                    faster than the cached path.

Response shape (verified live 2026-07-20):
  {
    "code": "000000",
    "message": null,
    "messageDetail": null,
    "success": true,
    "data": {
      "catalogs": [
        {
          "catalogId": 48,
          "parentCatalogId": null,
          "icon": "<url>",
          "catalogName": "New Cryptocurrency Listing",
          "description": null,
          "catalogType": <int>,
          "total": <int>,            # e.g. 2211 existing listing articles
          "articles": [
            {
              "id": <int>,           # unique article id (use for dedup)
              "code": "<hex>",        # article code used in the article URL
              "title": "<string>",    # e.g. "Binance Will List Aerodrome (AERO) ..."
              "type": <int>,
              "releaseDate": <int>   # publish timestamp in ms (publish_ts_ms)
            },
            ...
          ],
          "catalogs": []
        }
      ]
    }
  }

Mapping to list/48: catalogId=48 IS the list/48 category; each `articles[]`
entry corresponds to one row on the page. `releaseDate` == publish_ts_ms,
which we compare to the WS DATA frame's `publishDate` for latency measurement.

================================================================
LATENCY / HOT PATH
================================================================
Hot path per inbound WS frame: receive -> json parse -> extract catalogId ->
compute latency_ms = received_ts_ms - publishDate -> print raw JSON line.
No DB, no LLM, no extra network. PING is sent on a 30s timer to keep the
connection alive without violating the 5 msg/sec limit.

================================================================
DEPENDENCIES
================================================================
    pip install websockets

`websockets` is chosen over aiohttp here because it is lighter and gives
direct control over PING frames and reconnection.

================================================================
ENVIRONMENT
================================================================
Required:
    BINANCE_API_KEY     — your Binance API key
    BINANCE_API_SECRET  — your Binance API secret
Optional:
    ANN_TOPIC             (default com_announcement_en)
    ANN_RECV_WINDOW_MS    (default 30000)
    ANN_CATALOG_IDS       (default 48; comma-separated, e.g. 48,49,50,51,93,128,157,161)
                          Set to 0 to print ALL announcement categories.
                          Legacy single-value env: ANN_FILTER_CATALOG_ID (still honored if set).

The API key must have no trading permissions — read-only is enough for the
announcements stream. Create one at https://www.binance.com/en/my/settings/api-management.

================================================================
RUN (AWS Tokyo Ubuntu VPS, Python 3.11+)
================================================================
    pip install websockets python-dotenv
    # Put credentials in .env (see .env.example), or export as env vars.
    python3 announce_ws_client.py
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import secrets
import sys
import time

# Load .env from the script's directory if python-dotenv is available.
# Falls back silently to plain env vars if not installed.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

from log_setup import get_logger
log = get_logger()

import websockets
from websockets.exceptions import ConnectionClosed

# Local import: shared Telegram notifier (fire-and-forget, no-op if unconfigured).
from notifier import notifier

# websockets API changed across versions: <14 used `extra_headers`,
# 14+ uses `additional_headers`. Detect the supported kwarg once.
try:
    import inspect as _inspect
    _ws_connect_kwargs = set(_inspect.signature(websockets.connect).parameters.keys())
    if "additional_headers" in _ws_connect_kwargs:
        _HEADERS_KWARG = "additional_headers"
    elif "extra_headers" in _ws_connect_kwargs:
        _HEADERS_KWARG = "extra_headers"
    else:
        _HEADERS_KWARG = "additional_headers"  # let it raise clearly if wrong
except Exception:  # noqa: BLE001
    _HEADERS_KWARG = "additional_headers"


def ws_connect(url: str, headers: dict):
    """Version-agnostic websockets.connect() wrapper."""
    kwargs = {
        _HEADERS_KWARG: headers,
        "ping_interval": None,
        "ping_timeout": None,
        "close_timeout": 5,
        "open_timeout": 10,
        "max_size": None,
    }
    return websockets.connect(url, **kwargs)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "wss://api.binance.com/sapi/wss"
TOPIC = os.environ.get("ANN_TOPIC", "com_announcement_en")
RECV_WINDOW_MS = int(os.environ.get("ANN_RECV_WINDOW_MS", "30000"))

# Catalog IDs to monitor. Default: 48 (New Cryptocurrency Listing == list/48).
# Override via env: ANN_CATALOG_IDS=48,49,50,51,93,128,157,161
# Set to 0 to disable filtering (print every English announcement).
#
# Known categories:
#   48  -> New Cryptocurrency Listing
#   49  -> Latest Binance News
#   50  -> Latest Activities
#   51  -> Delistings
#   93  -> Latest Activities (alt)
#   128 -> P2P Merchant Announcements
#   157 -> Margin / Futures listings
#   161 -> Earn / Staking
def _parse_catalog_ids(raw: str) -> set[int]:
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return {int(p) for p in parts}


_raw_ids = os.environ.get("ANN_CATALOG_IDS", "48")
FILTER_CATALOG_IDS: set[int] = _parse_catalog_ids(_raw_ids)
# "0" means "monitor ALL categories" -> disable filtering entirely (empty set).
if FILTER_CATALOG_IDS == {0}:
    FILTER_CATALOG_IDS = set()
# Backward-compat: ANN_FILTER_CATALOG_ID (single value) still works if set.
_single_legacy = os.environ.get("ANN_FILTER_CATALOG_ID")
if _single_legacy:
    _legacy_set = {int(_single_legacy)}
    FILTER_CATALOG_IDS = set() if _legacy_set == {0} else _legacy_set

PING_INTERVAL_S = 30          # docs: send PING every 30s, empty payload
RECONNECT_MIN_S = 1.0
RECONNECT_MAX_S = 30.0


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------
def _sign(params: dict[str, str], secret: str) -> str:
    """HMAC-SHA256 over `k=v&k=v` joined in the FIXED Binance order:
    random, topic, recvWindow, timestamp.

    NOTE: the prose in the official docs says "sorted alphabetically", but the
    worked signature example only matches when params are concatenated in the
    literal order shown (random, topic, recvWindow, timestamp). Verified
    against the docs' expected HMAC on 2026-07-20.
    """
    order = ("random", "topic", "recvWindow", "timestamp")
    query = "&".join(f"{k}={params[k]}" for k in order)
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


def build_signed_url(api_secret: str) -> str:
    ts = str(int(time.time() * 1000))
    rand = secrets.token_hex(16)  # 32 hex chars, <=32 as recommended
    params = {
        "random": rand,
        "topic": TOPIC,
        "recvWindow": str(RECV_WINDOW_MS),
        "timestamp": ts,
    }
    sig = _sign(params, api_secret)
    # Keep URL query in the same order used for signing.
    order = ("random", "topic", "recvWindow", "timestamp")
    query = "&".join(f"{k}={params[k]}" for k in order)
    query += f"&signature={sig}"
    return f"{BASE_URL}?{query}"


# ---------------------------------------------------------------------------
# Hot path
# ---------------------------------------------------------------------------
def now_ms() -> int:
    return time.time_ns() // 1_000_000


def fmt_latency(ms_val: int | None) -> str:
    if ms_val is None:
        return "  N/A  "
    if ms_val < 100:
        return f" \033[92m{ms_val:>5}ms\033[0m"
    if ms_val < 150:
        return f" \033[93m{ms_val:>5}ms\033[0m"
    return f" \033[91m{ms_val:>5}ms\033[0m"


def handle_message(raw: str) -> None:
    """Parse one WS text frame, emit raw JSON line for new announcements.

    Hot path: keep cheap. No DB / LLM / network.
    """
    received_ts_ms = now_ms()
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        # Non-JSON frames (e.g. plain PONG) are ignored.
        return

    # Command acks (SUBSCRIBE / UNSUBSCRIBE responses) are logged, not emitted.
    if msg.get("type") == "COMMAND":
        log.info(
            "command ack subtype=%s code=%s data=%s",
            msg.get("subType"), msg.get("code"), msg.get("data"),
        )
        return

    # DATA frames carry announcement payloads.
    if msg.get("type") != "DATA":
        log.info("non-data frame: %s", raw)
        return

    topic = msg.get("topic")
    data_str = msg.get("data")
    if not isinstance(data_str, str):
        log.warning("DATA frame without string data: %s", raw)
        return

    # `data` is itself a JSON string -> unescape to get the announcement object.
    try:
        ann = json.loads(data_str)
    except json.JSONDecodeError as e:
        log.warning("announcement data JSON decode error: %r raw=%s", e, data_str[:200])
        return

    catalog_id = ann.get("catalogId")
    catalog_name = ann.get("catalogName")
    publish_ts_ms = ann.get("publishDate")

    if FILTER_CATALOG_IDS and isinstance(catalog_id, int) and catalog_id not in FILTER_CATALOG_IDS:
        # Announcement is not in one of the monitored categories; ignore.
        return

    latency_ms: int | None = None
    if isinstance(publish_ts_ms, int):
        latency_ms = received_ts_ms - publish_ts_ms

    record = {
        "received_ts_ms": received_ts_ms,
        "publish_ts_ms": publish_ts_ms,
        "latency_ms": latency_ms,
        "topic": topic,
        "catalog_id": catalog_id,
        "catalog_name": catalog_name,
        "title": ann.get("title"),
        "announcement": ann,
    }
    # Output raw JSON line for automation/logs.
    sys.stdout.write(json.dumps(record, ensure_ascii=False) + "\n")
    sys.stdout.flush()
    
    # Log the detection with high visibility.
    log.info(f"🚀 NEW [WS] latency{fmt_latency(latency_ms)} catalog={catalog_id}/{catalog_name} title={ann.get('title')!r}")

    # Telegram notification (fire-and-forget, never blocks the hot path).
    notifier.announcement(
        channel="WS",
        title=ann.get("title") or "",
        latency_ms=latency_ms,
        catalog_id=catalog_id if isinstance(catalog_id, int) else None,
        publish_ts_ms=publish_ts_ms if isinstance(publish_ts_ms, int) else None,
        recv_ts_ms=received_ts_ms,
        extra={"catalog": catalog_name} if catalog_name else None,
    )


# ---------------------------------------------------------------------------
# WS loop with PING + reconnect
# ---------------------------------------------------------------------------
async def run() -> None:
    api_key = os.environ.get("BINANCE_API_KEY")
    api_secret = os.environ.get("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        log.error("BINANCE_API_KEY / BINANCE_API_SECRET env vars are required")
        sys.exit(2)

    filter_label = ",".join(str(c) for c in sorted(FILTER_CATALOG_IDS)) if FILTER_CATALOG_IDS else "ALL"
    log.info(
        "starting announce_scanner WS client topic=%s filter_catalog=%s base=%s",
        TOPIC, filter_label, BASE_URL,
    )
    notifier.startup(
        "WS",
        f"endpoint: {BASE_URL}\n"
        f"topic: {TOPIC}\n"
        f"filter: {filter_label}",
    )

    backoff = RECONNECT_MIN_S
    while True:
        url = build_signed_url(api_secret)
        headers = {"X-MBX-APIKEY": api_key}
        try:
            log.info("connecting...")
            async with ws_connect(url, headers) as ws:
                log.info("connected; sending SUBSCRIBE")
                # Docs require an explicit SUBSCRIBE for the topic (in addition
                # to the topic param in URL). Belt-and-suspenders.
                await ws.send(json.dumps({"command": "SUBSCRIBE", "value": TOPIC}))
                backoff = RECONNECT_MIN_S

                # PING task + receive loop.
                async def ping_loop():
                    while True:
                        await asyncio.sleep(PING_INTERVAL_S)
                        try:
                            # Docs recommend empty payload PING.
                            await ws.send("")
                            log.debug("sent PING")
                        except ConnectionClosed:
                            return

                ping_task = asyncio.create_task(ping_loop())
                try:
                    async for raw in ws:
                        # Each text frame is either a DATA announcement or a
                        # COMMAND ack. PONG frames are handled internally.
                        handle_message(raw)
                finally:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except (asyncio.CancelledError, ConnectionClosed):
                        pass
        except ConnectionClosed as e:
            log.warning("ws closed: code=%s reason=%r", e.code, e.reason)
            notifier.reconnect("WS", f"code={e.code}")
        except Exception as e:  # noqa: BLE001 — keep client alive
            log.warning("ws error: %r", e)
            notifier.reconnect("WS", f"error={e!r}")

        log.info("reconnecting in %.1fs", backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, RECONNECT_MAX_S)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("stopped by user")
        notifier.shutdown("WS", "interrupted by user")
        # Give fire-and-forget notifier tasks a moment to flush.
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(asyncio.sleep(1.0))
            loop.run_until_complete(notifier.close())
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
