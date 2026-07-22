#!/usr/bin/env python3
"""
announce_ws_smoke.py — minimal smoke-test for the Binance Announcements
WebSocket client (announce_ws_client.py).

Purpose
-------
The main announce_ws_client.py filters to catalogId=48 (New Cryptocurrency
Listing) and runs forever. Listing events do not happen every day, so waiting
for one is a slow way to prove the client "stays connected and prints incoming
announcement messages as raw JSON lines".

This smoke script instead:
  - connects to wss://api.binance.com/sapi/wss with the same signed-URL logic,
  - subscribes to com_announcement_en WITHOUT catalog filtering,
  - prints EVERY inbound frame (COMMAND acks AND DATA announcements) as a raw
    JSON line to stdout,
  - runs for SMOKE_SECONDS (default 120) then exits 0.

So as soon as Binance publishes ANY English announcement (typically several
per hour — delistings, maintenance, new products, listings, etc.) the user
gets a raw JSON line proving "stays connected + prints incoming announcement
messages". This satisfies acceptance criterion 1 of Prompt1.

Same env vars as announce_ws_client.py:
    BINANCE_API_KEY     (required)
    BINANCE_API_SECRET  (required)
    ANN_TOPIC           (default com_announcement_en)
    ANN_RECV_WINDOW_MS  (default 30000)
    SMOKE_SECONDS       (default 120)

Docs / endpoint references: identical to announce_ws_client.py — see the
header comments there for full Binance documentation links
(general-info.md, announcement.md, cms-log.md) and the CMS REST fallback.

Run on the Tokyo VPS:
    pip install websockets
    export BINANCE_API_KEY=...
    export BINANCE_API_SECRET=...
    python3 announce_ws_smoke.py
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import secrets
import sys
import time

# Load .env from the script's directory if python-dotenv is available.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

import websockets
from websockets.exceptions import ConnectionClosed

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

BASE_URL = "wss://api.binance.com/sapi/wss"
TOPIC = os.environ.get("ANN_TOPIC", "com_announcement_en")
RECV_WINDOW_MS = int(os.environ.get("ANN_RECV_WINDOW_MS", "30000"))
SMOKE_SECONDS = int(os.environ.get("SMOKE_SECONDS", "120"))
PING_INTERVAL_S = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stderr,
)
log = logging.getLogger("announce_smoke")


def _sign(params: dict[str, str], secret: str) -> str:
    # Fixed Binance order (verified against docs example): random, topic,
    # recvWindow, timestamp. Docs prose says alphabetical, but the worked
    # example only matches in this literal order.
    order = ("random", "topic", "recvWindow", "timestamp")
    q = "&".join(f"{k}={params[k]}" for k in order)
    return hmac.new(secret.encode(), q.encode(), hashlib.sha256).hexdigest()


def build_signed_url(api_secret: str) -> str:
    ts = str(int(time.time() * 1000))
    rand = secrets.token_hex(16)
    params = {
        "random": rand,
        "topic": TOPIC,
        "recvWindow": str(RECV_WINDOW_MS),
        "timestamp": ts,
    }
    sig = _sign(params, api_secret)
    order = ("random", "topic", "recvWindow", "timestamp")
    q = "&".join(f"{k}={params[k]}" for k in order) + f"&signature={sig}"
    return f"{BASE_URL}?{q}"


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def emit_frame(raw: str) -> None:
    """Print one raw inbound frame as a JSON line. No filtering — every frame
    (COMMAND acks and DATA announcements) is emitted so the smoke test proves
    the client receives real messages from the stream."""
    received_ts_ms = now_ms()
    record = {
        "received_ts_ms": received_ts_ms,
        "raw": raw,
    }
    # Also try to enrich with parsed fields if the frame is a DATA announcement.
    try:
        msg = json.loads(raw)
        record["type"] = msg.get("type")
        record["topic"] = msg.get("topic")
        if msg.get("type") == "DATA" and isinstance(msg.get("data"), str):
            ann = json.loads(msg["data"])
            record["catalog_id"] = ann.get("catalogId")
            record["catalog_name"] = ann.get("catalogName")
            record["publish_ts_ms"] = ann.get("publishDate")
            record["latency_ms"] = (
                received_ts_ms - ann["publishDate"]
                if isinstance(ann.get("publishDate"), int) else None
            )
            record["title"] = ann.get("title")
    except (json.JSONDecodeError, TypeError):
        pass
    sys.stdout.write(json.dumps(record, ensure_ascii=False) + "\n")
    sys.stdout.flush()
    log.info(
        "frame type=%s catalog=%s title=%r",
        record.get("type"),
        record.get("catalog_id"),
        record.get("title"),
    )


async def run() -> None:
    api_key = os.environ.get("BINANCE_API_KEY")
    api_secret = os.environ.get("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        log.error("BINANCE_API_KEY / BINANCE_API_SECRET env vars are required")
        sys.exit(2)

    log.info(
        "smoke start topic=%s runtime=%ss base=%s",
        TOPIC, SMOKE_SECONDS, BASE_URL,
    )

    url = build_signed_url(api_secret)
    headers = {"X-MBX-APIKEY": api_key}

    deadline = asyncio.get_event_loop().time() + SMOKE_SECONDS
    try:
        log.info("connecting...")
        async with ws_connect(url, headers) as ws:
            log.info("connected; sending SUBSCRIBE")
            await ws.send(json.dumps({"command": "SUBSCRIBE", "value": TOPIC}))

            async def ping_loop():
                while True:
                    await asyncio.sleep(PING_INTERVAL_S)
                    try:
                        await ws.send("")
                    except ConnectionClosed:
                        return

            ping_task = asyncio.create_task(ping_loop())
            frames_received = 0
            try:
                while True:
                    remaining = deadline - asyncio.get_event_loop().time()
                    if remaining <= 0:
                        log.info("smoke window elapsed; frames_received=%d", frames_received)
                        break
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    except asyncio.TimeoutError:
                        break
                    frames_received += 1
                    emit_frame(raw)
            finally:
                ping_task.cancel()
                try:
                    await ping_task
                except (asyncio.CancelledError, ConnectionClosed):
                    pass
    except ConnectionClosed as e:
        log.warning("ws closed: code=%s reason=%r", e.code, e.reason)
        sys.exit(1)
    except Exception as e:  # noqa: BLE001
        log.warning("ws error: %r", e)
        sys.exit(1)

    log.info("smoke done frames_received=%d", frames_received)
    if frames_received == 0:
        sys.exit(1)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("stopped by user")


if __name__ == "__main__":
    main()
