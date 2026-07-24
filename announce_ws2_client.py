#!/usr/bin/env python3
"""
announce_ws2_client.py — Binance !announcements@arr WebSocket client

Detects new Binance listings via the undocumented public WebSocket stream
on stream.binance.com:9443. Works alongside the official signed WS client
(announce_ws_client.py) as an independent comparison channel.

================================================================
DATA SOURCE
================================================================
Endpoint:   wss://stream.binance.com:9443/ws/!announcements@arr
Auth:       None (public stream)
Format:     Unknown — first message will reveal the schema.
            Likely an array of announcement objects (following the
            !ticker@arr / !miniTicker@arr pattern).

Behaviour confirmed 2026-07-24:
  - TCP connect succeeds
  - SUBSCRIBE {"method":"SUBSCRIBE","params":["!announcements@arr"],"id":1}
    returns {"result":null,"id":1} (success)
  - No messages received during 20s test window (expected — rare events)
  - Single-stream URL auto-subscribes, no explicit SUBSCRIBE needed

================================================================
LATENCY
================================================================
If the message contains an event timestamp (field "E" in Unix ms), we
compute latency_ms = received_ts_ms - event_ts_ms.
Otherwise only received_ts_ms is recorded.

================================================================
RECONNECT
================================================================
Exponential backoff 1-30s. PING is handled by the websockets library
automatically (the server sends PING every ~3 min).

================================================================
CHANNEL TAG
================================================================
Tagged "[WS2]" in logs and "WS2" in Telegram notifications.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from typing import Any

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

import websockets
from websockets.exceptions import ConnectionClosed

from log_setup import get_logger
from notifier import notifier

log = get_logger()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
STREAM_URL = "wss://stream.binance.com:9443/ws/!announcements@arr"

# Reconnect backoff (exponential, capped).
RECONNECT_MIN_S = 1.0
RECONNECT_MAX_S = 30.0


# ---------------------------------------------------------------------------
# Helpers
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


_first_message = True


def handle_message(raw: str) -> None:
    """Parse and log one WS frame.

    Since the exact schema is unknown, this handler is defensive:
    it extracts whatever timing info it can and prints the structure.
    """
    global _first_message
    received_ts_ms = now_ms()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        log.warning("[WS2] non-JSON frame: %r", e)
        return

    # Try to extract event timestamp (Binance convention: field "E" = event time ms).
    event_ts_ms: int | None = None
    if isinstance(data, dict):
        event_ts_ms = data.get("E") or data.get("eventTime") or data.get("timestamp")
    elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        event_ts_ms = data[0].get("E") or data[0].get("eventTime") or data[0].get("timestamp")

    if isinstance(event_ts_ms, (int, float)):
        event_ts_ms = int(event_ts_ms)
    else:
        event_ts_ms = None

    latency_ms: int | None = None
    if event_ts_ms is not None:
        latency_ms = received_ts_ms - event_ts_ms

    # Log first message structure for debugging.
    if _first_message:
        _first_message = False
        log.info("[WS2] first message structure (%d bytes): %s",
                 len(raw), json.dumps(data, ensure_ascii=False)[:2000])
        log.info("[WS2] event_ts_ms=%s  latency_ms=%s", event_ts_ms, latency_ms)

    record: dict[str, Any] = {
        "received_ts_ms": received_ts_ms,
        "event_ts_ms": event_ts_ms,
        "latency_ms": latency_ms,
        "data": data,
    }

    # Print raw JSON line (for automation/logs).
    sys.stdout.write(json.dumps(record, ensure_ascii=False) + "\n")
    sys.stdout.flush()

    log.info("[WS2] received latency%s", fmt_latency(latency_ms))

    # Fire-and-forget Telegram notification.
    title = None
    if isinstance(data, dict):
        title = data.get("title") or data.get("heading") or data.get("data", {}).get("title") if isinstance(data.get("data"), dict) else None
    elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        title = data[0].get("title") or data[0].get("heading")

    notifier.announcement(
        channel="WS2",
        title=title or "(unknown format — see logs)",
        latency_ms=latency_ms,
        catalog_id=None,
        publish_ts_ms=event_ts_ms,
        recv_ts_ms=received_ts_ms,
    )


# ---------------------------------------------------------------------------
# WS loop with reconnect
# ---------------------------------------------------------------------------
async def run() -> None:
    log.info("[WS2] starting  stream=%s", STREAM_URL)
    notifier.startup("WS2", f"endpoint: {STREAM_URL}")

    backoff = RECONNECT_MIN_S
    while True:
        try:
            log.info("[WS2] connecting...")
            async with websockets.connect(
                STREAM_URL,
                ping_interval=None,  # server controls PING
                ping_timeout=None,
                close_timeout=5,
                open_timeout=10,
                max_size=None,
            ) as ws:
                log.info("[WS2] connected")
                backoff = RECONNECT_MIN_S

                # Receive loop. PING/PONG handled by library.
                async for raw in ws:
                    handle_message(raw)

        except ConnectionClosed as e:
            log.warning("[WS2] closed: code=%s reason=%r", e.code, e.reason)
            notifier.reconnect("WS2", f"code={e.code}")
        except Exception as e:  # noqa: BLE001
            log.warning("[WS2] error: %r", e)
            notifier.reconnect("WS2", f"error={e!r}")

        log.info("[WS2] reconnecting in %.1fs", backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, RECONNECT_MAX_S)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("[WS2] stopped by user")
        notifier.shutdown("WS2", "interrupted by user")
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(asyncio.sleep(1.0))
            loop.run_until_complete(notifier.close())
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
