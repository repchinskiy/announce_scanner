#!/usr/bin/env python3
"""Test multiple WS endpoint variants for Binance announcements."""

import asyncio
import json
import os
import sys
import time

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

import websockets

API_KEY = os.environ.get("BINANCE_API_KEY", "")

VARIANTS = [
    # 1) Example endpoint, raw stream (no subscribe needed in single-stream mode)
    {
        "name": "stream/!announcements (raw)",
        "url": "wss://stream.binance.com:9443/ws/!announcements@arr",
        "headers": {},
        "subscribe": None,
    },
    # 2) Example endpoint + send SUBSCRIBE after connect
    {
        "name": "stream/!announcements (subscribe after connect)",
        "url": "wss://stream.binance.com:9443/ws",
        "headers": {},
        "subscribe": {"method": "SUBSCRIBE", "params": ["!announcements@arr"], "id": 1},
    },
    # 3) Combined streams endpoint
    {
        "name": "combined streams: !announcements",
        "url": "wss://stream.binance.com:9443/stream?streams=!announcements@arr",
        "headers": {},
        "subscribe": None,
    },
    # 4) Try without @arr suffix
    {
        "name": "stream/!announcements (no @arr)",
        "url": "wss://stream.binance.com:9443/ws/!announcements",
        "headers": {},
        "subscribe": None,
    },
    # 5) Try listing-related stream names
    {
        "name": "stream/!listing",
        "url": "wss://stream.binance.com:9443/ws/!listing@arr",
        "headers": {},
        "subscribe": None,
    },
]


async def test_variant(variant: dict, listen_secs: int = 20):
    name = variant["name"]
    url = variant["url"]
    headers = variant.get("headers") or {}
    subscribe_msg = variant.get("subscribe")

    log.info("--- %s ---", name)
    log.info("  url: %s", url)

    try:
        async with websockets.connect(
            url,
            additional_headers=headers,
            ping_interval=10,
            ping_timeout=5,
            max_size=None,
            open_timeout=10,
        ) as ws:
            log.info("  connected!")

            if subscribe_msg:
                payload = json.dumps(subscribe_msg)
                await ws.send(payload)
                log.info("  sent: %s", payload)
                # Try to read a response
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
                    log.info("  response: %s", raw[:300])
                except asyncio.TimeoutError:
                    log.info("  no response to subscribe")

            deadline = time.time() + listen_secs
            msg_count = 0
            while time.time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
                    msg_count += 1
                    log.info("  [#%d] (%d bytes) %s", msg_count, len(raw), raw[:300])
                except asyncio.TimeoutError:
                    pass

            log.info("  done: %d messages in %ds", msg_count, listen_secs)

    except websockets.exceptions.WebSocketException as e:
        log.error("  WS ERROR: %s: %s", type(e).__name__, e)
    except Exception as e:
        log.error("  ERROR: %s: %s", type(e).__name__, e)


async def main():
    for v in VARIANTS:
        await test_variant(v, listen_secs=20)
        await asyncio.sleep(1)


if __name__ == "__main__":
    from log_setup import setup_logging
    log = setup_logging()
    asyncio.run(main())
