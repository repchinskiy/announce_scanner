#!/usr/bin/env python3
"""One-shot: dump the first CryptoListing announcement message with ALL fields.

Usage:
    python cryptolisting_ws_test.py

Connects to CryptoListing WebSocket, waits for the first announcement message,
and prints its full JSON payload with all field names and types.
"""
import asyncio
import json
import os
import sys
from typing import Any

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

import websockets

# Same header detection as production code.
try:
    import inspect as _inspect
    _ws_kwargs = set(_inspect.signature(websockets.connect).parameters.keys())
    _HEADERS_KWARG = "additional_headers" if "additional_headers" in _ws_kwargs else "extra_headers"
except Exception:
    _HEADERS_KWARG = "extra_headers"

ENDPOINT = os.environ.get("CL_ENDPOINT", "wss://cryptolisting.ws")
TOKEN = os.environ.get("CL_SPEEDTRIAL_TOKEN", "") or os.environ.get("CL_FREEDELAYED_TOKEN", "")
CEX = os.environ.get("CL_CEX", "binance")

if not TOKEN:
    print("ERROR: No CL_SPEEDTRIAL_TOKEN or CL_FREEDELAYED_TOKEN set in .env")
    sys.exit(1)

URL = f"{ENDPOINT}?cex={CEX}"
HEADERS = {"X-API-Key": TOKEN}

async def main():
    async with websockets.connect(
        URL,
        **{_HEADERS_KWARG: HEADERS},
        ping_interval=None,
        ping_timeout=None,
        max_size=None,
    ) as ws:
        print(f"Connected to {URL}\n")
        async for raw in ws:
            msg: dict[str, Any] = json.loads(raw)
            mtype = msg.get("type")
            if mtype in ("announcement", "test_announcement"):
                print("=== FULL MESSAGE (pretty) ===")
                print(json.dumps(msg, indent=2, ensure_ascii=False))
                print("\n=== ALL FIELDS WITH TYPES ===")
                for k, v in sorted(msg.items()):
                    print(f"  {k}: {type(v).__name__} = {v!r}")
                return
            elif mtype == "welcome":
                print(f"welcome: {msg}")
            elif mtype == "heartbeat":
                continue
            else:
                print(f"other: type={mtype}")

if __name__ == "__main__":
    asyncio.run(main())
# welcome  tier=SpeedTrial  cex=binance  max_ips=1  expires_in=498040s
# welcome: {'absoluteMaxConnections': 20, 'allowedCex': 'binance', 'expiresInSecs': 498754, 'maxConnectionsPerIp': 3, 'maxDistinctIps': 1, 'tier': 'SpeedTrial', 'type': 'welcome'}