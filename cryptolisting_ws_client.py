#!/usr/bin/env python3
"""
cryptolisting_client.py — CryptoListing.ws WebSocket client for announce_scanner

Fourth detection channel. Connects to CryptoListing.ws (co-located with
Binance in AWS Tokyo) and receives listing/delisting/airdrop alerts pushed
by their detector. Two free tiers can run in parallel:

  - SpeedTrial (0 ms delay, title/ticker REDACTED on listing events).
    Used as a benchmark: compare dispatchTimestampUs against our own
    announce_ws_client.py latency_ms to see whether co-location beats
    the direct Binance WS.
  - FreeDelayed (+240 ms delay, full payload). Used as an auxiliary feed.

Both tiers use the same endpoint wss://cryptolisting.ws and the same
X-API-Key header — the tier is determined by the key itself, and reported
back in the welcome frame.

Auth: API key in the `X-API-Key` HTTP header (NOT a query param).
Format: `dsk_<64 hex chars>`.

Docs: https://cryptolisting.ws/docs/book/

Usage:
    python cryptolisting_client.py             # both tiers (if both tokens set)
    python cryptolisting_client.py --tier speedtrial   # SpeedTrial only
    python cryptolisting_client.py --tier freedelayed # FreeDelayed only
    python cryptolisting_client.py --test      # send a test message on connect

Config (env, in .env):
    CL_SPEEDTRIAL_TOKEN   — dsk_... key for the SpeedTrial tier
    CL_FREEDELAYED_TOKEN  — dsk_... key for the FreeDelayed tier
    CL_CEX                — comma-separated exchanges (default: binance)
                            Valid: binance, upbit, bithumb
    CL_ENDPOINT           — wss URL (default: wss://cryptolisting.ws)
                            Seoul mirror: wss://kr.cryptolisting.ws (Upbit only)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from typing import Any

# Load .env from the script's directory if python-dotenv is available.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

import websockets
from websockets.exceptions import ConnectionClosed

from notifier import notifier
from log_setup import get_logger

log = get_logger()


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DEFAULT_ENDPOINT = "wss://cryptolisting.ws"
ENDPOINT = os.environ.get("CL_ENDPOINT", DEFAULT_ENDPOINT)
CEX = os.environ.get("CL_CEX", "binance")

SPEEDTRIAL_TOKEN = os.environ.get("CL_SPEEDTRIAL_TOKEN", "")
FREEDELAYED_TOKEN = os.environ.get("CL_FREEDELAYED_TOKEN", "")

# Reconnect backoff (exponential, capped).
RECONNECT_MIN_S = 1.0
RECONNECT_MAX_S = 300.0
MAX_RETRIES = 20

# Optional: send {"type":"test"} 15s after welcome (smoke check).
SMOKE_TEST_DELAY_S = 15.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def now_us() -> int:
    return time.time_ns() // 1_000


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


def build_url(token: str) -> str:
    """Build the WSS URL with cex filter (if any)."""
    if CEX:
        return f"{ENDPOINT}?cex={CEX}"
    return ENDPOINT


# ---------------------------------------------------------------------------
# Per-tier connection
# ---------------------------------------------------------------------------
async def run_tier(tier_name: str, token: str, send_test: bool) -> None:
    """Run one tier's WS connection with reconnect logic."""
    if not token:
        log.info(f"[{tier_name}] no token set in env; skipping")
        return

    url = build_url(token)
    headers = {"X-API-Key": token}
    tag = "CLWS" if tier_name == "SpeedTrial" else "CLWD"

    log.info(f"[{tag}] connecting  url={url}")
    notifier.startup(
        tag,
        f"endpoint: {url}\n"
        f"tier: {tier_name}\n"
        f"cex filter: {CEX or 'all'}",
    )

    backoff = RECONNECT_MIN_S
    for attempt in range(MAX_RETRIES):
        try:
            async with websockets.connect(
                url,
                additional_headers=headers,
                ping_interval=None,  # server pings every 15s; library handles PONG
                ping_timeout=None,
                close_timeout=5,
                open_timeout=10,
                max_size=None,
            ) as ws:
                log.info(f"[{tag}] connected")
                backoff = RECONNECT_MIN_S

                # Optional smoke test 15s after welcome.
                smoke_task: asyncio.Task | None = None
                if send_test:
                    async def smoke():
                        await asyncio.sleep(SMOKE_TEST_DELAY_S)
                        try:
                            await ws.send(json.dumps({"type": "test"}))
                            log.info(f"[{tag}] sent test message")
                        except ConnectionClosed:
                            pass
                    smoke_task = asyncio.create_task(smoke())

                try:
                    async for raw in ws:
                        handle_message(raw, tag, tier_name)
                finally:
                    if smoke_task:
                        smoke_task.cancel()
                        try:
                            await smoke_task
                        except (asyncio.CancelledError, ConnectionClosed):
                            pass

        except ConnectionClosed as e:
            reason = e.rcvd.reason if e.rcvd else ""
            if reason in ("key_expired", "key_invalidated"):
                log.warning(f"[{tag}] key lifecycle end: {reason}; stopping")
                notifier.shutdown(tag, f"key {reason}")
                return
            log.warning(f"[{tag}] closed: code={e.code} reason={reason!r}")
            notifier.reconnect(tag, f"code={e.code} reason={reason!r}")
        except Exception as e:  # noqa: BLE001 — keep client alive
            log.error(f"[{tag}] error: {e!r}")
            notifier.reconnect(tag, f"error={e!r}")

        backoff = min(backoff * 2, RECONNECT_MAX_S)
        log.info(f"[{tag}] reconnect in {backoff:.0f}s (attempt {attempt + 1}/{MAX_RETRIES})")
        await asyncio.sleep(backoff)

    log.error(f"[{tag}] exhausted {MAX_RETRIES} retries; giving up")
    notifier.shutdown(tag, "max retries exhausted")


def handle_message(raw: str | bytes, tag: str, tier_name: str) -> None:
    """Dispatch one server frame."""
    recv_us = now_us()
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return  # ignore non-JSON frames

    mtype = msg.get("type")

    if mtype == "welcome":
        log.info(
            f"[{tag}] welcome  tier={msg.get('tier')}  cex={msg.get('allowedCex')}  "
            f"max_ips={msg.get('maxDistinctIps')}  expires_in={msg.get('expiresInSecs')}s"
        )
        return

    if mtype == "heartbeat":
        # Quiet: too noisy to log every 30s. Could log at debug level.
        return

    if mtype == "error":
        log.error(
            f"[{tag}] error  code={msg.get('code')}  retry_after={msg.get('retryAfterSecs')}s"
        )
        return

    if mtype in ("announcement", "test_announcement"):
        emit_announcement(msg, tag, tier_name, recv_us, is_test=(mtype == "test_announcement"))
        return

    # Unknown type — log for visibility.
    log.warning(f"[{tag}] unknown type={mtype!r}  keys={list(msg.keys())}")


def emit_announcement(msg: dict[str, Any], tag: str, tier_name: str,
                      recv_us: int, is_test: bool = False) -> None:
    """Print + notify one announcement frame."""
    # TEMP: dump full payload to discover all available fields.
    log.info(f"[{tag}] FULL PAYLOAD: {json.dumps(msg, ensure_ascii=False)}")

    dispatch_us = msg.get("dispatchTimestampUs")
    detect_us = msg.get("detectedTimestampUs")

    # Total CL Latency: from their detection to our receipt.
    total_cl_ms: int | None = None
    if isinstance(detect_us, int):
        total_cl_ms = (recv_us - detect_us) // 1000  # us → ms

    # Network latency: time from their dispatch to our receipt.
    net_ms: int | None = None
    if isinstance(dispatch_us, int):
        net_ms = (recv_us - dispatch_us) // 1000  # us → ms

    # CL-side internal latency: time from their detection to their dispatch.
    cl_internal_ms: int | None = None
    if isinstance(detect_us, int) and isinstance(dispatch_us, int):
        cl_internal_ms = (dispatch_us - detect_us) // 1000

    title = msg.get("title", "") or "(redacted)"
    ticker = msg.get("ticker", "") or "(redacted)"
    publisher = msg.get("publisher", "?")
    listing_type = msg.get("listingType", "?")
    abnormal = msg.get("abnormalDetectionLatency", False)

    prefix = "[TEST] " if is_test else ""
    log.info(
        f"🚀 {prefix}NEW [{tag}]  "
        f"total{fmt_latency(total_cl_ms)}  "
        f"net{fmt_latency(net_ms)}  "
        f"cl_int{fmt_latency(cl_internal_ms)}  "
        f"type={listing_type}  ticker={ticker}  publisher={publisher}  "
        f"{'⚠ABNORMAL ' if abnormal else ''}"
        f"{title}"
    )

    # Telegram notification (skip test messages).
    if not is_test:
        notifier.announcement(
            channel=tag,
            title=f"{listing_type}: {ticker} ({publisher})" if ticker != "(redacted)"
                  else f"{listing_type} ({publisher})",
            latency_ms=total_cl_ms,
            detect_ts_ms=detect_us // 1000 if isinstance(detect_us, int) else None,
            dispatch_ts_ms=dispatch_us // 1000 if isinstance(dispatch_us, int) else None,
            recv_ts_ms=recv_us // 1000,
            extra={
                "tier": tier_name,
                "publisher": publisher,
                "listing_type": listing_type,
                "net_ms": net_ms,
                "cl_internal_ms": cl_internal_ms,
                "abnormal": "yes" if abnormal else "no",
                "title": title,
            },
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
async def _run_all(tasks: list) -> None:
    """Run all tier tasks concurrently in one event loop."""
    if not tasks:
        return
    await asyncio.gather(*tasks)


def main() -> None:
    parser = argparse.ArgumentParser(description="CryptoListing.ws client for announce_scanner")
    parser.add_argument(
        "--tier",
        choices=["speedtrial", "freedelayed", "both"],
        default="both",
        help="Which tier(s) to run (default: both, if tokens are set)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Send a test message 15s after welcome (smoke check)",
    )
    args = parser.parse_args()

    run_st = args.tier in ("speedtrial", "both")
    run_fd = args.tier in ("freedelayed", "both")

    coros = []
    if run_st:
        coros.append(run_tier("SpeedTrial", SPEEDTRIAL_TOKEN, args.test))
    if run_fd:
        coros.append(run_tier("FreeDelayed", FREEDELAYED_TOKEN, args.test))

    if not coros:
        log.error("No tier selected and no tokens set. Configure CL_SPEEDTRIAL_TOKEN "
                  "and/or CL_FREEDELAYED_TOKEN in .env.")
        sys.exit(2)

    try:
        asyncio.run(_run_all(coros))
    except KeyboardInterrupt:
        log.info("stopped")
        for tag in ("CLWS", "CLWD"):
            notifier.shutdown(tag, "interrupted by user")
        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(asyncio.sleep(1.0))
            loop.run_until_complete(notifier.close())
            loop.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
