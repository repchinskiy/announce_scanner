#!/usr/bin/env python3
"""
run.py — single entry point for announce_scanner

Runs all configured channels in parallel within a single asyncio event loop.
Each channel runs as its own task; they share the event loop but do not block
each other (asyncio cooperative scheduling). Each channel owns its own
aiohttp session and state file, so they do not compete for resources.

Config (env, in .env):
    ENABLED_CHANNELS  comma-separated list of channel names to run.
                      Default: all channels.
                      Valid: ws,cms_apex,exchange,sniper,cms_composite,clws
    CL_TIERS          comma-separated CL tiers (default: both)
                      Valid: both,speedtrial,freedelayed
    CL_TEST           send CL test message on connect (default: false)

Examples:
    python run.py                                              # all channels
    ENABLED_CHANNELS=ws,cms_apex python run.py                  # only WS + CMS apex
    ENABLED_CHANNELS=ws,sniper,clws python run.py               # WS + sniper + CL
    ENABLED_CHANNELS=ws python run.py                          # WS only (lightweight)
    ENABLED_CHANNELS=all,!clws python run.py                   # all except CLWS
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Awaitable, Callable

# Load .env from the script's directory if python-dotenv is available.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

from aggregator import aggregator
from log_setup import setup_logging, get_logger
from notifier import notifier

# Initialize centralized logging
log = setup_logging()


# ---------------------------------------------------------------------------
# Channel registry
# ---------------------------------------------------------------------------
ALL_CHANNELS = ["ws", "cms_apex", "exchange", "sniper", "cms_composite", "cms_catalog", "clws"]


# Each channel's runner is a lazy import so that a missing module (e.g. if
# `websockets` isn't installed) only fails that one channel, not the whole
# runner. Each runner is an async coroutine that runs forever (or until the
# process is interrupted).

async def _run_ws() -> None:
    from announce_ws_client import run as ws_run
    await ws_run()


async def _run_cms_apex() -> None:
    from cms_apex_poller import run_continuous as cms_apex_run
    await cms_apex_run()


async def _run_exchange() -> None:
    from exchangeinfo_poller import run_continuous as exchange_run
    await exchange_run()


async def _run_sniper() -> None:
    from ticker_sniper import run_continuous as sniper_run
    await sniper_run()


async def _run_cms_composite() -> None:
    from cms_composite_poller import run_continuous as cms_composite_run
    await cms_composite_run()


async def _run_cms_catalog() -> None:
    from cms_apex_catalog_poller import run_continuous as cms_catalog_run
    await cms_catalog_run()


async def _run_clws() -> None:
    from cryptolisting_ws_client import run_tier, SPEEDTRIAL_TOKEN, FREEDELAYED_TOKEN
    tiers_raw = os.environ.get("CL_TIERS", "both").strip().lower()
    send_test = os.environ.get("CL_TEST", "").lower() in ("1", "true", "yes", "on")

    coros = []
    if tiers_raw in ("both", "speedtrial") and SPEEDTRIAL_TOKEN:
        coros.append(run_tier("SpeedTrial", SPEEDTRIAL_TOKEN, send_test))
    elif tiers_raw in ("both", "speedtrial") and not SPEEDTRIAL_TOKEN:
        log.info("[CLWS] SpeedTrial token not set (CL_SPEEDTRIAL_TOKEN), skipping")

    if tiers_raw in ("both", "freedelayed") and FREEDELAYED_TOKEN:
        coros.append(run_tier("FreeDelayed", FREEDELAYED_TOKEN, send_test))
    elif tiers_raw in ("both", "freedelayed") and not FREEDELAYED_TOKEN:
        log.info("[CLWD] FreeDelayed token not set (CL_FREEDELAYED_TOKEN), skipping")

    if not coros:
        log.info("[CLWS] no CL tokens configured; channel idle. "
                 "Get keys via Telegram @CLWfeed.")
        return

    await asyncio.gather(*coros)


CHANNEL_RUNNERS: dict[str, Callable[[], Awaitable[None]]] = {
    "ws": _run_ws,
    "cms_apex": _run_cms_apex,
    "exchange": _run_exchange,
    "sniper": _run_sniper,
    "cms_composite": _run_cms_composite,
    "cms_catalog": _run_cms_catalog,
    "clws": _run_clws,
}


# ---------------------------------------------------------------------------
# Channel list parsing
# ---------------------------------------------------------------------------
def _parse_channels(raw: str) -> tuple[list[str], list[str]]:
    """Parse ENABLED_CHANNELS env var.

    Supports:
      - comma-separated list of channel names
      - "all" (alias for everything)
      - "all,!clws" — all except clws (negation with !)
      - "!clws" alone — same as all,!clws
    Returns (enabled, skipped) for reporting.
    """
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    if not parts:
        parts = ["all"]

    explicit: list[str] | None = None
    negations: set[str] = set()

    for p in parts:
        if p.startswith("!"):
            negations.add(p[1:])
        elif p == "all":
            explicit = list(ALL_CHANNELS)
        elif p in ALL_CHANNELS:
            if explicit is None:
                explicit = []
            explicit.append(p)
        else:
            log.warning(f"unknown channel: {p!r} (valid: {', '.join(ALL_CHANNELS)})")

    if explicit is None:
        # Only negations specified → start from all.
        explicit = list(ALL_CHANNELS)

    # Dedupe while preserving order.
    seen: set[str] = set()
    enabled: list[str] = []
    for ch in explicit:
        if ch not in seen and ch not in negations:
            seen.add(ch)
            enabled.append(ch)

    skipped = [c for c in ALL_CHANNELS if c not in enabled]
    return enabled, skipped


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main_async() -> None:
    enabled, skipped = _parse_channels(
        os.environ.get("ENABLED_CHANNELS", ",".join(ALL_CHANNELS))
    )
    if not enabled:
        log.error(f"no channels enabled. Set ENABLED_CHANNELS env to "
                  f"one or more of: {', '.join(ALL_CHANNELS)}")
        sys.exit(2)

    log.info(f"starting channels: {','.join(enabled)}"
                + (f" (skipped: {','.join(skipped)})" if skipped else ""))

    aggregator.start()

    coros = []
    for ch in enabled:
        runner = CHANNEL_RUNNERS[ch]
        coros.append(runner())

    # Each channel runs as a separate task in the same event loop. asyncio
    # schedules them cooperatively; they don't block each other as long as
    # each yields (uses await for IO).
    await asyncio.gather(*coros)


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        log.info("stopped by user")


if __name__ == "__main__":
    main()
