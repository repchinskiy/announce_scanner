#!/usr/bin/env python3
"""
ticker_sniper.py — Binance price ticker sniper for announce_scanner

Fifth detection channel. Polls GET /api/v3/ticker/price (all symbols,
lightweight ~150KB JSON) at a configurable rate and detects new trading
symbols by diffing the symbol set between requests.

Unlike exchangeInfo (17MB, ~510MB/min at 2s interval), the ticker/price
endpoint returns only symbol+price pairs (~150KB), making high-frequency
polling practical. This is the technique used by rickstaa/crypto-listings-sniper
to achieve sub-300ms detection of "symbol became tradable" events.

The signal is "matching engine started publishing prices for this pair",
which happens AFTER the symbol is added to exchangeInfo and typically
AFTER the announcement. This channel is for actionable "tradable now"
detection, not for early announcement detection.

Default hosts (in priority order):
  - api4.binance.com      : direct nginx origin, NO CloudFront, NO rate-limiter
                            (discovered in rickstaa/crypto-listings-sniper)
  - api.binance.com      : CloudFront-fronted, X-Cache: Miss
  - api-gcp.binance.com  : GCP GLB, no X-Cache, origin forbids cache

All three are polled in parallel per cycle; a new symbol on any host is
emitted.

Adaptive 429 backoff: on HTTP 429, switch to exponential backoff up to
SNIPER_MAX_429_BACKOFF_S; once 429s stop, gradually return to normal rate.

Usage:
    python ticker_sniper.py             # continuous polling
    python ticker_sniper.py --oneshot   # single check, print + exit
    python ticker_sniper.py --reset     # reset stored state

Config (env, optional):
    SNIPER_POLL_INTERVAL_MS   base interval between cycles (default 1000 = 1Hz)
    SNIPER_HOSTS              comma-separated API hosts
    SNIPER_MAX_429_BACKOFF_S  max backoff on 429 (default 60)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from typing import Any

from http_client import create_http_client, classify_cache_status, get_backend

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
DEFAULT_HOSTS = ["api4.binance.com", "api.binance.com", "api-gcp.binance.com", "api2.binance.com"]
HOSTS = [h.strip() for h in os.environ.get("SNIPER_HOSTS", ",".join(DEFAULT_HOSTS)).split(",") if h.strip()]

POLL_INTERVAL_MS = int(os.environ.get("SNIPER_POLL_INTERVAL_MS", "1000"))
MAX_429_BACKOFF_S = float(os.environ.get("SNIPER_MAX_429_BACKOFF_S", "60"))

# /api/v3/ticker/price without params returns all symbols. weight=2 per req,
# Spot API limit 6000/min → 3000 req/min = 50 req/sec ceiling per IP per host.
# We default to 1 req/sec (1000ms) — extremely conservative. Tune aggressively
# once we confirm no 429 in production.
HTTP_TIMEOUT_S = 5.0

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(PROJECT_ROOT, "state")
STATE_FILE = os.path.join(STATE_DIR, "ticker_sniper_state.json")


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


def fmt_latency(ms_val: int | None) -> str:
    if ms_val is None:
        return "  N/A  "
    if ms_val < 100:
        return f" \033[92m{ms_val:>5}ms\033[0m"
    if ms_val < 150:
        return f" \033[93m{ms_val:>5}ms\033[0m"
    return f" \033[91m{ms_val:>5}ms\033[0m"


def extract_symbols(body: Any) -> set[str]:
    """Extract symbol names from /api/v3/ticker/price response.

    Body shape: [{"symbol":"ETHBTC","price":"0.02923000"}, ...]
    """
    if not isinstance(body, list):
        return set()
    return {entry.get("symbol", "") for entry in body if entry.get("symbol")}


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------
async def fetch_one(session, host: str) -> tuple[str, set[str], int, str, int]:
    """Fetch /api/v3/ticker/price from one host.

    Returns (host, symbols, recv_ms, cache_status, status_code).
    On error returns (host, set(), 0, "Error", 0).
    """
    url = f"https://{host}/api/v3/ticker/price"
    try:
        resp = await session.get(url, headers={"User-Agent": "Mozilla/5.0"})
        recv_ms = now_ms()
        if resp.status_code == 429:
            return host, set(), recv_ms, "RateLimited", 429
        if resp.status_code != 200:
            return host, set(), recv_ms, "Error", resp.status_code
        body = resp.body
        if body is None:
            return host, set(), recv_ms, "Error", 0
        cache_status = classify_cache_status(resp.headers)
        return host, extract_symbols(body), recv_ms, cache_status, resp.status_code
    except Exception:
        return host, set(), 0, "Error", 0


async def poll_once(session: aiohttp.ClientSession,
                    known: set[str],
                    stats: dict[str, Any]) -> tuple[set[str], int]:
    """Poll all hosts in parallel, emit new symbols.

    Returns (new_symbols_set, num_429_seen).
    """
    tasks = [fetch_one(session, h) for h in HOSTS]
    results = await asyncio.gather(*tasks)

    merged: set[str] = set()
    num_429 = 0
    for host, symbols, recv_ms, cache_status, status_code in results:
        stats.setdefault("total", {}).setdefault(host, 0)
        stats["total"][host] += 1
        stats.setdefault("cache", {}).setdefault(host, {})
        stats["cache"][host][cache_status] = stats["cache"][host].get(cache_status, 0) + 1

        if status_code == 429:
            num_429 += 1
            stats.setdefault("rate_limited", {}).setdefault(host, 0)
            stats["rate_limited"][host] = stats["rate_limited"].get(host, 0) + 1
            continue
        if status_code != 200:
            log.warning(f"[SNIPER] HTTP {status_code}  host={host}")
            notifier.reconnect("SNIPER", f"HTTP {status_code} (host={host})")
            stats.setdefault("err_count", 0)
            stats["err_count"] += 1
            continue

        merged.update(symbols)

    new_symbols = merged - known
    recv_ms_now = now_ms()
    for sym in sorted(new_symbols):
        src_hosts = [h for h, syms, _, _, _ in results if sym in syms]
        src_host = ",".join(src_hosts) if src_hosts else "?"
        log.info(
            f"🚀 NEW [SNIPER]  symbol={sym}  host={src_host}"
        )
        notifier.announcement(
            channel="SNIPER",
            title=f"New tradable symbol: {sym}",
            latency_ms=None,  # no listed_at timestamp in ticker/price
            recv_ts_ms=recv_ms_now,
            extra={"host": src_host, "backend": get_backend()} if src_host != "?" else {"backend": get_backend()},
        )

    return new_symbols, num_429


async def run_oneshot() -> set[str]:
    async with create_http_client(timeout=HTTP_TIMEOUT_S) as session:
        new_syms, _ = await poll_once(session, known=set(), stats={})
        return new_syms


async def run_continuous() -> None:
    state = load_state()
    known: set[str] = set(state.get("symbols", []))
    stats: dict[str, Any] = {"total": {}, "cache": {}, "rate_limited": {}}
    cycle = 0
    prev_err = 0
    prev_total_reqs = 0

    # 429 backoff state
    backoff_s = 0.0  # 0 = normal rate
    consecutive_clean_cycles = 0  # cycles without 429

    log.info(f"[SNIPER] announce_scanner  hosts={HOSTS}  "
             f"interval={POLL_INTERVAL_MS}ms  max_429_backoff={MAX_429_BACKOFF_S}s  "
             f"state={STATE_FILE}")
    notifier.startup(
        "SNIPER",
        f"endpoints:\n"
        + "".join(f"  https://{h}/api/v3/ticker/price\n" for h in HOSTS)
        + f"backend: {get_backend()}\n"
        + f"interval: {POLL_INTERVAL_MS}ms",
    )

    async with create_http_client(timeout=HTTP_TIMEOUT_S) as session:
        # Bootstrap: seed baseline silently (no emits).
        if not known:
            baseline = set()
            for host, symbols, _, _, _ in await asyncio.gather(*[fetch_one(session, h) for h in HOSTS]):
                baseline.update(symbols)
            known = baseline
            state["symbols"] = sorted(known)
            save_state(state)
            log.info(f"[SNIPER] bootstrap  baseline={len(known)} symbols (no emits)")
        else:
            log.info(f"[SNIPER] ready  known={len(known)}")

        while True:
            cycle += 1
            start = now_ms()

            new_syms, num_429 = await poll_once(session, known, stats)
            if new_syms:
                known.update(new_syms)
                state["symbols"] = sorted(known)
                save_state(state)

            elapsed = now_ms() - start

            # 429 backoff logic
            if num_429 > 0:
                consecutive_clean_cycles = 0
                backoff_s = min(backoff_s * 2 if backoff_s > 0 else 1.0, MAX_429_BACKOFF_S)
                if backoff_s == 0:
                    backoff_s = 1.0  # first 429 → 1s backoff
                log.warning(f"[SNIPER] 429 detected on {num_429} host(s)  "
                            f"backoff={backoff_s:.0f}s")
                notifier.reconnect("SNIPER", f"429 rate-limited, backoff={backoff_s:.0f}s")
            else:
                consecutive_clean_cycles += 1
                # After 10 clean cycles, halve backoff (gradual recovery)
                if backoff_s > 0 and consecutive_clean_cycles >= 10:
                    backoff_s = backoff_s / 2
                    consecutive_clean_cycles = 0
                    if backoff_s < 1.0:
                        backoff_s = 0.0
                        log.info("[SNIPER] backoff cleared, returning to normal rate")
                    else:
                        log.info(f"[SNIPER] reducing backoff to {backoff_s:.0f}s")

            sleep_ms = (backoff_s * 1000) if backoff_s > 0 else POLL_INTERVAL_MS
            sleep_ms = max(0, sleep_ms - elapsed)
            await asyncio.sleep(sleep_ms / 1000)

            if cycle % int(os.environ.get("SNIPER_STATS_EVERY_CYCLES", "100")) == 0:
                total = stats["total"]
                cache = stats["cache"]
                rl = stats.get("rate_limited", {})
                parts = []
                for h in sorted(total):
                    c = cache.get(h, {})
                    bucket_str = " ".join(
                        f"{k[:1].upper() if k else '?'}:{v}"
                        for k, v in sorted(c.items(), key=lambda kv: -kv[1])
                    )
                    rl_count = rl.get(h, 0)
                    parts.append(f"{h}:{total[h]}({bucket_str} 429:{rl_count})")
                mode = f"BACKOFF={backoff_s:.0f}s" if backoff_s > 0 else "normal"
                log.info(
                    f"[SNIPER] STATS  cycle={cycle}  mode={mode}  known={len(known)}  "
                    f"err={stats.get('err_count', 0)}  "
                    f"[{', '.join(parts)}]"
                )
                # Notify Telegram on new errors (only if rate > 5% since last STATS).
                current_err = stats.get('err_count', 0)
                current_total = sum(stats['total'].values()) if stats['total'] else 0
                delta_err = current_err - prev_err
                delta_reqs = current_total - prev_total_reqs
                if delta_err > 0 and delta_reqs > 0 and (delta_err / delta_reqs) > 0.05:
                    rate = delta_err / delta_reqs * 100
                    log.warning(f"[SNIPER] {delta_err} new errors ({rate:.0f}% of requests, total: {current_err})")
                    notifier.reconnect("SNIPER", f"{delta_err} new errors ({rate:.0f}% rate), total {current_err}")
                elif delta_err > 0 and delta_reqs == 0:
                    log.warning(f"[SNIPER] {delta_err} new errors (100% of requests, total: {current_err})")
                    notifier.reconnect("SNIPER", f"{delta_err} new errors (100% rate), total {current_err}")
                prev_err = current_err
                prev_total_reqs = current_total


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Binance ticker sniper for announce_scanner")
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
        log.info("[SNIPER] stopped")
        notifier.shutdown("SNIPER", "interrupted by user")
        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(asyncio.sleep(1.0))
            loop.run_until_complete(notifier.close())
            loop.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
