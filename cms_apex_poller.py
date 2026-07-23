#!/usr/bin/env python3
"""
cms_apex_poller.py — parallel CMS (apex) API poller with CloudFront cache busting

Part of the announce_scanner project. Monitors Binance announcement
categories via the CMS REST API (apex path), using parallel requests with
different pageSize values to increase the probability of at least one cache
miss per cycle. Designed as a fallback / comparison channel alongside the
primary WebSocket listener (announce_ws_client.py). Each detection is tagged
"CMS_APEX" in the output.

Default category: 48 (New Cryptocurrency Listing == list/48).
Override via env: ANN_CATALOG_IDS=48,49,50,51,93,128,157,161

CloudFront behaviour (observed 2026-07-20):
  - Cache key includes (catalogId, pageNo, pageSize)
  - Valid pageSize values: 1, 2, 3, 5, 10, 15, 20, 50
  - Web UI uses 10, 50 -> those are always cached
  - We use rare pageSizes (1, 3, 5, 15, 20) -> higher miss probability
  - 5 parallel requests per cycle -> at least one may miss
  - "no-cache" / "Pragma: no-cache" request headers do NOT bypass CloudFront
  - Random query params -> 400 Bad Request

State persistence:
  max_id per catalogId is stored in ./state/cms_state.json (relative to the
  script's directory). This survives restarts without re-emitting already
  known announcements.

Usage:
    python cms_poller.py             # continuous polling
    python cms_poller.py --oneshot   # single check, print + exit
    python cms_poller.py --reset     # reset stored state
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
from typing import Any

# Load .env from the script's directory if python-dotenv is available.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

from http_client import create_http_client

# Local import: shared Telegram notifier (fire-and-forget, no-op if unconfigured).
from notifier import notifier
from log_setup import get_logger

log = get_logger()


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CMS_BASE = "https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query"

# State file lives in ./state/ relative to this script (inside the project).
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(PROJECT_ROOT, "state")
STATE_FILE = os.path.join(STATE_DIR, "cms_state.json")

# Default catalog IDs (only 48 by default; opt-in via env for more).
# Known categories:
#   48  -> New Cryptocurrency Listing
#   49  -> Latest Binance News
#   50  -> Latest Activities
#   51  -> Delistings
#   93  -> Latest Activities (alt)
#   128 -> P2P Merchant Announcements
#   157 -> Margin / Futures listings
#   161 -> Earn / Staking
def _parse_catalog_ids(raw: str) -> list[int]:
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return [int(p) for p in parts]


CATALOG_IDS: list[int] = _parse_catalog_ids(os.environ.get("ANN_CATALOG_IDS", "48"))

# "0" means "monitor ALL known categories" — expand it for the CMS poller
# (the CMS endpoint requires a concrete catalogId per request).
ALL_KNOWN_CATALOG_IDS = [48, 49, 50, 51, 93, 128, 157, 161]
if CATALOG_IDS == [0]:
    CATALOG_IDS = list(ALL_KNOWN_CATALOG_IDS)

POLL_INTERVAL_S = float(os.environ.get("CMS_POLL_INTERVAL_S", "2.0"))   # base interval
POLL_FAST_INTERVAL_S = float(os.environ.get("CMS_POLL_FAST_INTERVAL_S", "0.5"))  # reactive burst
POLL_FAST_CYCLES = int(os.environ.get("CMS_POLL_FAST_CYCLES", "4"))     # burst length after trigger
# Signals that trigger a burst of fast cycles (any of: "Miss", "RefreshHit").
ADAPTIVE_TRIGGERS = set(
    t.strip() for t in os.environ.get("CMS_ADAPTIVE_TRIGGERS", "Miss,RefreshHit").split(",")
    if t.strip()
)

# Cache busting via random pageSize per-request.
# Valid pageSizes: 1,2,3,5,10,15,20,50. Using full range 1..49 for max variety.
# Each cycle generates NUM_CACHE_KEYS random values — more distinct keys = higher
# probability of at least one CloudFront cache miss.
NUM_CACHE_KEYS = int(os.environ.get("CMS_NUM_CACHE_KEYS", "10"))
CACHE_KEY_RANGE = (1, 49)  # inclusive


def _generate_cache_keys(n: int = NUM_CACHE_KEYS) -> list[dict[str, int]]:
    """Return *n* random cache-key dicts with unique pageSize values."""
    sizes = random.sample(range(CACHE_KEY_RANGE[0], CACHE_KEY_RANGE[1] + 1),
                          min(n, CACHE_KEY_RANGE[1] - CACHE_KEY_RANGE[0] + 1))
    return [{"pageSize": s, "pageNo": 1} for s in sizes]


# ---------------------------------------------------------------------------
# State persistence (per-catalog max_id)
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


def extract_articles(data: dict[str, Any], catalog_id: int) -> list[dict[str, Any]]:
    """Safely dig out the articles list for a given catalogId from CMS response."""
    try:
        d = data.get("data", {}) or {}
        catalogs = d.get("catalogs", [])
        if not catalogs:
            return []
        # CMS returns a single catalog entry matching the requested catalogId.
        cat = catalogs[0]
        if cat.get("catalogId") != catalog_id:
            return []
        return cat.get("articles", [])
    except (KeyError, IndexError, TypeError):
        return []


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------
async def poll_one(session: aiohttp.ClientSession,
                   catalog_id: int,
                   params: dict[str, int],
                   known_max_id: int,
                   stats: dict[str, Any],
                   key_label: str,
                   emit: bool = True) -> tuple[int, bool]:
    """Fetch one cache key for one catalog.

    Returns (updated_max_id, triggered) where `triggered` is True if this
    response's cache status is in ADAPTIVE_TRIGGERS (Miss / RefreshHit).
    When `emit=False` (bootstrap), only updates max_id without logging/notifying.
    """
    url = (
        f"{CMS_BASE}?type=1"
        f"&pageNo={params['pageNo']}"
        f"&pageSize={params['pageSize']}"
        f"&catalogId={catalog_id}"
    )

    stat_key = f"cat{catalog_id}/{key_label}"

    try:
        resp = await session.get(url, headers={"User-Agent": "Mozilla/5.0"})
        recv_ms = now_ms()
        body = resp.body
        cache_status = resp.headers.get("X-Cache", "?")
        status_code = resp.status_code
    except Exception:
        stats.setdefault("err_count", 0)
        stats["err_count"] += 1
        stats.setdefault("total", {}).setdefault(stat_key, 0)
        stats["total"][stat_key] += 1
        return known_max_id, False

    if status_code == 429:
        log.warning(f"[CMS_APEX] 429 rate-limited  cat={catalog_id}  key={key_label}")
        notifier.reconnect("CMS_APEX", f"429 rate-limited (cat={catalog_id}, key={key_label})")
        stats.setdefault("err_count", 0)
        stats["err_count"] += 1
    elif status_code != 200:
        log.warning(f"[CMS_APEX] HTTP {status_code}  cat={catalog_id}  key={key_label}  cache={cache_status}")
        notifier.reconnect("CMS_APEX", f"HTTP {status_code} (cat={catalog_id}, key={key_label})")
        stats.setdefault("err_count", 0)
        stats["err_count"] += 1

    stats.setdefault("total", {}).setdefault(stat_key, 0)
    stats["total"][stat_key] += 1
    stats.setdefault("cache", {}).setdefault(stat_key, {})
    stats["cache"][stat_key][cache_status] = stats["cache"][stat_key].get(cache_status, 0) + 1

    # Did this response trigger an adaptive burst? Match on the short token
    # that appears in the X-Cache header (Miss / RefreshHit).
    triggered = any(t.lower() in cache_status.lower() for t in ADAPTIVE_TRIGGERS)

    if status_code != 200:
        return known_max_id, triggered

    articles = extract_articles(body, catalog_id)
    if not articles:
        return known_max_id, triggered

    latest = max(articles, key=lambda a: a["id"])
    art_id: int = latest["id"]

    if art_id > known_max_id and emit:
        pub_ms = latest.get("releaseDate")
        latency = (recv_ms - pub_ms) if isinstance(pub_ms, int) else None
        title = latest.get("title", "?")
        log.info(
            f"🚀 NEW [CMS_APEX] latency{fmt_latency(latency)}  "
            f"cat={catalog_id}  id={art_id}  key={key_label}  cache={cache_status}  "
            f"{title}"
        )
        # Telegram notification (fire-and-forget, never blocks the poll loop).
        notifier.announcement(
            channel="CMS_APEX",
            title=title,
            latency_ms=latency,
            catalog_id=catalog_id,
            article_id=art_id,
            publish_ts_ms=pub_ms if isinstance(pub_ms, int) else None,
            recv_ts_ms=recv_ms,
            extra={"cache": cache_status, "key": key_label} if cache_status else None,
        )
        return art_id, triggered

    if art_id > known_max_id and not emit:
        # Bootstrap: update max_id silently.
        return art_id, triggered

    return known_max_id, triggered


async def poll_catalog(session: aiohttp.ClientSession,
                       catalog_id: int,
                       known_max_id: int,
                       stats: dict[str, Any],
                       emit: bool = True) -> tuple[int, bool]:
    """Run all cache-key requests for one catalog in parallel.

    Returns (updated_max_id, any_triggered).
    When `emit=False` (bootstrap), only updates max_id without logging/notifying.
    """
    cache_keys = _generate_cache_keys()
    tasks = [
        poll_one(session, catalog_id, p, known_max_id, stats, f"PS{p['pageSize']}", emit=emit)
        for p in cache_keys
    ]
    results = await asyncio.gather(*tasks)
    if not results:
        return known_max_id, False
    max_id = max(r[0] for r in results)
    any_triggered = any(r[1] for r in results)
    return max_id, any_triggered


async def run_oneshot() -> dict[int, int]:
    """Single poll cycle across all catalogs, return {catalog_id: max_id}."""
    state = load_state()
    max_ids: dict[int, int] = {cid: int(state.get(f"cat{cid}", {}).get("max_id", 0))
                               for cid in CATALOG_IDS}
    async with create_http_client(timeout=5.0) as session:
        for cid in CATALOG_IDS:
            max_ids[cid], _ = await poll_catalog(session, cid, max_ids[cid], {})
    return max_ids


async def run_continuous() -> None:
    """Continuous polling loop across all catalogs with adaptive interval."""
    state = load_state()
    # Ensure state has a sub-dict per catalog.
    known: dict[int, int] = {}
    for cid in CATALOG_IDS:
        entry = state.get(f"cat{cid}", {}) if isinstance(state.get(f"cat{cid}"), dict) else {}
        known[cid] = int(entry.get("max_id", 0))

    stats: dict[str, Any] = {"total": {}, "cache": {}}
    cycle = 0
    fast_remaining = 0  # cycles left in an adaptive burst
    prev_err = 0  # for error-notification dedup
    prev_total_reqs = 0  # total requests at last STATS

    cats_str = ",".join(str(c) for c in CATALOG_IDS)
    log.info(f"[CMS_APEX] announce_scanner  catalogs=[{cats_str}]  "
             f"num_keys={NUM_CACHE_KEYS}  "
             f"base_interval={POLL_INTERVAL_S}s  fast={POLL_FAST_INTERVAL_S}s/{POLL_FAST_CYCLES}  "
             f"triggers={sorted(ADAPTIVE_TRIGGERS)}  state={STATE_FILE}")
    notifier.startup(
        "CMS_APEX",
        f"endpoint: {CMS_BASE}\n"
        f"catalogs: [{cats_str}]\n"
        f"cache keys: {NUM_CACHE_KEYS} (random 1..49)\n"
        f"interval: {POLL_INTERVAL_S}s base / {POLL_FAST_INTERVAL_S}s fast x{POLL_FAST_CYCLES}",
    )

    async with create_http_client(timeout=5.0) as session:
        # Bootstrap: populate max_id per catalog without emitting (catch-up).
        for cid in CATALOG_IDS:
            known[cid], _ = await poll_catalog(session, cid, known[cid], stats, emit=False)
            state[f"cat{cid}"] = {"max_id": known[cid]}
        save_state(state)
        ready_str = " ".join(f"cat{c}={known[c]}" for c in CATALOG_IDS)
        log.info(f"[CMS_APEX] ready  {ready_str}")

        # Poll loop
        while True:
            cycle += 1
            start = now_ms()

            # All catalogs in parallel (each catalog fires its 5 cache keys in parallel too)
            tasks = [poll_catalog(session, cid, known[cid], stats) for cid in CATALOG_IDS]
            results = await asyncio.gather(*tasks)

            dirty = False
            any_triggered = False
            for cid, (new_max, triggered) in zip(CATALOG_IDS, results):
                if new_max > known[cid]:
                    known[cid] = new_max
                    state[f"cat{cid}"] = {"max_id": new_max}
                    dirty = True
                if triggered:
                    any_triggered = True
            if dirty:
                save_state(state)

            # Adaptive: any Miss / RefreshHit triggers a burst of fast cycles.
            if any_triggered:
                fast_remaining = POLL_FAST_CYCLES

            elapsed = now_ms() - start
            if fast_remaining > 0:
                fast_remaining -= 1
                interval = POLL_FAST_INTERVAL_S
            else:
                interval = POLL_INTERVAL_S
            sleep_s = max(0, (interval * 1000) - elapsed) / 1000

            if cycle % int(os.environ.get("CMS_STATS_EVERY_CYCLES", "100")) == 0:
                total = stats["total"]
                cache = stats["cache"]
                parts = []
                for k in sorted(total):
                    c = cache.get(k, {})
                    parts.append(
                        f"{k}:{total[k]}"
                        f"(H:{c.get('Hit from cloudfront', 0)} "
                        f"M:{c.get('Miss from cloudfront', 0)} "
                        f"R:{c.get('RefreshHit from cloudfront', 0)})"
                    )
                ready_str = " ".join(f"cat{c}={known[c]}" for c in CATALOG_IDS)
                mode = "FAST" if fast_remaining > 0 else "base"
                log.info(
                    f"[CMS_APEX] STATS  cycle={cycle}  mode={mode}  {ready_str}  "
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
                    log.warning(f"[CMS_APEX] {delta_err} new errors ({rate:.0f}% of requests, total: {current_err})")
                    notifier.reconnect("CMS_APEX", f"{delta_err} new errors ({rate:.0f}% rate), total {current_err}")
                elif delta_err > 0 and delta_reqs == 0:
                    # All requests failed — 100% error rate.
                    log.warning(f"[CMS_APEX] {delta_err} new errors (100% of requests, total: {current_err})")
                    notifier.reconnect("CMS_APEX", f"{delta_err} new errors (100% rate), total {current_err}")
                prev_err = current_err
                prev_total_reqs = current_total

            await asyncio.sleep(sleep_s)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="CMS API poller for announce_scanner")
    parser.add_argument("--oneshot", action="store_true", help="single check, print + exit")
    parser.add_argument("--reset", action="store_true", help="reset stored max_id state")
    args = parser.parse_args()

    if args.reset:
        reset_state()
        return

    try:
        if args.oneshot:
            max_ids = asyncio.run(run_oneshot())
            for cid, mid in max_ids.items():
                log.info(f"cat={cid}  max_id={mid}")
        else:
            asyncio.run(run_continuous())
    except KeyboardInterrupt:
        log.info("[CMS_APEX] stopped")
        notifier.shutdown("CMS_APEX", "interrupted by user")
        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(asyncio.sleep(1.0))
            loop.run_until_complete(notifier.close())
            loop.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
