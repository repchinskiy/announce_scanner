#!/usr/bin/env python3
"""
cms_catalog_poller.py — alternative CMS (apex catalog) endpoint poller

Seventh channel (optional, comparison only). Polls the Binance CMS endpoint
`/bapi/apex/v1/public/apex/cms/article/catalog/list/query` — a hybrid path
that returns articles flat under data.articles[] (like composite) but on the
apex API host. Notably does NOT return publishDate/releaseDate on articles.

This channel exists to add another independent detection path. Even without a
publish timestamp, its recv_ts can be compared against other channels to
empirically determine which path sees new announcements first.

Response shape:
  {"code":"000000","message":"OK","data":{"articles":[{id,code,...}],"total":N},"success":true}

Uses same env-based config as the other CMS pollers.

Usage:
    python cms_catalog_poller.py             # continuous polling
    python cms_catalog_poller.py --oneshot   # single check, print + exit
    python cms_catalog_poller.py --reset     # reset stored state

Config (env, optional):
    ANN_CATALOG_IDS              shared with other CMS pollers (default: 48)
    CATALOG_POLL_INTERVAL_S      base interval (default 60.0)
    CMS_NUM_CACHE_KEYS           shared cache-bust key count (default: 5)
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

from http_client import create_http_client, get_backend

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
CMS_BASE = "https://www.binance.com/bapi/apex/v1/public/apex/cms/article/catalog/list/query"
# https://www.binance.com/bapi/apex/v1/public/apex/cms/article/catalog/list/query?catalogId=48&pageNo=1&pageSize=22
# Cache busting via random pageSize per-request.
# Same env var as other CMS pollers for consistency.
NUM_CACHE_KEYS = int(os.environ.get("CMS_NUM_CACHE_KEYS", "5"))
CACHE_KEY_RANGE = (1, 50)  # inclusive


def _generate_cache_keys(n: int = NUM_CACHE_KEYS) -> list[dict[str, int]]:
    """Return *n* random cache-key dicts with unique pageSize values."""
    sizes = random.sample(range(CACHE_KEY_RANGE[0], CACHE_KEY_RANGE[1] + 1),
                          min(n, CACHE_KEY_RANGE[1] - CACHE_KEY_RANGE[0] + 1))
    return [{"pageSize": s, "pageNo": 1} for s in sizes]


POLL_INTERVAL_S = float(os.environ.get("CATALOG_POLL_INTERVAL_S", "60.0"))

HTTP_TIMEOUT_S = 5.0

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(PROJECT_ROOT, "state")
STATE_FILE = os.path.join(STATE_DIR, "catalog_state.json")


# ---------------------------------------------------------------------------
# Catalog IDs (shared logic with other CMS pollers)
# ---------------------------------------------------------------------------
def _parse_catalog_ids(raw: str) -> list[int]:
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return [int(p) for p in parts]


ALL_KNOWN_CATALOG_IDS = [48, 49, 50, 51, 93, 128, 157, 161]

CATALOG_IDS: list[int] = _parse_catalog_ids(os.environ.get("ANN_CATALOG_IDS", "48"))

# "0" means "monitor ALL known categories".
if CATALOG_IDS == [0]:
    CATALOG_IDS = list(ALL_KNOWN_CATALOG_IDS)


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


def extract_articles(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Catalog path returns articles flat under data.articles (like composite)."""
    try:
        d = data.get("data", {}) or {}
        return d.get("articles", []) or []
    except (KeyError, TypeError):
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
                   emit: bool = True) -> tuple[int, str]:
    """Single poll for one catalog with one cache key.

    Returns (updated_max_id, cache_status).
    When `emit=False` (bootstrap), only updates max_id without logging/notifying.
    """
    page_size = params["pageSize"]
    url = (
        f"{CMS_BASE}?catalogId={catalog_id}"
        f"&pageNo={params['pageNo']}"
        f"&pageSize={page_size}"
    )

    stat_key = f"cat{catalog_id}/{key_label}"

    try:
        resp = await session.get(url, headers={"User-Agent": "Mozilla/5.0"})
        recv_ms = now_ms()
        body = resp.body
        cache_status = resp.header("X-Cache", "-")
        status_code = resp.status_code
    except Exception:
        stats.setdefault("err_count", 0)
        stats["err_count"] += 1
        stats.setdefault("total", {}).setdefault(stat_key, 0)
        stats["total"][stat_key] += 1
        return known_max_id, "Error"

    if status_code == 429:
        log.warning(f"[CMS_CATALOG] 429 rate-limited  cat={catalog_id}  key={key_label}")
        notifier.reconnect("CMS_CATALOG", f"429 rate-limited (cat={catalog_id}, key={key_label})")
        stats.setdefault("err_count", 0)
        stats["err_count"] += 1
    elif status_code != 200:
        log.warning(f"[CMS_CATALOG] HTTP {status_code}  cat={catalog_id}  key={key_label}  cache={cache_status}")
        notifier.reconnect("CMS_CATALOG", f"HTTP {status_code} (cat={catalog_id}, key={key_label})")
        stats.setdefault("err_count", 0)
        stats["err_count"] += 1

    stats.setdefault("total", {}).setdefault(stat_key, 0)
    stats["total"][stat_key] += 1
    stats.setdefault("cache", {}).setdefault(stat_key, {})
    stats["cache"][stat_key][cache_status] = stats["cache"][stat_key].get(cache_status, 0) + 1

    # Re-request on RefreshHit: CloudFront returns stale data and revalidates
    # in the background. A follow-up request gets fresh data from origin.
    if "refreshhit" in cache_status.lower() and status_code == 200:
        try:
            resp2 = await session.get(url, headers={"User-Agent": "Mozilla/5.0"})
            recv_ms = now_ms()
            body2 = resp2.body
            cache_status2 = resp2.header("X-Cache", "-")
            if resp2.status_code == 200:
                body = body2
                cache_status = cache_status2
            stats["total"][stat_key] += 1
            stats["cache"][stat_key][cache_status2] = stats["cache"][stat_key].get(cache_status2, 0) + 1
        except Exception:
            pass

    if status_code != 200:
        return known_max_id, cache_status

    articles = extract_articles(body)
    if not articles:
        return known_max_id, cache_status

    latest = max(articles, key=lambda a: a["id"])
    art_id: int = latest["id"]

    if art_id > known_max_id and emit:
        # Catalog path does NOT return releaseDate/publishDate.
        # Latency cannot be computed; we log when we saw it.
        title = latest.get("title", "?")
        code = latest.get("code", "?")
        log.info(
            f"🚀 NEW [CMS_CATALOG]  cat={catalog_id}  id={art_id}  code={code}  "
            f"cache={cache_status}  ps={page_size}  "
            f"{title}"
        )
        notifier.announcement(
            channel="CMS_CATALOG",
            title=title,
            latency_ms=None,  # no publishDate available
            catalog_id=catalog_id,
            article_id=art_id,
            recv_ts_ms=recv_ms,
            extra={"cache": cache_status, "page_size": page_size, "code": code, "backend": get_backend()},
        )
        return art_id, cache_status

    if art_id > known_max_id and not emit:
        # Bootstrap: update max_id silently.
        return art_id, cache_status

    return known_max_id, cache_status


async def poll_catalog(session: aiohttp.ClientSession,
                       catalog_id: int,
                       known_max_id: int,
                       stats: dict[str, Any],
                       emit: bool = True) -> tuple[int, str]:
    """Run all cache-key requests for one catalog in parallel.

    Returns (updated_max_id, cache_status_of_best).
    When `emit=False` (bootstrap), only updates max_id without logging/notifying.
    """
    cache_keys = _generate_cache_keys()
    tasks = [
        poll_one(session, catalog_id, p, known_max_id, stats, f"PS{p['pageSize']}", emit=emit)
        for p in cache_keys
    ]
    results = await asyncio.gather(*tasks)
    if not results:
        return known_max_id, "Error"
    max_id = max(r[0] for r in results)
    # Best cache status: prefer the one with the highest id.
    best_status = "?"
    for rid, rstat in results:
        if rid == max_id:
            best_status = rstat
    return max_id, best_status


async def run_oneshot() -> dict[int, int]:
    """Single poll cycle across all catalogs, return {catalog_id: max_id}."""
    state = load_state()
    max_ids: dict[int, int] = {
        cid: int(state.get(f"cat{cid}", {}).get("max_id", 0)) if isinstance(
            state.get(f"cat{cid}"), dict) else int(state.get(f"cat{cid}", {}).get("max_id", 0))
        for cid in CATALOG_IDS
    }
    async with create_http_client(timeout=HTTP_TIMEOUT_S) as session:
        for cid in CATALOG_IDS:
            entry = state.get(f"cat{cid}", {})
            known = int(entry.get("max_id", 0)) if isinstance(entry, dict) else 0
            max_ids[cid], _ = await poll_catalog(session, cid, known, {})
    return max_ids


async def run_continuous() -> None:
    state = load_state()
    # Ensure state has a sub-dict per catalog.
    known: dict[int, int] = {}
    for cid in CATALOG_IDS:
        entry = state.get(f"cat{cid}", {}) if isinstance(state.get(f"cat{cid}"), dict) else {}
        known[cid] = int(entry.get("max_id", 0))

    stats: dict[str, Any] = {"total": {}, "cache": {}}
    cycle = 0
    prev_err = 0  # for error-notification dedup
    prev_total_reqs = 0  # total requests at last STATS

    cats_str = ",".join(str(c) for c in CATALOG_IDS)
    log.info(f"[CMS_CATALOG] announce_scanner  catalogs=[{cats_str}]  "
             f"num_keys={NUM_CACHE_KEYS}  interval={POLL_INTERVAL_S}s  state={STATE_FILE}")
    notifier.startup(
        "CMS_CATALOG",
        f"endpoint: {CMS_BASE}\n"
        f"catalogs: [{cats_str}]\n"
        f"cache keys: {NUM_CACHE_KEYS} (random 1..50)\n"
        f"backend: {get_backend()}\n"
        f"interval: {POLL_INTERVAL_S}s",
    )

    async with create_http_client(timeout=HTTP_TIMEOUT_S) as session:
        # Bootstrap: populate max_id per catalog without emitting (catch-up).
        for cid in CATALOG_IDS:
            known[cid], _ = await poll_catalog(session, cid, known[cid], stats, emit=False)
            state[f"cat{cid}"] = {"max_id": known[cid]}
        save_state(state)
        ready_str = " ".join(f"cat{c}={known[c]}" for c in CATALOG_IDS)
        log.info(f"[CMS_CATALOG] ready  {ready_str}")

        while True:
            cycle += 1
            start = now_ms()

            # Poll all catalogs in parallel (each catalog runs N random cache keys).
            tasks = [poll_catalog(session, cid, known[cid], stats) for cid in CATALOG_IDS]
            results = await asyncio.gather(*tasks)

            dirty = False
            for cid, (new_max, _) in zip(CATALOG_IDS, results):
                if new_max > known[cid]:
                    known[cid] = new_max
                    state[f"cat{cid}"] = {"max_id": new_max}
                    dirty = True
            if dirty:
                save_state(state)

            elapsed = now_ms() - start
            sleep_s = max(0, (POLL_INTERVAL_S * 1000) - elapsed) / 1000

            if cycle % int(os.environ.get("CATALOG_STATS_EVERY_CYCLES", "10")) == 0:
                total = stats["total"]
                cache = stats["cache"]
                parts = []
                for k in sorted(total):
                    c = cache.get(k, {})
                    bucket_str = " ".join(
                        f"{ck[:1].upper() if ck else '-'}:{v}"
                        for ck, v in sorted(c.items(), key=lambda kv: -kv[1])
                    )
                    parts.append(f"{k}:{total[k]}({bucket_str})")
                ready_str = " ".join(f"cat{c}={known[c]}" for c in CATALOG_IDS)
                log.info(
                    f"[CMS_CATALOG] STATS  cycle={cycle}  {ready_str}  "
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
                    log.warning(f"[CMS_CATALOG] {delta_err} new errors ({rate:.0f}% of requests, total: {current_err})")
                    notifier.reconnect("CMS_CATALOG", f"{delta_err} new errors ({rate:.0f}% rate), total {current_err}")
                elif delta_err > 0 and delta_reqs == 0:
                    # All requests failed — 100% error rate.
                    log.warning(f"[CMS_CATALOG] {delta_err} new errors (100% of requests, total: {current_err})")
                    notifier.reconnect("CMS_CATALOG", f"{delta_err} new errors (100% rate), total {current_err}")
                prev_err = current_err
                prev_total_reqs = current_total

            await asyncio.sleep(sleep_s)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Catalog CMS poller for announce_scanner")
    parser.add_argument("--oneshot", action="store_true", help="single check, print + exit")
    parser.add_argument("--reset", action="store_true", help="reset stored state")
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
        log.info("[CMS_CATALOG] stopped")
        notifier.shutdown("CMS_CATALOG", "interrupted by user")
        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(asyncio.sleep(1.0))
            loop.run_until_complete(notifier.close())
            loop.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
