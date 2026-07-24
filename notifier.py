#!/usr/bin/env python3
"""
notifier.py — async Telegram notifier for announce_scanner

Sends notifications to a Telegram chat on:
  - service startup / shutdown (liveness signal)
  - new announcement detection (with latency)

Config (read from environment, usually via .env):
  TELEGRAM_BOT_TOKEN  — Telegram bot token from @BotFather
  TELEGRAM_CHAT_ID    — target chat id (negative for groups)

If either var is missing, the notifier becomes a no-op (logs a warning once
and never blocks). This lets the same code run in CI / local dev without
Telegram configured.

Architecture (queue + single worker):
  All send_* methods enqueue text messages into an asyncio.Queue. A single
  background worker (_send_loop) drains the queue sequentially, enforcing:
    - MIN_INTERVAL_S between sends (throttle, ~2 msg/sec — safe for groups)
    - 429 retry_after: when Telegram returns 429 with retry_after=N, the
      worker pauses for N+1 seconds before sending the next message.
  This prevents the "burst → 429 flood → more retries → longer ban" loop
  that occurred when messages were fired in parallel.

Hot path safety:
  All send_* methods are fire-and-forget: they put on the queue and return
  immediately. Network IO happens in the background worker and never blocks
  the announcement detection loop.

Usage:
    from notifier import notifier
    notifier.startup("WS", "filter: 48,49")
    notifier.shutdown("WS")
    notifier.announcement(channel="WS", title="...", latency_ms=21, ...)
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp

from log_setup import get_logger
log = get_logger()

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
HTTP_TIMEOUT_S = 5.0
MAX_RETRIES = 2          # per-message retries before giving up on that message
RETRY_BACKOFF_S = 1.0    # backoff between retries (not 429-related)
MAX_TITLE_LEN = 200      # Telegram message cap safety

# Throttle: minimum interval between sends to Telegram.
# Telegram group rate limit ~20 msg/min; bot global limit ~30 msg/sec.
# 0.5s = 2 msg/sec is safe for a single chat.
MIN_INTERVAL_S = 0.5

# Max queue size. If exceeded (worker is stuck / Telegram is down for a long
# time), oldest messages are dropped with a warning to avoid unbounded memory.
QUEUE_MAXSIZE = 200


def _fmt_ts(epoch_ms: int | None) -> str:
    """Format epoch ms as ``raw | HH:MM:SS.mmm`` (UTC).

    Returns ``epoch_ms`` unchanged when *epoch_ms* is ``None`` or when the
    timestamp would be unreasonably small (<1e10 → likely not epoch ms).
    """
    if epoch_ms is None:
        return ""
    # Sanity: epoch ms in year 2025+ should be > 1.7e12.
    if epoch_ms < 1_000_000_000_000:
        return f"{epoch_ms}"
    try:
        dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        return f"{epoch_ms} | {dt.strftime('%H:%M:%S.')}{epoch_ms % 1000:03d}"
    except (OSError, OverflowError, ValueError):
        return f"{epoch_ms}"


class Notifier:
    """Async Telegram notifier with a queue + single throttled worker.

    A single Notifier instance is shared across the process (module-level
    `notifier`). Env vars are read lazily on first use (not at import time),
    so a .env file loaded by the importer before the first send call is honoured.
    """

    def __init__(self) -> None:
        self.token: str | None = None
        self.chat_id: str | None = None
        self.enabled: bool = False
        self._initialised: bool = False
        self._session: aiohttp.ClientSession | None = None
        self._warned_disabled = False
        self._component: str = "announce_scanner"

        # Queue + worker state
        self._queue: asyncio.Queue[str | None] | None = None
        self._worker_task: asyncio.Task | None = None
        self._paused_until: float = 0.0  # epoch seconds; worker sleeps until this

    def _ensure_initialised(self) -> None:
        """Read env vars on first use (after .env is loaded)."""
        if self._initialised:
            return
        self._initialised = True
        self.token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        self.enabled = bool(self.token and self.chat_id)

    def _ensure_worker(self) -> None:
        """Lazily start the queue + worker on first use within a running loop."""
        if self._queue is not None:
            return  # already initialised
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return  # no running loop — can't start worker (sync context)
        self._queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
        self._worker_task = loop.create_task(self._send_loop())

    # ------------------------------------------------------------------ #
    # Session lifecycle
    # ------------------------------------------------------------------ #
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_S),
            )
        return self._session

    async def close(self) -> None:
        # Signal worker to stop (sentinel = None).
        if self._queue is not None:
            try:
                self._queue.put_nowait(None)
            except asyncio.QueueFull:
                pass  # queue full; worker will drain then exit on empty
        if self._worker_task is not None:
            try:
                await asyncio.wait_for(self._worker_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._worker_task.cancel()
            self._worker_task = None
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        self._queue = None

    def _disabled_warn_once(self) -> None:
        if not self._warned_disabled:
            log.warning("Telegram notifier disabled: set TELEGRAM_BOT_TOKEN and "
                        "TELEGRAM_CHAT_ID to enable")
            self._warned_disabled = True

    # ------------------------------------------------------------------ #
    # Worker (single-threaded send loop with throttle + 429 backoff)
    # ------------------------------------------------------------------ #
    async def _send_loop(self) -> None:
        """Background worker: drain queue, send one message at a time.

        Respects:
          - MIN_INTERVAL_S between sends (throttle)
          - _paused_until (set when Telegram returns 429 with retry_after)
        """
        assert self._queue is not None
        while True:
            text = await self._queue.get()
            if text is None:
                # Sentinel: shutdown signal.
                self._queue.task_done()
                return

            # Respect 429 pause if active.
            now = time.monotonic()
            if self._paused_until > now:
                pause = self._paused_until - now
                log.info(f"[notifier] 429 pause: sleeping {pause:.0f}s before next send")
                await asyncio.sleep(pause)

            await self._send_one(text)
            self._queue.task_done()

            # Throttle: min interval between sends.
            await asyncio.sleep(MIN_INTERVAL_S)

    async def _send_one(self, text: str) -> None:
        """Send one message with retry. Updates _paused_until on 429."""
        url = TELEGRAM_API.format(token=self.token)
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        session = await self._get_session()
        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        return
                    body = await resp.text()
                    if resp.status == 429:
                        # Parse retry_after from Telegram's JSON response.
                        retry_after = self._parse_retry_after(body)
                        if retry_after:
                            self._paused_until = time.monotonic() + retry_after + 1
                            log.warning(f"telegram 429: retry_after={retry_after}s, "
                                        f"pausing worker for {retry_after + 1}s")
                        else:
                            # No retry_after in body; use default backoff.
                            self._paused_until = time.monotonic() + 5.0
                            log.warning(f"telegram 429 (no retry_after): pausing 5s")
                        return  # don't retry within this call; worker will pause
                    if 400 <= resp.status < 500:
                        log.warning(f"telegram {resp.status}: {body[:200]}")
                        return
                    last_exc = RuntimeError(f"telegram {resp.status}: {body[:200]}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc

            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BACKOFF_S * (attempt + 1))

        log.warning(f"telegram send failed after {MAX_RETRIES + 1} attempts: {last_exc!r}")

    @staticmethod
    def _parse_retry_after(body: str) -> int | None:
        """Extract retry_after (seconds) from Telegram 429 JSON body."""
        try:
            data = json.loads(body)
            params = data.get("parameters", {})
            ra = params.get("retry_after")
            if isinstance(ra, (int, float)):
                return int(ra)
        except (json.JSONDecodeError, TypeError, KeyError):
            pass
        return None

    # ------------------------------------------------------------------ #
    # Public helpers (enqueue, fire-and-forget)
    # ------------------------------------------------------------------ #
    def _enqueue(self, text: str) -> None:
        """Enqueue a message for the worker. Drops oldest if queue is full."""
        self._ensure_initialised()
        if not self.enabled:
            self._disabled_warn_once()
            return
        self._ensure_worker()
        if self._queue is None:
            return  # no running loop; can't enqueue
        try:
            self._queue.put_nowait(text)
        except asyncio.QueueFull:
            # Queue is full (worker stuck or Telegram down for long).
            # Drop the oldest message to make room.
            try:
                dropped = self._queue.get_nowait()
                self._queue.task_done()
                log.warning(f"[notifier] queue full; dropped oldest message "
                            f"({len(dropped)} chars)")
            except asyncio.QueueEmpty:
                pass
            try:
                self._queue.put_nowait(text)
            except asyncio.QueueFull:
                log.error("[notifier] queue still full after drop; message lost")

    def startup(self, component: str, detail: str = "") -> None:
        """🟢 service started — call from the run() coroutine."""
        text = f"🟢 <b>{component}</b> started"
        if detail:
            text += f"\n<pre>{detail}</pre>"
        self._enqueue(text)

    def shutdown(self, component: str, detail: str = "") -> None:
        """🔴 service stopped — call on graceful exit / KeyboardInterrupt."""
        text = f"🔴 <b>{component}</b> stopped"
        if detail:
            text += f"\n<pre>{detail}</pre>"
        self._enqueue(text)

    def reconnect(self, component: str, reason: str = "") -> None:
        """connection issue — rate-limit, HTTP error, or elevated error rate.

        Icon is chosen based on *reason* content:
          ⏳ — 429 rate-limited
          🔴 — HTTP error (403, 500, …) or network error
          🟡 — elevated error rate (aggregated stats check)
        """
        if "429" in reason:
            icon = "\u23F3"  # ⏳
        elif "HTTP" in reason or "new errors" in reason.lower():
            icon = "\U0001F534"  # 🔴
        else:
            icon = "\U0001F7E1"  # 🟡
        text = f"{icon} <b>{component}</b> {reason}" if reason else f"{icon} <b>{component}</b> reconnecting"
        self._enqueue(text)

    def announcement(self,
                     channel: str,
                     title: str,
                     latency_ms: int | None,
                     catalog_id: int | None = None,
                     article_id: int | None = None,
                     publish_ts_ms: int | None = None,
                     recv_ts_ms: int | None = None,
                     detect_ts_ms: int | None = None,
                     dispatch_ts_ms: int | None = None,
                     extra: dict[str, Any] | None = None) -> None:
        """🚀 new announcement detected.

        channel: "WS", "CMS_APEX", "CMS_COMPOSITE", "EXCHANGE", "SNIPER", "CLWS", or "CLWD"

        Timestamps (all epoch milliseconds):
          publish_ts_ms   — Binance publish time (from Binance payload: publishDate / releaseDate)
          detect_ts_ms    — Source-side detection time (e.g. CryptoListing detectedTimestampUs)
          dispatch_ts_ms  — Source-side dispatch time (e.g. CryptoListing dispatchTimestampUs)
          recv_ts_ms      — Our local receive time (when bytes hit our socket)
        """
        # Latency string (no icon — lives inside <pre>).
        if latency_ms is None:
            latency_str = "N/A"
        elif latency_ms < 100:
            latency_str = f"{latency_ms} ms"
        elif latency_ms < 150:
            latency_str = f"{latency_ms} ms"
        else:
            latency_str = f"{latency_ms} ms"

        title_short = title[:MAX_TITLE_LEN] + ("…" if len(title) > MAX_TITLE_LEN else "")
        # Escape HTML special chars in user content.
        title_safe = (title_short
                      .replace("&", "&amp;")
                      .replace("<", "&lt;")
                      .replace(">", "&gt;"))

        lines = [
            f"🚀 <b>NEW [{channel}]</b>",
            f"<pre>latency: <b>{latency_str}</b>",
        ]
        # Timestamps block (all epoch ms, one per line, with HH:MM:SS.mmm).
        if publish_ts_ms is not None:
            lines.append(f"publish: {_fmt_ts(publish_ts_ms)}")
        if detect_ts_ms is not None:
            lines.append(f"detect: {_fmt_ts(detect_ts_ms)}")
        if dispatch_ts_ms is not None:
            lines.append(f"dispatch: {_fmt_ts(dispatch_ts_ms)}")
        if recv_ts_ms is not None:
            lines.append(f"recv: {_fmt_ts(recv_ts_ms)}")
        if catalog_id is not None:
            lines.append(f"catalog: {catalog_id}")
        if article_id is not None:
            lines.append(f"id: {article_id}")
        if extra:
            for k, v in extra.items():
                lines.append(f"{k}: {v}")
        lines.append(f"{title_safe}</pre>")

        self._enqueue("\n".join(lines))


# Module-level singleton used by both channels.
notifier = Notifier()
