#!/usr/bin/env python3
"""
aggregator.py — aggregates announcement detection events across channels.

Two parallel paths per detection event:

  1. IMMEDIATE (speed):  notifier.announcement() is called right away so
     the channel fires a TG notification instantly — critical for latency.

  2. BUFFERED STATS:     event is buffered. Every FLUSH_INTERVAL_S seconds
     a flush drains the buffer, groups by news item (catalog_id + title),
     and sends one aggregated Telegram message per group:
       - header: title (shown once)
       - table: channel | recv | delta_ms, sorted ascending by delta.

Usage (channels call once, aggregator handles both paths):
    aggregator.announcement(
        channel="WS", title="...", latency_ms=21,
        recv_ts_ms=..., catalog_id=48, ...
    )
"""

from __future__ import annotations

import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

from log_setup import get_logger
log = get_logger()

FLUSH_WINDOW_S = 120  # flush 120 seconds after first event in empty buffer.


def _fmt_recv(epoch_ms: int) -> str:
    """epoch ms → SS.mmm (seconds.milliseconds only)."""
    if epoch_ms < 1_000_000_000_000:
        return str(epoch_ms)
    try:
        dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        return f"{dt.second:02d}.{epoch_ms % 1000:03d}"
    except (OSError, OverflowError, ValueError):
        return str(epoch_ms)


def _escape_html(text: str) -> str:
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))


@dataclass
class DetectionEvent:
    channel: str = ""
    title: str = ""
    latency_ms: int | None = None
    catalog_id: int | None = None
    article_id: int | None = None
    publish_ts_ms: int | None = None
    recv_ts_ms: int | None = None
    detect_ts_ms: int | None = None
    dispatch_ts_ms: int | None = None
    extra: dict[str, Any] | None = None


def _best_title(events: list[DetectionEvent]) -> str:
    """Pick the best title from events for display.
    
    Priority:
    1. CLWS/CLWD extra["title"] (real title from CryptoListing feed)
    2. WS/CMS title (real Binance title)
    3. Fallback to first event's title
    """
    # CLWS/CLWD store real title in extra["title"]
    for ev in events:
        if ev.channel in ("CLWS", "CLWD") and ev.extra and ev.extra.get("title"):
            return ev.extra["title"]
    # WS/CMS have real title directly
    for ev in events:
        if ev.channel in ("WS", "CMS_APEX", "CMS_CATALOG", "CMS_COMPOSITE") and ev.title:
            return ev.title
    # Fallback
    return events[0].title if events else "(unknown)"


class Aggregator:
    """Buffers detection events and flushes aggregated stats periodically.

    announcement() itself calls notifier.announcement() for immediate
    Telegram notification (keep detection speed). It also buffers for
    periodic aggregated stats.
    """

    def __init__(self) -> None:
        self._buf: list[DetectionEvent] = []
        self._notifier: Any = None  # resolved lazily
        self._task: Any = None
        self._flush_scheduled: Any = None  # scheduled flush task handle

    # ------------------------------------------------------------------ #
    # Fire immediately + buffer for stats
    # ------------------------------------------------------------------ #
    def announcement(
        self,
        channel: str = "",
        title: str = "",
        latency_ms: int | None = None,
        catalog_id: int | None = None,
        article_id: int | None = None,
        publish_ts_ms: int | None = None,
        recv_ts_ms: int | None = None,
        detect_ts_ms: int | None = None,
        dispatch_ts_ms: int | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        # Path 1: immediate individual TG notification (speed-critical).
        self._notify_immediate(
            channel=channel,
            title=title,
            latency_ms=latency_ms,
            catalog_id=catalog_id,
            article_id=article_id,
            publish_ts_ms=publish_ts_ms,
            recv_ts_ms=recv_ts_ms,
            detect_ts_ms=detect_ts_ms,
            dispatch_ts_ms=dispatch_ts_ms,
            extra=extra,
        )
        # Path 2: buffer for aggregated stats.
        self._buf.append(DetectionEvent(
            channel=channel,
            title=title,
            latency_ms=latency_ms,
            catalog_id=catalog_id,
            article_id=article_id,
            publish_ts_ms=publish_ts_ms,
            recv_ts_ms=recv_ts_ms,
            detect_ts_ms=detect_ts_ms,
            dispatch_ts_ms=dispatch_ts_ms,
            extra=extra,
        ))
        # Schedule flush if buffer was empty (first event in window).
        self._schedule_flush_if_needed()

    def _notify_immediate(self, **kwargs: Any) -> None:
        """Enqueue individual TG notification for speed.

        Uses notifier._enqueue() directly (bypasses queue wait) so the
        message is queued immediately and dispatched by the worker as
        soon as it can. This is fire-and-forget — does not block.
        """
        try:
            from notifier import notifier as _n
        except Exception:  # noqa: BLE001
            return

        latency = kwargs.get("latency_ms")
        if latency is None:
            latency_str = "N/A"
        elif latency < 100:
            latency_str = f"{latency} ms"
        elif latency < 150:
            latency_str = f"{latency} ms"
        else:
            latency_str = f"{latency} ms"

        title_short = (kwargs.get("title") or "")[:200]
        title_safe = _escape_html(title_short + ("…" if len(kwargs.get("title") or "") > 200 else ""))

        lines = [
            f"🚀 <b>NEW [{kwargs.get('channel', '')}]</b>",
            f"<pre>latency: <b>{latency_str}</b>",
        ]
        for label, val in [
            ("publish", kwargs.get("publish_ts_ms")),
            ("detect", kwargs.get("detect_ts_ms")),
            ("dispatch", kwargs.get("dispatch_ts_ms")),
            ("recv", kwargs.get("recv_ts_ms")),
        ]:
            if val is not None:
                lines.append(f"{label}: {_fmt_recv(val)}")

        if kwargs.get("catalog_id") is not None:
            lines.append(f"catalog: {kwargs['catalog_id']}")
        if kwargs.get("article_id") is not None:
            lines.append(f"id: {kwargs['article_id']}")
        extra = kwargs.get("extra")
        if extra:
            for k, v in extra.items():
                lines.append(f"{k}: {v}")

        lines.append(f"{title_safe}</pre>")
        _n._enqueue("\n".join(lines))

    # ------------------------------------------------------------------ #
    # Flush scheduling (120-second window after first event)
    # ------------------------------------------------------------------ #
    def _schedule_flush_if_needed(self) -> None:
        """If buffer had 1 event (was empty before this event), schedule flush in 120s."""
        if len(self._buf) == 1 and self._flush_scheduled is None:
            try:
                loop = __import__("asyncio").get_running_loop()
            except RuntimeError:
                return
            self._flush_scheduled = loop.create_task(self._delayed_flush())
            log.info("[aggregator] first event in window, flush scheduled in %ds", FLUSH_WINDOW_S)

    async def _delayed_flush(self) -> None:
        """Wait 120s then flush buffer."""
        import asyncio
        await asyncio.sleep(FLUSH_WINDOW_S)
        try:
            await self.flush()
        except Exception as e:  # noqa: BLE001
            log.warning("[aggregator] flush error: %r", e)
        finally:
            self._flush_scheduled = None

    # ------------------------------------------------------------------ #
    # Flush loop (no-op now, flush is event-driven)
    # ------------------------------------------------------------------ #
    def start(self) -> None:
        """No-op: flush is now event-driven (120s window after first event)."""
        log.info("[aggregator] event-driven flush enabled (window=%ds)", FLUSH_WINDOW_S)

    async def flush(self) -> None:
        """Drain buffer, send one aggregated msg with all events in window."""
        if not self._buf:
            return

        events, self._buf = self._buf, []

        if self._notifier is None:
            try:
                from notifier import notifier as _n
                self._notifier = _n
            except Exception:  # noqa: BLE001
                log.warning("[aggregator] notifier unavailable, dropping %d stats events", len(events))
                return

        try:
            text = self._format_group(events)
            self._notifier._enqueue(text)
        except Exception as e:  # noqa: BLE001
            log.warning("[aggregator] failed to send aggregated stats: %r", e)

    # ------------------------------------------------------------------ #
    # Formatting
    # ------------------------------------------------------------------ #
    def _extract_ps(self, ev: DetectionEvent) -> str | None:
        """Return PS/preset label for CMS channels from extra data."""
        if not ev.extra:
            return None
        if ev.channel == "CMS_APEX":
            return ev.extra.get("key")
        if ev.channel in ("CMS_CATALOG", "CMS_COMPOSITE"):
            ps = ev.extra.get("page_size")
            return f"PS{ps}" if ps is not None else None
        return None

    def _format_group(self, events: list[DetectionEvent]) -> str:
        events.sort(key=lambda e: e.recv_ts_ms or 0)
        earliest = events[0].recv_ts_ms or 0

        raw_title = _best_title(events)
        title_safe = _escape_html(raw_title[:200] + ("…" if len(raw_title) > 200 else ""))

        max_ch = max((len(ev.channel) for ev in events), default=4)
        ch_width = max(max_ch, len("канал"))

        # Have CMS rows? If yes, add a PS column.
        has_ps = any(self._extract_ps(ev) is not None for ev in events)
        ps_col = " PS" if has_ps else ""

        lines = [f"🚀 <b>{title_safe}</b>", ""]
        lines.append(f"<pre>{'канал':<{ch_width}} recv     delta{ps_col}")

        for ev in events:
            recv_str = _fmt_recv(ev.recv_ts_ms or 0)
            delta = (ev.recv_ts_ms or 0) - earliest
            ps = self._extract_ps(ev)
            ps_str = f" {ps}" if ps else (" " if has_ps else "")
            lines.append(f"{ev.channel:<{ch_width}} {recv_str:<8} {delta:<5}{ps_str}")

        lines.append("</pre>")
        return "\n".join(lines)


aggregator = Aggregator()
