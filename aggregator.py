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

FLUSH_INTERVAL_S = 60  # flush aggregated stats every 60 seconds.


def _fmt_recv(epoch_ms: int) -> str:
    """epoch ms → HH:MM:SS.mmm (UTC)."""
    if epoch_ms < 1_000_000_000_000:
        return str(epoch_ms)
    try:
        dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        return dt.strftime("%H:%M:%S.") + f"{epoch_ms % 1000:03d}"
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


def _group_key(ev: DetectionEvent) -> tuple[str, str]:
    return (
        str(ev.catalog_id) if ev.catalog_id is not None else "",
        ev.title,
    )


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
    # Flush loop
    # ------------------------------------------------------------------ #
    def start(self) -> None:
        """Start the periodic flush loop (call from running asyncio loop)."""
        try:
            loop = __import__("asyncio").get_running_loop()
        except RuntimeError:
            return
        self._task = loop.create_task(self._flush_loop())
        log.info("[aggregator] flush loop started (interval=%ds)", FLUSH_INTERVAL_S)

    async def _flush_loop(self) -> None:
        import asyncio
        while True:
            await asyncio.sleep(FLUSH_INTERVAL_S)
            try:
                await self.flush()
            except Exception as e:  # noqa: BLE001
                log.warning("[aggregator] flush error: %r", e)

    async def flush(self) -> None:
        """Drain buffer, group by news item, send one aggregated msg per group."""
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

        groups: dict[tuple[str, str], list[DetectionEvent]] = defaultdict(list)
        for ev in events:
            groups[_group_key(ev)].append(ev)

        for grouper, group_events in groups.items():
            try:
                text = self._format_group(group_events)
                self._notifier._enqueue(text)
            except Exception as e:  # noqa: BLE001
                log.warning("[aggregator] failed to send group %s: %r", grouper, e)

    # ------------------------------------------------------------------ #
    # Formatting
    # ------------------------------------------------------------------ #
    def _format_group(self, events: list[DetectionEvent]) -> str:
        events.sort(key=lambda e: e.recv_ts_ms or 0)
        earliest = events[0].recv_ts_ms or 0

        # Title from first event (all grouped items share the same title+catalog).
        raw_title = events[0].title or "(unknown)"
        title_safe = _escape_html(raw_title[:200] + ("…" if len(raw_title) > 200 else ""))

        # Dynamic channel column width.
        max_ch = max((len(ev.channel) for ev in events), default=4)
        ch_width = max(max_ch, len("канал"))

        lines = [f"🚀 <b>{title_safe}</b>", ""]
        lines.append(f"<pre>{'канал':<{ch_width}}  recv              delta_ms")

        for ev in events:
            recv_str = _fmt_recv(ev.recv_ts_ms or 0)
            delta = (ev.recv_ts_ms or 0) - earliest
            lines.append(f"{ev.channel:<{ch_width}}  {recv_str:>16}  {delta} ms")

        lines.append("</pre>")
        return "\n".join(lines)


aggregator = Aggregator()
