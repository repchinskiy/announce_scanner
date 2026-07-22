#!/usr/bin/env python3
"""
announce_ws_parser_test.py — offline unit test for the hot-path parser of
announce_ws_client.py, using the EXACT DATA-frame schema documented by
Binance (developers.binance.com/en/docs/products/announcements/announcement).

This is NOT a live capture (a live DATA frame requires a valid read-only
Binance API key, which the user must provision; testnet has no sapi/wss).
It only proves that the parser/hot path in announce_ws_client.py correctly
handles the documented payload shape, including the catalogId=48 filter and
the latency_ms computation.

Run:
    python3 announce_ws_parser_test.py
Exits 0 if all assertions pass, 1 otherwise.
"""

from __future__ import annotations

import json
import sys

# Import the hot-path function from the main client.
import importlib
import announce_ws_client as client  # noqa: E402

import io
import contextlib


def _make_data_frame(catalog_id: int, title: str, publish_ts_ms: int) -> str:
    """Build a WS text frame exactly like the docs' announcement example."""
    ann = {
        "catalogId": catalog_id,
        "catalogName": "New Cryptocurrency Listing" if catalog_id == 48 else "Delisting",
        "publishDate": publish_ts_ms,
        "title": title,
        "body": "This is...",
        "disclaimer": "Trade on-the-go...",
    }
    msg = {
        "type": "DATA",
        "topic": "com_announcement_en",
        "data": json.dumps(ann),  # data is a JSON STRING, as documented
    }
    return json.dumps(msg)


def _capture_stdout(raw_frame: str) -> dict:
    """Run handle_message and capture the emitted stdout JSON line."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        client.handle_message(raw_frame)
    out = buf.getvalue().strip()
    assert out, "handle_message emitted nothing to stdout"
    return json.loads(out)


def main() -> int:
    failures: list[str] = []

    # 1. catalogId=48 listing -> must be emitted (matches list/48).
    publish_ts = 1784303104983
    raw = _make_data_frame(48, "Binance Will List Aerodrome (AERO) with Seed Tag Applied", publish_ts)
    rec = _capture_stdout(raw)
    assert_eq(rec["catalog_id"], 48, "catalog_id", failures)
    assert_eq(rec["catalog_name"], "New Cryptocurrency Listing", "catalog_name", failures)
    assert_eq(rec["publish_ts_ms"], publish_ts, "publish_ts_ms", failures)
    assert_eq(rec["topic"], "com_announcement_en", "topic", failures)
    assert_eq(rec["title"], "Binance Will List Aerodrome (AERO) with Seed Tag Applied", "title", failures)
    # latency_ms = received - publish; received is 'now' so latency >= 0 and finite.
    if not (isinstance(rec["latency_ms"], int) and rec["latency_ms"] >= 0):
        failures.append(f"latency_ms invalid: {rec.get('latency_ms')!r}")
    # announcement inner object preserved.
    if rec["announcement"].get("catalogId") != 48:
        failures.append("announcement.catalogId not preserved")

    # 2. catalogId != 48 (e.g. Delisting=161) -> must be filtered OUT (no stdout).
    raw2 = _make_data_frame(161, "Notice of Delisting", publish_ts)
    buf2 = io.StringIO()
    with contextlib.redirect_stdout(buf2):
        client.handle_message(raw2)
    if buf2.getvalue().strip():
        failures.append("non-48 catalog leaked to stdout despite ANN_CATALOG_IDS=48")

    # 3. COMMAND ack -> must NOT emit a DATA record to stdout (only logs).
    cmd = json.dumps({"type": "COMMAND", "subType": "REGISTER", "code": None, "data": "SUCCESS"})
    buf3 = io.StringIO()
    with contextlib.redirect_stdout(buf3):
        client.handle_message(cmd)
    if buf3.getvalue().strip():
        failures.append("COMMAND ack leaked to stdout as a data record")

    if failures:
        print("FAIL")
        for f in failures:
            print(" -", f)
        return 1
    print("PASS — parser handles documented DATA schema, catalogId=48 filter, and COMMAND acks")
    return 0


def assert_eq(actual, expected, name, failures):
    if actual != expected:
        failures.append(f"{name}: expected {expected!r}, got {actual!r}")


if __name__ == "__main__":
    sys.exit(main())
