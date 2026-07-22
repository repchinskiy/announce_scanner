Project name: announce_scanner
Goal:
Build a Python-based system running on an AWS Tokyo Ubuntu VPS that detects new cryptocurrency listings on Binance as fast as possible, with end‑to‑end latency <150 ms (ideal: <100 ms) from Binance’s announcement time to our detection time, with at least console output and later Telegram notifications.

1. Roles: architects vs agent
We (architects / tech leads) define:

system architecture and data sources;

latency targets and constraints;

step‑by‑step tasks and prompts for the ZCode agent;

acceptance criteria and performance evaluation.

ZCode agent (GLM‑5.2) is responsible for:

reading docs and finding APIs;

writing and modifying Python code;

configuring the VPS environment;

iteratively improving the system based on our feedback.

We always work iteratively: one focused prompt → one concrete task, then review and adjustments, then the next prompt.

2. Hard priorities
Detection speed is absolute priority.

If we must choose between clean architecture / pretty UI and 10–20 ms of extra latency, we choose speed.

UI, code style, and “niceness” are secondary; basic logging and console output are enough.

Latency targets:

Main metric:
latency_ms = our_detect_ts_ms - binance_announcement_ts_ms.

Target: <150 ms for real listings; ideal: <100 ms.

Environment: AWS Tokyo VPS, Python 3.11+; network latency to Binance is already low and code must add minimal overhead.

Data sources and priority:

Primary technical source: Binance Announcements WebSocket (official real‑time announcement stream).

Primary business URL for listings:
https://www.binance.com/en/support/announcement/list/48 — this page is the business source we care about (New Cryptocurrency Listing category).

Secondary source: Binance CMS JSON (REST endpoints that drive announcement pages, including category 48).

Third‑party providers (CryptoListing.ws etc.): considered only on free plans and only as auxiliary/reference channels, not core.

Resource constraints:

Free solutions only: no paid subscriptions for external feeds.

Model for development: GLM‑5.2 in ZCode free tier; use it for reasoning and coding, not in the hot latency path.

3. Technical principles for the agent
3.1 Stack and environment
Language: Python 3.11+ on Ubuntu (AWS Tokyo VPS).

Async stack: asyncio + (later) uvloop, and websockets or aiohttp for WebSocket connections.

Keep the “hot path” from a WebSocket frame to a detection event extremely thin (timestamping + minimal processing + output/notification).

3.2 Working with sources
HTML list/48 page:

Agent must treat https://www.binance.com/en/support/announcement/list/48 as the business root of “New Cryptocurrency Listings”.

Direct HTML scraping must not be the primary detection method because of Cloudflare caching and rendering delays; it’s for validation/comparison and monitoring.

Binance Announcements WebSocket:

Agent must find the official WebSocket endpoint and message schema for the announcements stream.

The client must maintain a long‑lived WebSocket connection, not reconnect per event.

Binance CMS JSON:

Agent must discover the REST JSON endpoint that returns the announcements list for category 48 or equivalent, based on patterns like bapi/composite/v1/public/cms/article/list/query.

CMS REST poller is a fallback and monitoring channel, not part of the main detection hot path.

4. Prompt rules for ZCode agent
One prompt = one focused task.

Do not overload a single prompt with five different tasks; we sequence them: “find WS”, “build minimal client”, “add latency”, “add Telegram”, etc.

Each prompt must:

Explicitly mention the list/48 URL when the task relates to listings or business logic.

Clearly restate our latency goals and that speed is priority.

Include acceptance criteria (how we will decide the task is done correctly).

Avoid over‑engineering.

Ask for simple, minimal implementations that we can optimize later.

If the agent tries to introduce heavy frameworks or complex abstractions, we correct it via follow‑up prompts.

UDP / low‑level protocols:

It’s fine to explore whether UDP or custom protocols make sense, but any decision must be justified and must not break our need for stability and simplicity.

5. How the agent should treat latency
Operations in the “hot path” (WS handler → event creation → console print / Telegram send) must:

be as light as possible on CPU;

not block the event loop;

move heavy work (LLM analysis, complex parsing, database writes) into background workers.

The agent must:

record received_ts_ms for each message;

extract publish_ts_ms or equivalent from Binance payload/JSON when available;

compute latency;

output latency to console for each event at least in early versions.

6. Project evolution (for us)
We will:

Use ZCode agent to discover and connect to the Announcements WebSocket, always in the context of list/48 as the business page we care about.

Add timestamping and latency calculation + console output.

Add Telegram notifications.

Add CMS REST poller as fallback and latency comparison channel.

Only then experiment with GLM‑5.2 for semantic classification, importance scoring, etc.