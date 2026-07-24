# announce_scanner

Detects new cryptocurrency listing announcements on Binance as fast as possible.

**Business source:** https://www.binance.com/en/support/announcement/list/48
(`catalogId=48` — "New Cryptocurrency Listing")

**Target latency:** `<150 ms` from Binance publish time to local detection
(ideal: `<100 ms`).

**Environment:** AWS Tokyo VPS, Ubuntu, Python 3.11+.

By default only `catalogId=48` (New Cryptocurrency Listing) is monitored.
Additional categories can be opted in via `ANN_CATALOG_IDS`:

| catalogId | Category |
|-----------|----------|
| 48 | New Cryptocurrency Listing |
| 49 | Latest Binance News |
| 50 | Latest Activities |
| 51 | Delistings |
| 93 | Latest Activities (alt) |
| 128 | P2P Merchant Announcements |
| 157 | Margin / Futures listings |
| 161 | Earn / Staking |

---

## Architecture

Eight independent channels run in parallel. Each writes detections to stdout
with a tag (`WS`, `WS2`, `CMS_APEX`, `CMS_CATALOG`, `CMS_COMPOSITE`,
`EXCHANGE`, `SNIPER`, `CLWS`/`CLWD`), so real listings can be compared
across sources.

```
┌──────────────────────────────────────────────────────────────────────────┐
│                             announce_scanner                             │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  announce_ws_client.py       ←── WS push API ──→  target <100 ms         │
│  (primary channel)                wss://api.binance.com/sapi/wss         │
│                                   HMAC-SHA256 signed subscription        │
│                                   topic: com_announcement_en             │
│                                   filter: catalogId in WS_CATALOG_IDS    │
│                                                                          │
│  announce_ws2_client.py      ←── WS push (pub) ──→  target <100 ms       │
│  (secondary WS channel)          wss://stream.binance.com:9443/ws/       │
│                                   !announcements@arr (undocumented)      │
│                                   no auth, public stream                 │
│                                                                          │
│  cms_apex_poller.py          ←── CMS REST apex ──→  fallback ~60-120s    │
│  (fallback / comparison)         /bapi/apex/.../article/list/query       │
│                                   CloudFront-cached, 5 pageSizes         │
│                                   adaptive interval (base + burst)       │
│                                   state: ./state/cms_state.json          │
│                                                                          │
│  cms_apex_catalog_poller.py  ←── CMS REST catalog ──→  comparison        │
│  (catalog endpoint)              /bapi/apex/.../catalog/list/query       │
│                                   flat articles, no releaseDate          │
│                                   state: ./state/catalog_state.json      │
│                                                                          │
│  cms_composite_poller.py     ←── CMS REST composite ──→  comparison      │
│  (cache-behavior comparison)      /bapi/composite/.../catalog/list       │
│                                   random pageSize 10..49 cache-buster    │
│                                   60s interval (Binance IP-ban limit)    │
│                                   state: ./state/composite_state.json    │
│                                                                          │
│  exchangeinfo_poller.py      ←── market data ──→  ~1-5 s                 │
│  (backup / early signal)         GET /api/v3/exchangeInfo                │
│                                   3 hosts in parallel:                   │
│                                     api.binance.com (CloudFront/AWS)     │
│                                     api-gcp.binance.com (GCP GLB)        │
│                                     api4.binance.com (direct nginx)      │
│                                   state: ./state/exchangeinfo_state.json │
│                                                                          │
│  ticker_sniper.py            ←── price ticker ──→  ~1ms + RTT            │
│  (tradable-now signal)           GET /api/v3/ticker/price                │
│                                   lightweight ~150KB (vs 17MB exg)       │
│                                   3 hosts in parallel, default 1 Hz      │
│                                   adaptive 429 backoff                   │
│                                   state: ./state/ticker_sniper_state.json│
│                                                                          │
│  cryptolisting_client.py    ←── CL WS push ──→  0 ms / +240 ms           │
│  (co-located benchmark)          wss://cryptolisting.ws                  │
│                                   X-API-Key header (dsk_...)             │
│                                   SpeedTrial (CLWS) + FreeDelayed (CLWD) │
│                                                                          │
│  notifier.py — shared Telegram notifications (fire-and-forget)           │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### Why eight channels

- **WebSocket (WS)** is the only realistic path to `<150 ms`: it is a server
  push, no polling, no CloudFront in the hot path.
- **WebSocket 2 (WS2)** is an undocumented public stream
  (`!announcements@arr`) on `stream.binance.com:9443`. No auth required.
  Runs as an independent comparison channel alongside the signed WS.
- **CMS_APEX** (`/bapi/apex/...`) is a fallback/comparison channel.
  CloudFront-cached, bounded by CDN TTL (~60–120 s). Returns `releaseDate`
  (needed for latency calc).
- **CMS_CATALOG** (`/bapi/apex/.../catalog/list/query`) is an alternative
  apex path with flat articles. No `releaseDate` available.
- **CMS_COMPOSITE** (`/bapi/composite/...`) is a comparison-only channel.
  Same CloudFront cache behaviour as apex, but does NOT return
  `releaseDate`. Used to validate that both CMS paths cache identically.
- **exchangeInfo** polls Binance's market-data API for newly listed trading
  symbols. Polls three hosts in parallel (CloudFront, GCP GLB, direct nginx)
  for independent cache paths. Heavier payload (~17MB JSON).
- **Ticker sniper** polls `/api/v3/ticker/price` — the lightweight version
  of exchangeInfo (~150KB). Same "new symbol detected" signal, but at much
  higher frequency. This is the technique used by
  rickstaa/crypto-listings-sniper to achieve sub-300ms "tradable now"
  detection. Default 1 Hz (conservative); tunable to 100 Hz.
- **CryptoListing.ws** is a co-located third-party detector sitting in the
  same AWS Tokyo region as Binance. Benchmark tier (`CLWS`, 0 ms, title
  redacted) + auxiliary feed (`CLWD`, +240 ms, full payload).

### Channel classification by event type

Channels split into two groups by what they detect:

**Announcement events** (publication of news on Binance):
| Channel | Source | What it detects |
|---------|--------|-----------------|
| `WS` | Binance Announcements WebSocket (signed) | Push of announcement publish event |
| `WS2` | Binance `!announcements@arr` (public stream) | Push of announcement publish event (undocumented) |
| `CMS_APEX` | CMS REST (`/bapi/apex/.../list/query`) | New article in CMS (after CloudFront cache TTL) |
| `CMS_CATALOG` | CMS REST (`/bapi/apex/.../catalog/list/query`) | New article in CMS (flat, no releaseDate) |
| `CMS_COMPOSITE` | CMS REST (`/bapi/composite/...`) | New article in CMS (comparison channel) |
| `CLWS` / `CLWD` | CryptoListing.ws WebSocket | Push from co-located detector covering listings, delistings, airdrops, monitoring tag changes |

**Market data events** (symbol appears in trading infrastructure):
| Channel | Source | What it detects |
|---------|--------|-----------------|
| `EXCHANGE` | `GET /api/v3/exchangeInfo` | New trading pair registered (symbol added) |
| `SNIPER` | `GET /api/v3/ticker/price` | Matching engine started publishing prices for a pair (tradable now) |

### Detection event ordering (depends on listing type)

Timeline of events for a typical listing:

```
T=0    Binance publishes announcement
       ├─ WS receives push (signed)                ← <100ms
       ├─ WS2 receives push (!announcements@arr)   ← <100ms
       ├─ CMS_APEX sees in REST (after cache)      ← ~60-120s
       ├─ CMS_CATALOG sees in REST                 ← ~60-120s
       ├─ CMS_COMPOSITE sees in REST               ← ~60-120s
       └─ CLWS/CLWD receives push                  ← 0ms / +240ms

T+?    Symbol added to exchangeInfo
       └─ EXCHANGE detects                       ← ~1-5s (poll interval)

T+??   Matching engine starts, first price appears
       └─ SNIPER detects                         ← ~1ms + RTT
```

| Listing type | WS announcement | exchangeInfo symbol | ticker/price symbol |
|--------------|------------------|----------------------|----------------------|
| Pre-announce | first | later (minutes/hours) | after exchangeInfo |
| Direct listing | same time | same time | same time |
| Stealth listing | later | first | after exchangeInfo |

---

## Files

| File | Purpose |
|------|---------|
| `announce_ws_client.py` | Primary channel — Binance Announcements WebSocket client (HMAC-SHA256 signed subscription, PING every 30 s, exponential reconnect backoff). Filters by `catalogId in WS_CATALOG_IDS`. |
| `announce_ws2_client.py` | Secondary WS channel — connects to undocumented public stream `!announcements@arr` on `stream.binance.com:9443`. No auth. Auto-subscribes via single-stream URL. |
| `cms_apex_poller.py` | Fallback channel — parallel CMS REST (apex path) poller using 5 `pageSize` values as separate CloudFront cache keys. Adaptive interval (base + burst). Persists per-catalog `max_id` state to `./state/cms_state.json`; supports `--oneshot` and `--reset`. |
| `cms_apex_catalog_poller.py` | Alternative CMS poller — hits `/bapi/apex/.../catalog/list/query` with flat articles. No `releaseDate`. Comparison-only channel. |
| `cms_composite_poller.py` | Comparison channel — polls alternative CMS (composite path) `/bapi/composite/v1/public/cms/article/catalog/list/query` with random `pageSize` (10..49) cache-buster. 60s interval. Confirms identical CloudFront cache behaviour vs apex path. Persists `max_id` to `./state/composite_state.json`; supports `--oneshot` and `--reset`. |
| `exchangeinfo_poller.py` | Backup channel — polls `GET /api/v3/exchangeInfo` on three hosts in parallel (CloudFront, GCP GLB, direct nginx) for new trading symbols. Persists the known symbol set to `./state/exchangeinfo_state.json`; supports `--oneshot` and `--reset`. |
| `ticker_sniper.py` | Tradable-now signal — polls lightweight `GET /api/v3/ticker/price` (~150KB) on three hosts in parallel at high frequency. Default 1 Hz, tunable to 100 Hz. Adaptive 429 backoff. Persists the known symbol set to `./state/ticker_sniper_state.json`; supports `--oneshot` and `--reset`. |
| `cryptolisting_client.py` | Fourth channel — connects to CryptoListing.ws (co-located with Binance in AWS Tokyo). Two free tiers run in parallel: SpeedTrial (`CLWS`, 0 ms, title redacted, benchmark) and FreeDelayed (`CLWD`, +240 ms, full payload). Auth via `X-API-Key: dsk_...` header; supports `--tier` and `--test`. |
| `notifier.py` | Shared async Telegram notifier (fire-and-forget). Emits startup / shutdown / reconnect / new-announcement events. No-op if `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` are unset. |
| `run.py` | Single entry point — runs all configured channels in one asyncio event loop. Select channels via `ENABLED_CHANNELS` env var (e.g. `all,!clws` to run everything except CryptoListing while waiting for a token). |
| `.env` | API credentials + Telegram + CryptoListing config (not committed). |
| `.env.example` | Template for `.env`. |
| `requirements.txt` | Python dependencies. |
| `state/*.json` | Persisted state per channel (auto-created; gitignored). |

---

## Setup

```bash
# 1. Create a read-only Binance API key (no trading, no withdrawal)
#    https://www.binance.com/en/my/settings/api-management

# 2. Create .env from the example and fill in your keys
cp .env.example .env
# edit .env -> BINANCE_API_KEY, BINANCE_API_SECRET

# 3. Install dependencies (use your venv)
pip install -r requirements.txt
```

### `.env`

```ini
BINANCE_API_KEY=your_read_only_api_key_here
BINANCE_API_SECRET=your_api_secret_here

# Optional overrides:
# ANN_TOPIC=com_announcement_en
# ANN_RECV_WINDOW_MS=30000
# WS_CATALOG_IDS=0                          # WS filter: 0 = all, default
# WS_CATALOG_IDS=48                         # WS: listings only
# ANN_CATALOG_IDS=48                        # CMS: default: 48 only
# ANN_CATALOG_IDS=48,49,50,51,93,128,157,161 # CMS: multiple categories
# ANN_CATALOG_IDS=0                          # CMS: monitor ALL categories

# Telegram notifications (optional — notifier is a no-op if unset).
# Create a bot via @BotFather, then get chat id (negative for groups).
# TELEGRAM_BOT_TOKEN=123456789:ABCdef...
# TELEGRAM_CHAT_ID=-1001234567890
# SMOKE_SECONDS=120
```

---

## Telegram notifications

Both channels (`announce_ws_client.py`, `cms_apex_poller.py`) share a single
`notifier.py` instance that sends events to a Telegram chat. This gives a
liveness signal (so you know the channel is up) and pushes new detections.

Events sent:

| Event | When | Example |
|-------|------|---------|
| 🟢 **startup** | Service starts | `🟢 WS started` / `🟢 CMS started` |
| 🔴 **shutdown** | Service stops (Ctrl-C / exit) | `🔴 WS stopped` |
| 🟡 **reconnect** | WS connection dropped, reconnecting | `🟡 WS reconnecting` |
| 🚀 **announcement** | New listing detected | `🚀 NEW [WS] 🟢 latency: 21 ms …` |

Latency badges in the announcement message:
- 🟢 `< 100 ms`
- 🟡 `< 150 ms`
- 🔴 `>= 150 ms`
- ⚪️ `N/A` (no publish timestamp available)

The notifier is **fire-and-forget**: sends are scheduled as background
`asyncio` tasks and never block the detection hot path. If
`TELEGRAM_BOT_TOKEN` or `TELEGRAM_CHAT_ID` are unset, the notifier logs a
single warning and becomes a no-op (so the same code runs in CI / local dev
without Telegram configured).

---

## Run

### All channels (recommended)

```bash
# Start all configured channels in one process (asyncio, cooperative)
python run.py
```

Channels are selected via `ENABLED_CHANNELS` in `.env`:

```bash
ENABLED_CHANNELS=all                    # all 8 channels
ENABLED_CHANNELS=all,!clws               # all except CryptoListing (while waiting for token)
ENABLED_CHANNELS=ws,ws2,cms_apex        # lightweight: WS + WS2 + CMS apex only
ENABLED_CHANNELS=ws,ws2,sniper          # WS + WS2 + sniper only
ENABLED_CHANNELS=ws,ws2                 # WS + WS2 only
```

Each channel runs as a separate asyncio task in the same event loop. They
share the loop but do not block each other (cooperative scheduling on `await`).
Each owns its own `aiohttp.ClientSession` and state file, so they do not
compete for resources.

### Individual channels (debugging)

```bash
python announce_ws_client.py             # primary (WS push, signed)
python announce_ws2_client.py            # secondary (WS push, public)
python cms_apex_poller.py                # fallback (CMS apex REST)
python cms_apex_catalog_poller.py       # comparison (CMS catalog REST)
python exchangeinfo_poller.py           # backup (exchangeInfo, 3 hosts)
python ticker_sniper.py                 # tradable-now (price ticker, 3 hosts)
python cms_composite_poller.py          # comparison (alt CMS path, 60s)
python cryptolisting_client.py          # CryptoListing.ws (CLWS + CLWD)
python cryptolisting_client.py --tier speedtrial   # one tier only
python cryptolisting_client.py --test               # smoke test after welcome

# One-shot checks
python cms_apex_poller.py --oneshot
python cms_apex_catalog_poller.py --oneshot
python exchangeinfo_poller.py --oneshot
python ticker_sniper.py --oneshot
python cms_composite_poller.py --oneshot

# Reset persisted state
python cms_apex_poller.py --reset
python cms_apex_catalog_poller.py --reset
python exchangeinfo_poller.py --reset
python ticker_sniper.py --reset
python cms_composite_poller.py --reset
```

For long-running deployments, run under `tmux` / `systemd` / `supervisord`.

---

## Output format

Each detection is a single stdout line, tagged with the source channel:

```
<recv_ts_ms>  WS   latency    21ms  id=280400  Binance Will List SOMECOIN (SOME) ...
<recv_ts_ms>  CMS  latency  5024ms  id=280400  key=PS1  cache=Miss from cloudfront  Binance Will List SOMECOIN (SOME) ...
```

Latency is colour-coded in the console:
- green `<100 ms`
- yellow `<150 ms`
- red `>=150 ms`

The WS client also emits a JSON record per announcement on stdout (one JSON
object per line), with fields: `received_ts_ms`, `publish_ts_ms`,
`latency_ms`, `topic`, `catalog_id`, `catalog_name`, `title`,
`announcement`.

---

## Configuration reference

### Channel selection (`run.py`)

| Env var | Default | Description |
|---------|---------|-------------|
| `ENABLED_CHANNELS` | `all,!clws` | Comma-separated list of channels to run. Valid: `ws`, `ws2`, `cms_apex`, `cms_catalog`, `cms_composite`, `exchange`, `sniper`, `clws`. Special: `all` (everything), `!name` (exclude one). |
| `CL_TIERS` | `both` | CryptoListing tiers: `both`, `speedtrial`, `freedelayed`. |
| `CL_TEST` | `false` | Send `{"type":"test"}` 15s after CryptoListing welcome. |

Examples:
```bash
ENABLED_CHANNELS=all                    # all 8 channels
ENABLED_CHANNELS=all,!clws               # all except CryptoListing
ENABLED_CHANNELS=ws,ws2,cms_apex,sniper # WS + WS2 + CMS apex + sniper
```

### Shared env var (announcement channels)

| Env var | Default | Description |
|---------|---------|-------------|
| `ANN_CATALOG_IDS` | `48` | Comma-separated list of `catalogId` values for **CMS pollers**. Set to `0` to monitor ALL categories. Example: `48,49,50,51,93,128,157,161`. |
| `WS_CATALOG_IDS` | `0` | Comma-separated list of `catalogId` values for **WS channel**. Default `0` = all announcements pass through. Set to e.g. `48` to restrict WS to listings only. |

### WebSocket client (`announce_ws_client.py`)

| Env var | Default | Description |
|---------|---------|-------------|
| `BINANCE_API_KEY` | — | Binance API key (read-only is enough). Required. |
| `BINANCE_API_SECRET` | — | Binance API secret. Required. |
| `ANN_TOPIC` | `com_announcement_en` | WS topic to subscribe to. |
| `ANN_RECV_WINDOW_MS` | `30000` | Signature recvWindow, ms. Max 60000. |
| `WS_CATALOG_IDS` | `0` | Catalog filter for WS channel. `0` = all announcements. Example: `48,49` to restrict. |

### CMS apex poller (`cms_apex_poller.py`)

| Flag | Description |
|------|-------------|
| `--oneshot` | Run a single poll cycle and exit. |
| `--reset` | Delete the persisted `max_id` state file. |

| Env var | Default | Description |
|---------|---------|-------------|
| `CMS_POLL_INTERVAL_S` | `2.0` | Base interval between poll cycles (seconds). |
| `CMS_POLL_FAST_INTERVAL_S` | `0.5` | Interval during a fast burst (seconds). |
| `CMS_POLL_FAST_CYCLES` | `4` | Number of fast cycles after a trigger. |
| `CMS_ADAPTIVE_TRIGGERS` | `Miss,RefreshHit` | Cache statuses that trigger a fast burst. |

The CMS apex poller uses an **adaptive interval**:
- By default it polls every `CMS_POLL_INTERVAL_S` (2 s) seconds.
- When a response has a cache status matching `CMS_ADAPTIVE_TRIGGERS`
  (Miss or RefreshHit), it switches to fast mode (`CMS_POLL_FAST_INTERVAL_S`,
  0.5 s) for `CMS_POLL_FAST_CYCLES` (4) cycles. This raises the chance of
  catching a fresh announcement that appears right after a cache refresh.
- After the burst, it returns to the base interval.

**Tuning for aggressive polling:** set `CMS_POLL_INTERVAL_S=0.5` and
`CMS_POLL_FAST_INTERVAL_S=0.2` — but watch the request volume: with all 8
catalogs and 5 cache keys each, that's 40 req per cycle.

State file: `./state/cms_state.json` (relative to the project root). Holds the
last seen `max_id` per `catalogId` so restarts do not re-emit already-known
announcements. Auto-created on first run; gitignored.

### exchangeInfo poller (`exchangeinfo_poller.py`)

| Flag | Description |
|------|-------------|
| `--oneshot` | Run a single poll cycle and exit. |
| `--reset` | Delete the persisted symbol set. |

| Env var | Default | Description |
|---------|---------|-------------|
| `EI_POLL_INTERVAL_S` | `2.0` | Interval between poll cycles (seconds). |
| `EI_HOSTS` | `api.binance.com,api-gcp.binance.com` | Comma-separated API hosts. |

The exchangeInfo poller queries `GET /api/v3/exchangeInfo` on two hosts in
parallel:
- `api.binance.com` — AWS CloudFront
- `api-gcp.binance.com` — Google Cloud Load Balancer

These sit behind different CDNs with independent caches, so polling both
raises the chance of seeing a fresh response on at least one of them. A new
symbol seen on either host is emitted.

**Bootstrap:** on first run (empty state), the current symbol set is stored
as a baseline silently — no symbols are emitted. After that, only symbols not
present in the previous snapshot are emitted.

**Rate limits:** exchangeInfo has weight=20 per request. The Spot API limit
is 6000 weight/min. At a 2 s interval, each host uses 600 weight/min — well
within limits.

State file: `./state/exchangeinfo_state.json`. Holds the known symbol set so
restarts do not re-emit already-known symbols. Auto-created on first run;
gitignored.

**Note:** Binance does not expose a "listed_at" timestamp in the exchangeInfo
response, so this channel cannot compute a true publish-to-detect latency.
The detection delay is bounded by the poll interval (1–2 s on average).

### Ticker sniper (`ticker_sniper.py`)

| Flag | Description |
|------|-------------|
| `--oneshot` | Run a single poll cycle and exit. |
| `--reset` | Delete the persisted symbol set. |

| Env var | Default | Description |
|---------|---------|-------------|
| `SNIPER_POLL_INTERVAL_MS` | `1000` | Base interval between cycles (ms). 1000 = 1 Hz. |
| `SNIPER_HOSTS` | `api4.binance.com,api.binance.com,api-gcp.binance.com` | Comma-separated API hosts. |
| `SNIPER_MAX_429_BACKOFF_S` | `60` | Max backoff on HTTP 429 (seconds). |

Polls `GET /api/v3/ticker/price` (all symbols, ~150KB JSON) on three hosts
in parallel. Detects new tradable symbols by diffing the symbol set between
requests. This is the technique used by rickstaa/crypto-listings-sniper for
sub-300ms "tradable now" detection.

**Rate limits:** `/api/v3/ticker/price` has weight=2 per request. Spot API
limit is 6000 weight/min → 3000 req/min = 50 req/sec ceiling per IP. Default
1 Hz is extremely conservative; tune aggressively once no 429 in production:
- `SNIPER_POLL_INTERVAL_MS=100` (10 Hz)
- `SNIPER_POLL_INTERVAL_MS=50` (20 Hz)
- `SNIPER_POLL_INTERVAL_MS=10` (100 Hz)

**Adaptive 429 backoff:** on HTTP 429, switches to exponential backoff
(doubling up to `SNIPER_MAX_429_BACKOFF_S`). After 10 consecutive clean
cycles, backoff is halved until it returns to the normal rate.

### CMS composite poller (`cms_composite_poller.py`)

| Flag | Description |
|------|-------------|
| `--oneshot` | Run a single poll cycle and exit. |
| `--reset` | Delete the persisted `max_id` state. |

| Env var | Default | Description |
|---------|---------|-------------|
| `COMPOSITE_POLL_INTERVAL_S` | `60.0` | Interval between poll cycles (seconds). |
| `ANN_CATALOG_IDS` | `48` | Shared with `cms_apex_poller.py`. Comma-separated `catalogId` list for CMS pollers. Set to `0` for ALL known categories. WS channel has its own `WS_CATALOG_IDS`. |

Polls the alternative CMS path
`/bapi/composite/v1/public/cms/article/catalog/list/query` with a randomized
`pageSize` (10..49) per request as a weak cache-buster (technique from
rickstaa). This is a **comparison-only** channel to confirm that the
composite path has the same CloudFront cache behaviour as the apex path
used by `cms_apex_poller.py`. Monitors the same set of `catalogId` values
as `cms_apex_poller.py` via the shared `ANN_CATALOG_IDS` env var.

**Important:** the composite path does NOT return `releaseDate`, so this
channel cannot compute latency. It only reports the article `id`, `code`,
and `title` when a new article is detected.

**Interval rationale:** 60s interval is the safe ceiling for CMS endpoints.
rickstaa's code comment: "Don't set above 0.016666667 Hz or binance will
(temporary) ban your IP." We respect this limit.

### CryptoListing.ws client (`cryptolisting_client.py`)

| Flag | Description |
|------|-------------|
| `--tier speedtrial` | Run only the SpeedTrial tier. |
| `--tier freedelayed` | Run only the FreeDelayed tier. |
| `--tier both` (default) | Run both tiers in parallel (if tokens set). |
| `--test` | Send `{"type":"test"}` 15 s after `welcome` (smoke check). |

| Env var | Default | Description |
|---------|---------|-------------|
| `CL_SPEEDTRIAL_TOKEN` | — | `dsk_...` API key for SpeedTrial tier (0 ms delay, title/ticker redacted). |
| `CL_FREEDELAYED_TOKEN` | — | `dsk_...` API key for FreeDelayed tier (+240 ms delay, full payload). |
| `CL_CEX` | `binance` | Comma-separated exchanges: `binance,upbit,bithumb`. |
| `CL_ENDPOINT` | `wss://cryptolisting.ws` | WSS endpoint. Seoul mirror: `wss://kr.cryptolisting.ws` (Upbit only). |

Auth is via `X-API-Key: dsk_...` HTTP header (NOT a query param). The tier
is determined by the API key itself; the `welcome` frame reports it. Keys
are obtained from Telegram `@CLWfeed` and expire after one week (the server
sends a close frame with reason `key_expired`).

Two free tiers run in parallel:
- **SpeedTrial** (tagged `CLS`) — 0 ms delay, title/ticker REDACTED on
  listing events. Used as a benchmark: compare `dispatchTimestampUs`
  against the direct Binance WS receipt on real listings to confirm
  whether co-location gives a measurable edge over the direct WS.
- **FreeDelayed** (tagged `CLD`) — +240 ms delay, full payload. Used as an
  auxiliary feed.

The server sends a WebSocket PING every 15 s (library auto-responds with
PONG) and a JSON `heartbeat` every 30 s — no application-layer keep-alive
needed. Reconnect uses exponential backoff capped at 300 s with max 20
retries; on `key_expired` / `key_invalidated` close reasons the client
exits immediately.

Docs: https://cryptolisting.ws/docs/book/

---

## Data sources

### Primary — Binance Announcements WebSocket API

- Base URL: `wss://api.binance.com/sapi/wss`
- Topic: `com_announcement_en`
- Auth: HMAC-SHA256 signature over `random`, `topic`, `recvWindow`,
  `timestamp` (in that literal order, verified against the docs' worked
  example on 2026-07-20). API key sent in the `X-MBX-APIKEY` header.
- Limits: 5 messages/sec, PING every 30 s, connection TTL 24 h.
- Docs:
  - https://developers.binance.com/en/docs/products/announcements/general-info
  - https://developers.binance.com/en/docs/products/announcements/announcement

Payload (per announcement push):

```json
{
  "catalogId":   48,
  "catalogName": "New Cryptocurrency Listing",
  "publishDate": 1784303104983,
  "title":       "Binance Will List ...",
  "body":        "<html>",
  "disclaimer":  "..."
}
```

`catalogId == 48` maps exactly to
https://www.binance.com/en/support/announcement/list/48.

### Fallback — CMS REST API (CloudFront-cached)

```
GET https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query
    ?type=1&pageNo=1&pageSize=<N>&catalogId=48
```

Response shape:

```json
{
  "code": "000000",
  "success": true,
  "data": {
    "catalogs": [{
      "catalogId": 48,
      "catalogName": "New Cryptocurrency Listing",
      "total": 2211,
      "articles": [
        {
          "id": 280389,
          "code": "5c52a77daddf4e0d9769b87cdc91fae4",
          "title": "Binance Will Add Aerodrome (AERO) ...",
          "type": 1,
          "releaseDate": 1784303104983
        }
      ]
    }]
  }
}
```

`id` is used for dedup; `releaseDate` is the publish timestamp (ms) used for
latency calculation.

---

## CloudFront cache behaviour (measured 2026-07-20)

The CMS endpoint is served behind CloudFront. Key observations from an
AWS Tokyo VPS running `cms_apex_poller.py`:

- The cache key includes `(catalogId, pageNo, pageSize)`.
- Request headers `Cache-Control: no-cache` and `Pragma: no-cache` do **not**
  bypass CloudFront — responses are still served from cache.
- Random query parameters (e.g. `?_=timestamp`) are rejected with `400 Bad
  Request` by the origin.
- Valid `pageSize` values: `1, 2, 3, 5, 10, 15, 20, 50`.
  `4, 6, 7, 8, 9, 25, 100` return `400`.
- The web UI uses `pageSize=10` and `50`, so those entries are always cached.
  The poller uses rare pageSizes (`1, 3, 5, 15, 20`) to raise the chance of
  a cache miss.

### Stats output format

```
[CMS_APEX] STATS  cycle=120  max=280389  err=0  [PS1:121(H:119 M:1 R:1), PS15:121(H:120 M:1 R:0), ...]
                                                      │      │     │
                  pageSize=1                          │      │     └── RefreshHit
                  total requests in 120 cycles ───────┘      └──────── Miss
```

| Tag | CloudFront header | Meaning |
|-----|------------------|---------|
| **H** | `Hit from cloudfront` | Response served from cache (stale data). |
| **M** | `Miss from cloudfront` | Request reached origin (fresh data). |
| **R** | `RefreshHit from cloudfront` | Stale response served, but CloudFront refreshed the cache in the background. |

### Sample run (AWS Tokyo, ~2 minutes, no new listings during the window)

| pageSize | Total | H (Hit) | M (Miss) | R (Refresh) | Notes |
|----------|-------|---------|----------|-------------|-------|
| PS1  | 121 | 119 | 1 | 1 | only the first request missed |
| PS3  | 121 | 120 | 1 | 0 | only the first request missed |
| PS5  | 121 | 120 | 0 | 1 | one RefreshHit early on |
| PS15 | 121 | 120 | 1 | 0 | only the first request missed |
| PS20 | 121 | 121 | 0 | 0 | never missed — likely pre-warmed by other Binance services |

**Interpretation:** after the cache is warmed, ~100 % of requests are `Hit`.
This means a real new listing will only be visible through the CMS channel
once the CloudFront TTL for at least one cache key expires — typically
60–120 s after publication. The CMS channel therefore cannot reach `<150 ms`;
it is kept as a fallback and for cross-validation with the WS channel.

The only way for the CMS endpoint to approach `<1 s` latency is if Binance
invalidates the CloudFront cache at publish time — which is outside our
control. For sub-150 ms detection, the WebSocket channel is the only viable
free source.
