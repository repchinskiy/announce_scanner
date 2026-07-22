# Research: Binance announcement detection — alternative channels

Date: 2026-07-21

Context: `announce_scanner` targets `<150 ms` (ideal `<100 ms`) detection latency
from Binance publish time to local detection, on AWS Tokyo VPS, free-tier only.
The primary channel is already the official Binance Announcements WebSocket
(`wss://api.binance.com/sapi/wss`, topic `com_announcement_en`, catalogId 48).
Three alternative channels were evaluated as potential speed-ups or fallbacks.

---

## 1. Binance origin IP discovery (bypass CloudFront)

### Available vectors

| Vector | What to look for | Realistic yield for Binance |
|--------|------------------|------------------------------|
| **crt.sh / Censys** | `*.binance.com` certs from non-Amazon CA with unusual SAN | Low |
| **Shodan** | `ssl.cert.subject.cn:"binance.com" -org:"Amazon"` + favicon hash | Low |
| **Passive DNS** (ViewDNS, SecurityTrails) | Historical A-records pre-CloudFront | Low (stale/dead) |
| **Subdomain enum** (subfinder, amass) | `*.binance.com` resolving outside CloudFront IP ranges | Low–Medium |
| **HTTP headers** | Already see `Via: 1.1 tesla` — internal proxy name, **no IP** | Informational only |
| **Error pages** | Trigger 4xx/5xx → backend service names | Not attempted (noisy) |
| **Origin probing** | `curl --resolve www.binance.com:443:<ip>` → compare `Server:` | Likely blocked |

### Realistic assessment

**Main blocker: AWS WAF + security groups.** Standard best practice — origin
security group allows 443 **only from CloudFront IP ranges**. If Binance does
this (almost certainly), `--resolve` to a candidate IP gets TCP RST, even if
the IP is discovered.

**Second issue:** origin is likely behind ALB/NLB (load balancer IP, not app
server), plus autoscaling — IPs rotate within hours-to-days.

**Third issue:** public write-ups about Binance origin leaks — **zero results**
in GitHub/blogs. Either no such leak happened, or it was closed via bug bounty.

**ToS risk:** polling origin directly in bypass of CDN = ToS violation,
potentially CFAA / Computer Misuse Act.

### Concrete signals checklist (for completeness, not for active use)

| Vector | Signal to grep/search | Likely yield for Binance |
|---|---|---|
| crt.sh | non-Amazon certs with unusual `*.binance.com` SAN | Low–Medium |
| Censys | `ssl.cert.subject.cn:"binance.com"` not in CloudFront IP ranges | Low |
| Shodan | `ssl.cert.subject.cn:"*.binance.com" -org:"Amazon"` + favicon | Low |
| Passive DNS | pre-CloudFront A records for `www.binance.com` | Low (stale/dead) |
| Subdomain enum | `*.binance.com` outside CloudFront IP ranges | Low–Medium |
| HTTP headers | `Via: 1.1 tesla` already seen — internal proxy, no IP | Info only |
| Error pages | backend service names in 4xx/5xx bodies | Not attempted |
| Origin probing | non-CloudFront `Server:`, Binance-issued TLS cert, fresh `Age` | Likely blocked |

### Verdict

- **Do not pursue as production channel.** Realistic success probability is
  low (single-digit percent), risk of IP ban / ToS violation is high.
- A passive literature check (`crt.sh`, `subfinder`) is acceptable as a
  one-off experiment to test the hypothesis, but should not be wired into the
  production detection path.
- The CMS endpoint is explicitly a fallback channel, not the latency-critical
  hot path — bypassing CloudFront cache on the fallback would not help the
  primary `<150 ms` metric.

---

## 2. HTML scraping of `list/48`

Hypothesis was that HTML scraping
(https://www.binance.com/en/support/announcement/list/48) is strictly worse
than the CMS REST endpoint. Evidence confirms the hypothesis — and is in fact
stronger than expected.

### What `list/48` returns to a plain HTTP client

```
HTTP/1.1 202 Accepted
Server: CloudFront
x-amzn-waf-action: challenge      ← AWS WAF
Cache-Control: no-store, max-age=0
X-Cache: Error from cloudfront
Content-Length: 2036
```

Body is **not the application HTML** — it's an AWS WAF JavaScript challenge:

```html
<script src="https://...token.awswaf.com/.../challenge.js"></script>
AwsWafIntegration.getToken().then(() => { window.location.reload(true); });
<noscript>JavaScript is disabled ... Enable JavaScript and then reload.</noscript>
```

### Comparison: HTML `list/48` vs CMS REST

| | HTML `list/48` | CMS REST `article/list/query` |
|---|---|---|
| Status | 202 | 200 |
| `X-Cache` | `Error from cloudfront` | `Miss` / `Hit` (cacheable) |
| `x-amzn-waf-action` | `challenge` | absent |
| Cached? | No (`no-store`) | Yes (ETag revalidation) |
| Body | 2036 bytes, WAF challenge | 1278 bytes, 5 articles |
| What is needed to get data | JS exec + reload + SPA XHR to CMS REST | One request |

### Key discovery

Even if the WAF challenge is solved with a headless browser, the SPA inside
makes an XHR to the **same CMS REST endpoint** the project already polls
directly. HTML scraping is a strict superset of REST latency with zero
freshness gain.

### Other checks

- **`__NEXT_DATA__` / `__APP_DATA__`**: not present in the WAF challenge body.
- **SSE / EventSource**: none on the page.
- **`sitemap.xml`**: same WAF challenge (HTTP 202, empty body).
- **`robots.txt`**: explicitly disallows `/bapi/`, `/api/`, `*/sitemap_.xml`.

### Verdict

- **Final answer: no.** HTML scraping `list/48` is not a viable signal at all.
  AWS WAF returns a JS challenge to every non-cookie-bearing client, so a
  plain poller cannot retrieve the article list. Even with a JS-executing
  browser, the SPA's data path terminates at the **same CMS REST endpoint**
  the project already polls directly.

---

## 3. Third-party Binance-listing notification services

### Key finding first

No third-party service can plausibly beat Binance's own public Announcements
WebSocket on a sustained basis. The WS push is the *source* these services
consume. Any third party that monitors Binance adds at least one extra hop
(their poll/parse → their server → their WS → you), so a well-built direct
client of Binance's official WS is already on the fastest publicly available
path.

### Why a third party *could* (in principle) be faster

- **Co-location / same-region detection**: if their scraper sits in the same
  AWS region as Binance's announcement origin and Binance's WS gateway adds
  serialization/queueing before pushing to retail subscribers, the scraper
  could detect the CMS/origin change a few ms before the WS fan-out reaches a
  remote client. CryptoListing.ws markets exactly this ("Tokyo endpoint sits
  in the same AWS region as Binance"). This is the only credible mechanism.
- **Private/paid institutional feeds**: Binance does not document any private
  "early announcement" feed. No evidence of one was found. Speculation.
- **Insider access**: illegal (MNPI / market manipulation). No legitimate
  service claims this.
- **Monitoring an upstream source Binance reads**: there is no public upstream;
  the announcement originates at Binance. Not applicable.

### Per-service findings

#### 3.1 CryptoListing.ws

- **Monitors**: Binance, Upbit, Bithumb — listings, delistings, airdrops,
  monitoring-tag changes, Futures listings.
- **Source**: scrapes official exchange announcements; explicitly co-located
  ("Tokyo endpoint in same AWS region as Binance").
- **Delivery**: WebSocket `wss://cryptolisting.ws` (plus Seoul mirror
  `wss://kr.cryptolisting.ws` for Upbit). µs-precision timestamps, zero-copy
  broadcast. Telegram channel exists but real alerts go via WS only.
- **Latency claims**: "#1 Fastest WebSocket provider"; µs-precision dispatch.
  Does NOT explicitly claim to beat Binance's official WS.
- **Tiers**:
  - Premium: 0 ms delay, paid, full title + ticker.
  - Basic: +20 ms, paid, full title + ticker.
  - SpeedTrial: 0 ms, free, renewable weekly — **title & ticker REDACTED**
    on listing events (useless for trading).
  - FreeDelayed: **+240 ms delay**, free weekly — full payload but delayed
    beyond the 150 ms target by itself.
- **Could it beat Binance WS?**: plausibly yes on Premium (co-location), by a
  few ms — but Premium is paid and out of scope. Free tiers do not help.

#### 3.2 CoinMarketCal

- Community-submitted crypto events across exchanges. Source is
  community-submitted + moderator-verified. Latency minutes-to-hours. REST
  API only, no WS. Strictly slower than official announcement.

#### 3.3 CoinGecko listings / status updates

- Exchange listings aggregation. CoinGecko's own ingestion pipeline.
  Minutes-to-tens-of-minutes. REST, no listings WS. The `/status-updates`
  endpoint is now deprecated (404). Strictly slower, downstream aggregation.

#### 3.4 Listing-alert / CoinList / Telegram bots

- **CoinList**: a token-sale platform, not a Binance-listing alert service.
- **Generic Telegram "listing alert" bots**: typically Twitter-sourced or
  REST-polling. Latency seconds-to-minutes. Free, but slow and unreliable.

#### 3.5 Cointelegraph / CryptoSlate news APIs

- Human-written news articles. Minutes to hours. CryptoSlate has only RSS +
  newsletter, no real-time API. Cointelegraph has paid news APIs but still
  human-gated editorial latency. Far too slow.

#### 3.6 Other-exchange announcement APIs (MEXC / Bybit / KuCoin)

- KuCoin has public REST "Get Announcements" — REST polling, **no announcements
  WS**. Bybit/MEXC: no public announcements WebSocket.
- Usefulness as cross-exchange signal: LOW for Binance-listing target. A coin
  listing on MEXC/Bybit does not predict a Binance listing. REST-poll-bound
  at 1–5 s.

#### 3.7 GitHub projects scraping Binance announcements

None of the cited repos use a WebSocket for listing detection. Verified
source code of each:

- **eyupbarlas/New-Coin-Listing-Detection-Bot**
  (https://github.com/eyupbarlas/New-Coin-Listing-Detection-Bot) — REST
  polling via `python-binance`, interval ~10 minutes. Detection latency up
  to 10 min. Far too slow.
- **rickstaa/crypto-listings-sniper**
  (https://github.com/rickstaa/crypto-listings-sniper) — Go project using
  REST polling of `GET /api/v3/ticker/price` via go-binance
  `NewListPricesService()` on a `rate.Limiter` loop, diffing against the
  previous list (`utils.CompareLists`). Claims `<0.3 s` (300 ms) end-to-end
  to Telegram/Discord. 300 ms is above the 150 ms target. The author notes
  Binance left their market, limiting maintenance.
- **CyberPunkMetalHead/binance-trading-bot-new-coins**
  (https://github.com/CyberPunkMetalHead/binance-trading-bot-new-coins) —
  Selenium scrape of `https://www.binance.com/en/support/announcement/c-48`,
  extracting uppercase tickers from `id='link-0-0-p1'`. No WS, no
  exchangeInfo. Seconds-to-minutes latency (HTML scrape).
- **CyberPunkMetalHead/new-listings-trading-bot**
  (https://github.com/CyberPunkMetalHead/new-listings-trading-bot) — REST
  polling of the CMS announcement endpoint
  `GET /bapi/apex/v1/public/apex/cms/article/list/query?type=1&pageNo=1&pageSize=10`,
  regex `\(([A-Z]+)\)` on titles. Seconds latency. C# project
  (`Services/ListingsGetterService.cs`).
- **defidummy/listing-trading-bot**
  (https://github.com/defidummy/listing-trading-bot) — HTTP scrape of Binance
  announcements (`get_news.py` uses `requests.get` + `lxml` XPath, looks for
  "WILL LIST"). No WS. Seconds latency.

**Did anyone solve <150 ms without WS?**: No public GitHub project found.
Every sub-second project either uses WS or accepts hundreds-of-ms-to-seconds
latency. None of the four popular "binance new-listing" repos above uses
WebSocket detection; they all rely on REST/HTML scraping.

#### 3.7b "New-symbol WS" technique — why it does NOT help

A community trick exists (no official stream): subscribe to
`wss://stream.binance.com:9443/ws/!miniTicker@arr` (all-market tickers),
maintain a `known_symbols` set seeded from `GET /api/v3/exchangeInfo`, and
emit a detection when a previously-unseen `s` field appears in the ticker
array.

This does **not** help the project because:

- **No official stream** exists for "symbol added to exchange". The Binance
  Spot WS docs enumerate 15 stream types (`!miniTicker@arr`, `!ticker@arr`,
  `!bookTicker`, etc.) — none fires on listing addition. Futures mirrors
  the same set.
- **The trick detects "first ticker activity"** (matching engine produces
  first change), which happens *after* the symbol is already in
  `exchangeInfo`. It is strictly slower than the existing
  `exchangeinfo_poller.py` (which detects "added to exchange" at t=0 with
  ~1–5 s polling latency).
- The announcements WS (our primary) fires at publish time, typically
  minutes before the symbol appears in `exchangeInfo`, let alone ticks.
- Can produce false positives (stale seed snapshot) and missed events
  (transient connection drops).

Verdict: **do not add a new-symbol WS channel**. It would be a strictly
slower duplicate of `exchangeinfo_poller.py`.

#### 3.8 Twitter/X monitoring (@binance, @BinanceAnnounce)

- The @binance tweet is published by Binance's social team *after* the
  website/announcement WS push. Downstream of the WS push; adds API latency
  and rate-limit headaches.

### Comparison table

| Service | Monitors | Source | Claimed latency vs Binance ann. | Free? | Delivery | Could beat Binance WS? |
|---|---|---|---|---|---|---|
| CryptoListing.ws | Binance/Upbit/Bithumb listings, delistings, airdrops | Scrapes official announcements, co-located in Binance AWS region | µs-precision dispatch; not vs Binance WS explicitly | Free tiers: SpeedTrial redacts title (0 ms), FreeDelayed full payload +240 ms | WebSocket `wss://cryptolisting.ws` | Paid Premium: plausibly a few ms faster. Free: no |
| CoinMarketCal | Cross-exchange crypto events (community) | Community-submitted + moderated | minutes–hours | Limited free REST, paid tiers | REST API | No |
| CoinGecko (listings/status) | Exchange listings aggregation | CoinGecko ingestion pipeline | minutes–tens of minutes | Free REST, paid Commercial | REST (status-updates deprecated) | No |
| CoinList | Own token-sales platform | N/A — not Binance listings | N/A | Free | Web/email | No |
| Generic Telegram listing bots (Twitter-sourced) | Binance + others via X | Twitter API / REST polling | seconds–minutes | Free | Telegram bot | No |
| Cointelegraph / CryptoSlate | News articles | Human editorial | minutes–hours | RSS / paid news API | REST/RSS | No |
| MEXC/Bybit/KuCoin announcement APIs | Their own exchange announcements | Official REST (KuCoin), no announcements WS | 1–5 s polling-bound | Free | REST polling | No (wrong exchange, REST-bound) |
| GitHub bots (eyupbarlas, rickstaa, etc.) | Binance listings/announcements | REST polling / exchangeInfo; rickstaa claims WS-ish | 10 min (eyupbarlas); ~300 ms (rickstaa); seconds (others) | Free | Telegram/Discord | No — all slower than direct Binance WS |
| Twitter/X @binance | Binance social posts | X account | at best simultaneous, usually after WS push | Free (limited) / paid X API | REST/streaming | No |

### Verdict

**Do not integrate any of these as a hot-path source.** The project's existing
primary channel (official Binance Announcements WebSocket via
`announce_ws_client.py`) is already on the fastest publicly available path to
sub-150 ms. None of the researched services offers a faster *free* signal.

- **CryptoListing.ws** — the only service with a structural speed edge
  (co-location in Binance's AWS region), but the free tiers do not help:
  SpeedTrial redacts the ticker (can't detect the actual coin), and
  FreeDelayed is +240 ms (exceeds the 150 ms target on its own). Both free
  tiers require weekly manual renewal and depend on a third party's uptime.
  *Optional, low-priority* use: subscribe to the **SpeedTrial** tier as a
  *cross-validation* channel only — compare its dispatch timestamp against
  `announce_ws_client.py`'s `latency_ms` to confirm whether a co-located
  detector genuinely beats the direct WS. Do NOT treat it as a detection
  source (redacted titles). This is a measurement experiment, not a data feed.
- **Cross-exchange announcement APIs (KuCoin REST etc.)** — skip. Wrong
  exchange, REST-poll-bound at 1–5 s.
- **News/RSS/community (Cointelegraph, CryptoSlate, CoinMarketCal, CoinGecko)**
  — skip entirely. All are downstream human/editorial/aggregation layers,
  minutes-to-hours behind. Not in the latency class.
- **GitHub bots** — skip as feeds. Useful only as implementation references.
- **Twitter/X @binance** — skip. Downstream of the WS push.

---

---

## Appendix A — Direct CMS endpoint reference

All endpoints below are public (no API key required) and are served behind
CloudFront (cache TTL ~60–75 s on the Tokyo edge).

### Base URL

```
https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query
```

### Query parameters

| Parameter | Value | Notes |
|-----------|-------|-------|
| `type` | `1` | Article list query type (the only observed value) |
| `pageNo` | `1` | Page number, 1-indexed |
| `pageSize` | one of `1, 2, 3, 5, 10, 15, 20, 50` | Other values return `400 Bad Request` |
| `catalogId` | see table below | CMS category ID |

### Known catalog IDs

| catalogId | Category |
|-----------|----------|
| 48 | New Cryptocurrency Listing (== list/48, primary target) |
| 49 | Latest Binance News |
| 50 | Latest Activities |
| 51 | Delistings |
| 93 | Latest Activities (alt) |
| 128 | P2P Merchant Announcements |
| 157 | Margin / Futures listings |
| 161 | Earn / Staking |

### Example requests (catalogId=48, all cache keys used by cms_apex_poller.py)

```
https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query?type=1&pageNo=1&pageSize=1&catalogId=48
https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query?type=1&pageNo=1&pageSize=3&catalogId=48
https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query?type=1&pageNo=1&pageSize=5&catalogId=48
https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query?type=1&pageNo=1&pageSize=15&catalogId=48
https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query?type=1&pageNo=1&pageSize=20&catalogId=48
```

The web UI uses `pageSize=10` and `pageSize=50`, so those cache entries are
always warm. The poller uses rare pageSizes (`1, 3, 5, 15, 20`) to raise the
chance of a cache miss per cycle, but after warm-up all keys are still
~100 % Hit.

### Article detail (HTML, SSR via `__APP_DATA`)

```
https://www.binance.com/en/support/announcement/detail/{code}
```

Where `{code}` is the hex article code returned in the list response (e.g.
`5c52a77daddf4e0d9769b87cdc91fae4`). The HTML page embeds the full article
JSON inside a `<script id="__APP_DATA" type="application/json">` block.
Also served via CloudFront (same stale-while-revalidate behaviour).

### Response shape (list query)

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
          "title": "Binance Will List ...",
          "type": 1,
          "releaseDate": 1784303104983
        }
      ]
    }]
  }
}
```

- `id` — unique article ID, used for dedup (monotonically increasing per catalog)
- `code` — hex article code used in the detail URL
- `releaseDate` — publish timestamp in epoch ms (== `publish_ts_ms` used for
  latency calculation against the WebSocket `publishDate` field)

### CloudFront cache behaviour

- Cache key = `(catalogId, pageNo, pageSize)`
- TTL ~60–75 s on the Tokyo edge (`NRT12-P9`)
- Stale-while-revalidate: after TTL expiry, the object is kept as "stale";
  the next request returns a `RefreshHit` (stale body + background
  revalidation), the following request returns a fresh `Hit`
- `Cache-Control: no-cache` / `Pragma: no-cache` request headers do NOT
  bypass CloudFront
- Random query parameters (e.g. `?_=1234567890`) return `400 Bad Request`
- CloudFront rate-limit: ~40 req/2 s from a single IP triggers HTTP 429
  (ban cycle ~5 min, re-applies if the load persists). Safe config:
  2 catalogs × 5 keys at 5 s interval = 2 req/sec.

### What does NOT work on `api-gcp.binance.com`

The GCP host is market-data only (Spot REST API). CMS endpoints do not exist
there:

| Path on api-gcp.binance.com | Result |
|---|---|
| `/api/v3/exchangeInfo` | ✅ 200 OK (market data — used by exchangeinfo_poller.py) |
| `/bapi/apex/v1/public/apex/cms/article/list/query` | ❌ 404 Not Found (nginx) |
| `/sapi/wss` | ❌ 404 Not Found |

Other Binance hosts are not viable alternatives for CMS either:
- `data-api.binance.com/bapi/...` → 302 redirect to `www.binance.com` (CloudFront)
- `fapi.binance.com/bapi/...` → 403 Forbidden
- `dapi.binance.com/bapi/...` → 403 Forbidden

So the CMS channel is bound to `www.binance.com` (CloudFront) — there is no
GCP / non-CloudFront mirror of the CMS announcements endpoint.

---

## Appendix B — Composite CMS endpoint reference

Alternative CMS path used by `rickstaa/crypto-listings-sniper` and by the
`cms_composite_poller.py` channel. Same CloudFront cache behaviour as the apex
path, but different JSON shape and missing `releaseDate`.

### Base URL

```
https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query
```

### Query parameters

| Parameter | Value | Notes |
|-----------|-------|-------|
| `catalogId` | see Appendix A table | CMS category ID |
| `pageNo` | `1` | Page number, 1-indexed |
| `pageSize` | random `10..49` | rickstaa's cache-buster: ~40 distinct cache keys, collisions common |

### Example requests (catalogId=48)

```
https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query?catalogId=48&pageNo=1&pageSize=10
https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query?catalogId=48&pageNo=1&pageSize=22
https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query?catalogId=48&pageNo=1&pageSize=49
```

### Response shape (composite — flat)

```json
{
  "code": "000000",
  "data": {
    "articles": [
      {
        "id": 280389,
        "code": "5c52a77daddf4e0d9769b87cdc91fae4",
        "title": "Binance Will Add Aerodrome (AERO) ...",
        "imageLink": null
      }
    ]
  }
}
```

### Comparison: composite vs apex

| | composite (`/bapi/composite/.../catalog/list/query`) | apex (`/bapi/apex/.../article/list/query`) |
|---|---|---|
| CloudFront cache | ✅ Yes, identical TTL ~60–75 s | ✅ Yes, TTL ~60–75 s |
| Hit latency | ~110 ms | ~120 ms |
| JSON shape | `data.articles[]` (flat) | `data.catalogs[].articles[]` (nested) |
| `releaseDate` | ❌ not returned | ✅ epoch ms (needed for latency calc) |
| `catalogName` | ❌ not returned | ✅ returned |
| Cache-buster | random `pageSize` 10..49 | 5 fixed pageSizes (1,3,5,15,20) |
| Used by | rickstaa/crypto-listings-sniper, `cms_composite_poller.py` | `cms_apex_poller.py` |

**Verdict:** composite gives **no cache-bypass benefit** — CloudFront
caches it identically. The apex path is strictly better for detection
because it returns `releaseDate` (needed to compute latency). The composite
channel is kept only as a comparison/validation channel.

---

## 4. Overall recommendation

| Idea | Verdict |
|------|---------|
| Origin IP polling | **Do not pursue as production.** One-off passive research (`crt.sh`, `subfinder`) is acceptable as a hypothesis check, but realistic success is low and risk of IP ban / ToS violation is high. |
| HTML scraping `list/48` | **Final answer: no.** AWS WAF challenge + SPA XHR to the same CMS REST. Strictly worse than the CMS REST poller. |
| Third-party free services | **Do not integrate as feeds.** Optional: subscribe to CryptoListing.ws SpeedTrial as a benchmark to measure whether a co-located detector beats the direct WS. |

### What we kept

The architecture remains three channels:

```
WS        → push announcements      (<100 ms) ⭐ primary
CMS       → CloudFront REST         (~60–120 s)   fallback
EXCHANGE  → market data symbols     (~1–5 s)      backup
```

All third-party sources are either slower, paid, or monitor the same upstream.
The path to `<150 ms` is to keep optimizing the existing direct Binance
Announcements WebSocket client (`announce_ws_client.py`) — keep the hot path
thin, stay on the AWS Tokyo VPS (already low RTT to Binance), single long-lived
WS connection, minimal per-frame processing, background workers for
Telegram / DB.

---

## 5. Channel classification by event type

The six channels split into two groups by what they actually detect.

### Announcement events (publication of news on Binance)

| Channel | Source | What it detects |
|---------|--------|-----------------|
| `WS` | Binance Announcements WebSocket (`wss://api.binance.com/sapi/wss`) | Push of announcement publish event |
| `CMS_APEX` | CMS REST apex (`/bapi/apex/.../article/list/query`) | New article in CMS (after CloudFront cache TTL) |
| `CMS_COMPOSITE` | CMS REST composite (`/bapi/composite/.../catalog/list/query`) | New article in CMS (comparison channel) |
| `CLWS` / `CLWD` | CryptoListing.ws WebSocket | Push from co-located detector; covers `spot_listing`, `futures_listing`, `spot_delisting`, `futures_delisting`, `hodler_airdrop`, `monitoring_tag_extend`, `monitoring_tag_remove`, `not_listing` |

### Market data events (symbol appears in trading infrastructure)

| Channel | Source | What it detects |
|---------|--------|-----------------|
| `EXCHANGE` | `GET /api/v3/exchangeInfo` | New trading pair registered (symbol added to exchange) |
| `SNIPER` | `GET /api/v3/ticker/price` | Matching engine started publishing prices for a pair (tradable now) |

### Event timeline for a typical listing

```
T=0    Binance publishes announcement
       ├─ WS receives push                      ← <100ms
       ├─ CMS_APEX sees in REST (after cache)    ← ~60-120s
       ├─ CMS_COMPOSITE sees in REST             ← ~60-120s
       └─ CLWS/CLWD receives push                ← 0ms / +240ms

T+?    Symbol added to exchangeInfo
       └─ EXCHANGE detects                       ← ~1-5s (poll interval)

T+??   Matching engine starts, first price appears
       └─ SNIPER detects                         ← ~1ms + RTT
```

The announcement channels fire first (WS being the fastest). The market data
channels fire later — when Binance actually registers the pair and the
matching engine starts producing prices. The exact ordering between
announcement and market data events depends on the listing type
(pre-announce, direct listing, or stealth listing).
