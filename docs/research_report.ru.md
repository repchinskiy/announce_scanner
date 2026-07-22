# Исследование: Детекция анонсов Binance — альтернативные каналы

Дата: 2026-07-21

Контекст: проект `announce_scanner` нацелен на `<150 мс` (идеально `<100 мс`)
латентности детекта от момента публикации анонса Binance до локального
обнаружения, на VPS AWS Tokyo, только free-tier. Основной канал — официальный
Binance Announcements WebSocket (`wss://api.binance.com/sapi/wss`, топик
`com_announcement_en`, catalogId 48). Три альтернативных канала были
рассмотрены как потенциальные ускорения или резервные источники.

---

## 1. Обнаружение origin-IP Binance (обход CloudFront)

### Доступные векторы

| Вектор | Что искать | Реалистичная отдача для Binance |
|--------|-----------|--------------------------------|
| **crt.sh / Censys** | Сертификаты `*.binance.com` от не-Amazon CA с необычными SAN | Низкая |
| **Shodan** | `ssl.cert.subject.cn:"binance.com" -org:"Amazon"` + favicon hash | Низкая |
| **Passive DNS** (ViewDNS, SecurityTrails) | Исторические A-записи до CloudFront | Низкая (stale/dead) |
| **Перечисление поддоменов** (subfinder, amass) | `*.binance.com` вне IP-диапазонов CloudFront | Низкая–Средняя |
| **HTTP-заголовки** | Уже видим `Via: 1.1 tesla` — имя внутреннего прокси, **без IP** | Только информация |
| **Error pages** | Триггер 4xx/5xx → имена backend-сервисов | Не пробовали (шумно) |
| **Origin probing** | `curl --resolve www.binance.com:443:<ip>` → сравнить `Server:` | Вероятно заблокировано |

### Реалистичная оценка

**Главный блокер: AWS WAF + security groups.** Стандартная best practice —
security group origin'а разрешает 443 **только с IP-диапазонов CloudFront**.
Если Binance это делает (почти наверняка), `--resolve` к кандидату получит
TCP RST, даже если IP найден.

**Вторая проблема:** origin скорее всего за ALB/NLB (IP балансировщика, не
сервера приложения), плюс autoscaling — IP ротируются за часы-дни.

**Третья проблема:** публичные статьи про утечки origin Binance — **ноль
результатов** в GitHub/блогах. Либо таких утечек не было, либо они закрыты
через bug bounty.

**Риск ToS:** прямой polling origin в обход CDN = нарушение Terms of Use,
потенциально CFAA / Computer Misuse Act.

### Чек-лист конкретных сигналов (для полноты, не для активного использования)

| Вектор | Сигнал для поиска | Вероятная отдача |
|---|---|---|
| crt.sh | не-Amazon сертификаты с необычным SAN `*.binance.com` | Низкая–Средняя |
| Censys | `ssl.cert.subject.cn:"binance.com"` вне IP-диапазонов CloudFront | Низкая |
| Shodan | `ssl.cert.subject.cn:"*.binance.com" -org:"Amazon"` + favicon | Низкая |
| Passive DNS | A-записи `www.binance.com` до CloudFront | Низкая (stale/dead) |
| Subdomain enum | `*.binance.com` вне IP-диапазонов CloudFront | Низкая–Средняя |
| HTTP-заголовки | `Via: 1.1 tesla` уже виден — внутренний прокси, без IP | Только инфо |
| Error pages | имена backend-сервисов в телах 4xx/5xx | Не пробовали |
| Origin probing | не-CloudFront `Server:`, сертификат Binance, свежий `Age` | Вероятно заблокировано |

### Вердикт

- **Не использовать как production-канал.** Реалистичная вероятность успеха
  низкая (однозначные проценты), риск бана IP / нарушения ToS высокий.
- Пассивная литературная проверка (`crt.sh`, `subfinder`) допустима как
  одноразовый эксперимент для проверки гипотезы, но не должна быть встроена
  в production-путь детекции.
- CMS endpoint явно является резервным каналом, не latency-критичным hot path —
  обход кеша CloudFront на резерве не поможет первичному показателю `<150 мс`.

---

## 2. HTML-скрапинг `list/48`

Гипотеза: HTML-скрапинг (https://www.binance.com/en/support/announcement/list/48)
строго хуже CMS REST endpoint. Доказательства подтверждают гипотезу — и даже
сильнее, чем ожидалось.

### Что `list/48` отдаёт обычному HTTP-клиенту

```
HTTP/1.1 202 Accepted
Server: CloudFront
x-amzn-waf-action: challenge      ← AWS WAF
Cache-Control: no-store, max-age=0
X-Cache: Error from cloudfront
Content-Length: 2036
```

Тело — **не HTML приложения** — это AWS WAF JavaScript challenge:

```html
<script src="https://...token.awswaf.com/.../challenge.js"></script>
AwsWafIntegration.getToken().then(() => { window.location.reload(true); });
<noscript>JavaScript is disabled ... Enable JavaScript and then reload.</noscript>
```

### Сравнение: HTML `list/48` vs CMS REST

| | HTML `list/48` | CMS REST `article/list/query` |
|---|---|---|
| Статус | 202 | 200 |
| `X-Cache` | `Error from cloudfront` | `Miss` / `Hit` (кешируется) |
| `x-amzn-waf-action` | `challenge` | отсутствует |
| Кешируется? | Нет (`no-store`) | Да (ETag revalidation) |
| Тело | 2036 байт, WAF challenge | 1278 байт, 5 статей |
| Что нужно для получения данных | JS exec + reload + SPA XHR к CMS REST | Один запрос |

### Ключевое открытие

Даже если решить WAF challenge headless-браузером, SPA внутри сделает XHR к
**тому же CMS REST endpoint**, который проект уже опрашивает напрямую.
HTML-скрапинг — строго надмножество латентности REST без выгоды по свежести.

### Прочие проверки

- **`__NEXT_DATA__` / `__APP_DATA__`**: нет в теле WAF challenge.
- **SSE / EventSource**: на странице нет.
- **`sitemap.xml`**: тот же WAF challenge (HTTP 202, пустое тело).
- **`robots.txt`**: явно запрещает `/bapi/`, `/api/`, `*/sitemap_.xml`.

### Вердикт

- **Окончательный ответ: нет.** HTML-скрапинг `list/48` вообще не является
  рабочим сигналом. AWS WAF отдаёт JS challenge каждому клиенту без cookie,
  поэтому обычный poller не может получить список статей. Даже с
  JS-исполняющим браузером путь данных SPA заканчивается на **том же CMS REST
  endpoint**, который проект уже опрашивает напрямую.

---

## 3. Сторонние сервисы уведомлений о листингах Binance

### Главный вывод

Ни один сторонний сервис не может устойчиво обогнать публичный Binance
Announcements WebSocket. WS push — это *источник*, который эти сервисы
потребляют. Любой сторонний мониторинг Binance добавляет минимум один лишний
хоп (их poll/parse → их сервер → их WS → вы), поэтому хорошо построенный
прямой клиент официального WS уже находится на самом быстром публично
доступном пути.

### Почему сторонний сервис *в принципе* может быть быстрее

- **Co-location / детект в том же регионе**: если их скрапер сидит в том же
  AWS-регионе, что и origin анонсов Binance, а WS-шлюз Binance добавляет
  сериализацию/очереди перед push к удалённым подписчикам, скрапер может
  обнаружить изменение CMS/origin на несколько мс раньше, чем fan-out WS
  достигнет удалённого клиента. CryptoListing.ws именно это и маркетирует
  ("Tokyo endpoint сидит в том же AWS-регионе, что и Binance"). Это
  единственный достоверный механизм.
- **Частные платные институциональные feed'ы**: Binance не документирует
  никаких частных "ранних анонс" feed'ов. Доказательств не найдено. Спекуляция.
- **Инсайдерский доступ**: нелегально (MNPI / манипуляция рынком). Ни один
  легитимный сервис этого не заявляет.
- **Мониторинг upstream-источника, который читает сам Binance**: публичного
  upstream нет; анонс originates у Binance. Неприменимо.

### Находки по сервисам

#### 3.1 CryptoListing.ws

- **Мониторит**: Binance, Upbit, Bithumb — листинги, делистинги, airdrop'ы,
  изменения monitoring-tag, Futures-листинги.
- **Источник**: скрапит официальные анонсы бирж; явно co-located
  ("Tokyo endpoint в том же AWS-регионе, что и Binance").
- **Доставка**: WebSocket `wss://cryptolisting.ws` (плюсеул mirror
  `wss://kr.cryptolisting.ws` для Upbit). µs-точные timestamp'ы, zero-copy
  broadcast. Telegram-канал есть, но реальные алерты идут только через WS.
- **Заявки по латентности**: "#1 Fastest WebSocket provider"; µs-точность
  dispatch. НЕ заявляет явно, что обгоняет официальный WS Binance.
- **Тарифы**:
  - Premium: 0 мс задержки, платный, полный title + ticker.
  - Basic: +20 мс, платный, полный title + ticker.
  - SpeedTrial: 0 мс, бесплатный, продлевается еженедельно — **title &
    ticker REDACTED** на событиях листинга (бесполезно для торговли).
  - FreeDelayed: **+240 мс задержка**, бесплатный еженедельный — полный
    payload, но задержка сама по себе превышает цель 150 мс.
- **Может ли обогнать Binance WS?**: правдоподобно да на Premium (co-location),
  на несколько мс — но Premium платный и вне scope. Бесплатные тарифы не
  помогают.

#### 3.2 CoinMarketCal

- Крипто-события от сообщества по биржам. Источник — community-submitted +
  модерация. Латентность минуты-часы. Только REST API, без WS. Строго медленнее
  официального анонса.

#### 3.3 CoinGecko listings / status updates

- Агрегация листингов бирж. Собственный ingestion-пайплайн CoinGecko.
  Минуты-десятки минут. REST, без listings WS. Endpoint `/status-updates`
  теперь deprecated (404). Строго медленнее, downstream-агрегация.

#### 3.4 Listing-alert / CoinList / Telegram-боты

- **CoinList**: платформа для token-sale, не сервис алертов листингов Binance.
- **Обычные Telegram "listing alert" боты**: обычно на базе Twitter или
  REST-polling. Латентность секунды-минуты. Бесплатно, но медленно и ненадёжно.

#### 3.5 Cointelegraph / CryptoSlate news APIs

- Статьи, написанные людьми. Минуты-часы. У CryptoSlate только RSS +
  newsletter, без real-time API. У Cointelegraph платные news API, но всё
  равно editorial-латентность. Слишком медленно.

#### 3.6 API анонсов других бирж (MEXC / Bybit / KuCoin)

- У KuCoin есть публичный REST "Get Announcements" — REST polling, **нет
  announcements WS**. Bybit/MEXC: нет публичного announcements WebSocket.
- Полезность как cross-exchange-сигнала: НИЗКАЯ для цели листинга Binance.
  Листинг монеты на MEXC/Bybit не предсказывает листинг на Binance.
  REST-poll-bound на 1–5 с.

#### 3.7 GitHub-проекты, скрапящие анонсы Binance

Ни один из перечисленных репозиториев не использует WebSocket для детекции.
Проверен исходный код каждого:

- **eyupbarlas/New-Coin-Listing-Detection-Bot**
  (https://github.com/eyupbarlas/New-Coin-Listing-Detection-Bot) — REST
  polling через `python-binance`, интервал ~10 минут. Латентность до 10 мин.
  Слишком медленно.
- **rickstaa/crypto-listings-sniper**
  (https://github.com/rickstaa/crypto-listings-sniper) — Go-проект, использует
  REST polling `GET /api/v3/ticker/price` через go-binance
  `NewListPricesService()` на `rate.Limiter`, сравнение с предыдущим списком
  (`utils.CompareLists`). Заявляет `<0,3 с` (300 мс) end-to-end в
  Telegram/Discord. 300 мс выше цели 150 мс. Автор отмечает, что Binance ушёл
  с их рынка, что ограничивает поддержку.
- **CyberPunkMetalHead/binance-trading-bot-new-coins**
  (https://github.com/CyberPunkMetalHead/binance-trading-bot-new-coins) —
  Selenium-скрап `https://www.binance.com/en/support/announcement/c-48`,
  извлечение тикеров uppercase из `id='link-0-0-p1'`. Без WS, без
  exchangeInfo. Латентность секунды-минуты (HTML-скрап).
- **CyberPunkMetalHead/new-listings-trading-bot**
  (https://github.com/CyberPunkMetalHead/new-listings-trading-bot) — REST
  polling CMS-эндпоинта анонсов
  `GET /bapi/apex/v1/public/apex/cms/article/list/query?type=1&pageNo=1&pageSize=10`,
  regex `\(([A-Z]+)\)` по title. Латентность секунды. C#-проект
  (`Services/ListingsGetterService.cs`).
- **defidummy/listing-trading-bot**
  (https://github.com/defidummy/listing-trading-bot) — HTTP-скрап анонсов
  Binance (`get_news.py` использует `requests.get` + `lxml` XPath, ищет
  "WILL LIST"). Без WS. Латентность секунды.

**Кто-нибудь решил <150 мс без WS?**: ни одного публичного GitHub-проекта не
найдено. Каждый sub-second проект либо использует WS, либо принимает
сотни-мс-до-секунд латентность. Ни один из четырёх популярных
"binance new-listing" репозиториев выше не использует WebSocket-детекцию;
все полагаются на REST/HTML-скрапинг.

#### 3.7b Техника "new-symbol WS" — почему она НЕ помогает

Существует community-трюк (не официальный stream): подписаться на
`wss://stream.binance.com:9443/ws/!miniTicker@arr` (all-market tickers),
поддерживать множество `known_symbols`, засечённое из
`GET /api/v3/exchangeInfo`, и эмитить детект, когда ранее не виденное поле
`s` появляется в массиве тикеров.

Это **не** помогает проекту, потому что:

- **Нет официального stream'а** для события "символ добавлен на биржу". В
  документации Binance Spot WS перечислены 15 типов stream'ов
  (`!miniTicker@arr`, `!ticker@arr`, `!bookTicker` и т.д.) — ни один не
  срабатывает при добавлении. Futures зеркалирует тот же набор.
- **Трюк детектит "первую тикерную активность"** (matching engine производит
  первое изменение), что происходит *после* того, как символ уже в
  `exchangeInfo`. Это строго медленнее существующего `exchangeinfo_poller.py`
  (который детектит "добавлен на биржу" при t=0 с polling-латентностью ~1–5 с).
- WS анонсов (наш основной) срабатывает в момент публикации, обычно за
  минуты до того, как символ появится в `exchangeInfo`, не говоря уже о
  тиках.
- Может давать ложные срабатывания (устаревший seed snapshot) и пропущенные
  события (временные разрывы соединения).

Вердикт: **не добавлять new-symbol WS-канал**. Это была бы строго более
медленная копия `exchangeinfo_poller.py`.

#### 3.8 Мониторинг Twitter/X (@binance, @BinanceAnnounce)

- Твит @binance публикуется социальной командой Binance *после* push анонса
  на сайте/в WS. Downstream от WS-push; добавляет API-латентность и проблемы
  rate-limit.

### Сравнительная таблица

| Сервис | Мониторит | Источник | Заявленная латентность vs анонса Binance | Бесплатно? | Доставка | Может обогнать Binance WS? |
|---|---|---|---|---|---|---|
| CryptoListing.ws | Binance/Upbit/Bithumb листинги, делистинги, airdrop'ы | Скрапит официальные анонсы, co-located в AWS-регионе Binance | µs-точность dispatch; явно vs WS Binance не заявляет | Бесплатные тарифы: SpeedTrial редачит title (0 мс), FreeDelayed полный payload +240 мс | WebSocket `wss://cryptolisting.ws` | Платный Premium: правдоподобно на несколько мс быстрее. Бесплатный: нет |
| CoinMarketCal | Cross-exchange крипто-события (community) | Community-submitted + модерация | минуты–часы | Ограниченный free REST, платные тарифы | REST API | Нет |
| CoinGecko (listings/status) | Агрегация листингов бирж | Ingestion-пайплайн CoinGecko | минуты–десятки минут | Бесплатный REST, платный Commercial | REST (status-updates deprecated) | Нет |
| CoinList | Собственная платформа token-sale | Н/Д — не листинги Binance | Н/Д | Бесплатно | Web/email | Нет |
| Обычные Telegram-боты (на базе Twitter) | Binance + другие через X | Twitter API / REST polling | секунды–минуты | Бесплатно | Telegram-бот | Нет |
| Cointelegraph / CryptoSlate | Новости | Человеческая editorial | минуты–часы | RSS / платный news API | REST/RSS | Нет |
| API анонсов MEXC/Bybit/KuCoin | Анонсы своих бирж | Официальный REST (KuCoin), нет announcements WS | 1–5 с polling-bound | Бесплатно | REST polling | Нет (другая биржа, REST-bound) |
| GitHub-боты (eyupbarlas, rickstaa, etc.) | Листинги/анонсы Binance | REST polling / exchangeInfo; rickstaa заявляет WS-ish | 10 мин (eyupbarlas); ~300 мс (rickstaa); секунды (остальные) | Бесплатно | Telegram/Discord | Нет — все медленнее прямого Binance WS |
| Twitter/X @binance | Социальные посты Binance | X-аккаунт | в лучшем случае одновременно, обычно после WS-push | Бесплатно (ограничено) / платный X API | REST/streaming | Нет |

### Вердикт

**Не интегрировать ни один из них как hot-path-источник.** Существующий
основной канал проекта (официальный Binance Announcements WebSocket через
`announce_ws_client.py`) уже находится на самом быстром публично доступном
пути к sub-150 мс. Ни один из исследованных сервисов не предлагает более
быстрый *бесплатный* сигнал.

- **CryptoListing.ws** — единственный сервис со структурным преимуществом по
  скорости (co-location в AWS-регионе Binance), но бесплатные тарифы не
  помогают: SpeedTrial редачит ticker (нельзя определить реальную монету),
  а FreeDelayed даёт +240 мс (превышает цель 150 мс сам по себе). Оба
  бесплатных тарифа требуют еженедельного ручного продления и зависят от
  uptime третьей стороны.
  *Опциональное, низкоприоритетное* использование: подписаться на тариф
  **SpeedTrial** только как *cross-validation*-канал — сравнивать его dispatch
  timestamp с `latency_ms` из `announce_ws_client.py`, чтобы подтвердить,
  действительно ли co-located-детектор обгоняет прямой WS. НЕ использовать как
  источник детекции (заредактированные title). Это эксперимент по измерению,
  а не data feed.
- **API анонсов других бирж (KuCoin REST и т.д.)** — пропустить. Не та биржа,
  REST-poll-bound на 1–5 с.
- **News/RSS/community (Cointelegraph, CryptoSlate, CoinMarketCal, CoinGecko)**
  — пропустить полностью. Все они — downstream-слои с человеческой editorial /
  агрегацией, отстают на минуты-часы. Не в том latency-классе.
- **GitHub-боты** — пропустить как feed. Полезны только как reference по
  реализации.
- **Twitter/X @binance** — пропустить. Downstream от WS-push.

---

---

## Приложение А — Справочник прямых CMS endpoint'ов

Все endpoint'ы ниже публичные (без API-ключа) и обслуживаются через
CloudFront (TTL кеша ~60–75 с на Tokyo edge).

### Базовый URL

```
https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query
```

### Параметры запроса

| Параметр | Значение | Примечания |
|----------|----------|------------|
| `type` | `1` | Тип запроса списка статей (единственное наблюдаемое значение) |
| `pageNo` | `1` | Номер страницы, с 1 |
| `pageSize` | одно из `1, 2, 3, 5, 10, 15, 20, 50` | Другие значения возвращают `400 Bad Request` |
| `catalogId` | см. таблицу ниже | ID категории CMS |

### Известные catalogId

| catalogId | Категория |
|-----------|-----------|
| 48 | New Cryptocurrency Listing (== list/48, основная цель) |
| 49 | Latest Binance News |
| 50 | Latest Activities |
| 51 | Delistings |
| 93 | Latest Activities (alt) |
| 128 | P2P Merchant Announcements |
| 157 | Margin / Futures listings |
| 161 | Earn / Staking |

### Примеры запросов (catalogId=48, все cache-ключи, используемые cms_apex_poller.py)

```
https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query?type=1&pageNo=1&pageSize=1&catalogId=48
https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query?type=1&pageNo=1&pageSize=3&catalogId=48
https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query?type=1&pageNo=1&pageSize=5&catalogId=48
https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query?type=1&pageNo=1&pageSize=15&catalogId=48
https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query?type=1&pageNo=1&pageSize=20&catalogId=48
```

Веб-UI использует `pageSize=10` и `pageSize=50`, поэтому эти cache-записи
всегда тёплые. Poller использует редкие pageSizes (`1, 3, 5, 15, 20`), чтобы
повысить вероятность cache-miss за цикл, но после прогрева все ключи всё
равно ~100 % Hit.

### Детальная статья (HTML, SSR через `__APP_DATA`)

```
https://www.binance.com/en/support/announcement/detail/{code}
```

Где `{code}` — hex-код статьи из ответа list query (например
`5c52a77daddf4e0d9769b87cdc91fae4`). HTML-страница встраивает полный JSON
статьи внутри `<script id="__APP_DATA" type="application/json">`. Также
обслуживается через CloudFront (то же поведение stale-while-revalidate).

### Структура ответа (list query)

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

- `id` — уникальный ID статьи, используется для дедупликации (монотонно
  возрастает в рамках категории)
- `code` — hex-код статьи, используется в URL детальной страницы
- `releaseDate` — timestamp публикации в epoch ms (== `publish_ts_ms`,
  используется для расчёта латентности относительно поля `publishDate` из WS)

### Поведение кеша CloudFront

- Cache key = `(catalogId, pageNo, pageSize)`
- TTL ~60–75 с на Tokyo edge (`NRT12-P9`)
- Stale-while-revalidate: после истечения TTL объект остаётся "stale";
  следующий запрос возвращает `RefreshHit` (stale-тело + фоновая
  revalidation), последующий запрос — свежий `Hit`
- Заголовки запроса `Cache-Control: no-cache` / `Pragma: no-cache` НЕ
  обходят CloudFront
- Случайные query-параметры (например `?_=1234567890`) возвращают
  `400 Bad Request`
- Rate-limit CloudFront: ~40 req/2 с с одного IP вызывает HTTP 429
  (цикл бана ~5 мин, повторно накладывается при сохранении нагрузки).
  Безопасная конфигурация: 2 каталога × 5 ключей с интервалом 5 с =
  2 req/sec.

### Что НЕ работает на `api-gcp.binance.com`

GCP-хост — только для market-data (Spot REST API). CMS endpoint'ов там нет:

| Путь на api-gcp.binance.com | Результат |
|---|---|
| `/api/v3/exchangeInfo` | ✅ 200 OK (market data — используется exchangeinfo_poller.py) |
| `/bapi/apex/v1/public/apex/cms/article/list/query` | ❌ 404 Not Found (nginx) |
| `/sapi/wss` | ❌ 404 Not Found |

Другие хосты Binance также не являются жизнеспособными альтернативами для
CMS:
- `data-api.binance.com/bapi/...` → 302 редирект на `www.binance.com` (CloudFront)
- `fapi.binance.com/bapi/...` → 403 Forbidden
- `dapi.binance.com/bapi/...` → 403 Forbidden

Таким образом, CMS-канал привязан к `www.binance.com` (CloudFront) —
GCP / non-CloudFront зеркала CMS-эндпоинта анонсов не существует.

---

## Приложение Б — Справочник composite CMS endpoint'а

Альтернативный CMS-путь, используемый `rickstaa/crypto-listings-sniper` и
каналом `cms_composite_poller.py`. То же поведение кеша CloudFront, что и у
apex-пути, но другая JSON-структура и отсутствие `releaseDate`.

### Базовый URL

```
https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query
```

### Параметры запроса

| Параметр | Значение | Примечания |
|----------|----------|------------|
| `catalogId` | см. таблицу в Приложении А | ID категории CMS |
| `pageNo` | `1` | Номер страницы, с 1 |
| `pageSize` | случайный `10..49` | cache-buster rickstaa: ~40 различных cache-ключей, коллизии часты |

### Примеры запросов (catalogId=48)

```
https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query?catalogId=48&pageNo=1&pageSize=10
https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query?catalogId=48&pageNo=1&pageSize=22
https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query?catalogId=48&pageNo=1&pageSize=49
```

### Структура ответа (composite — плоская)

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

### Сравнение: composite vs apex

| | composite (`/bapi/composite/.../catalog/list/query`) | apex (`/bapi/apex/.../article/list/query`) |
|---|---|---|
| Кеш CloudFront | ✅ Да, идентичный TTL ~60–75 с | ✅ Да, TTL ~60–75 с |
| Latency Hit | ~110 мс | ~120 мс |
| JSON-структура | `data.articles[]` (плоско) | `data.catalogs[].articles[]` (вложено) |
| `releaseDate` | ❌ не возвращается | ✅ epoch ms (нужно для расчёта latency) |
| `catalogName` | ❌ не возвращается | ✅ возвращается |
| Cache-buster | случайный `pageSize` 10..49 | 5 фиксированных pageSizes (1,3,5,15,20) |
| Используется | rickstaa/crypto-listings-sniper, `cms_composite_poller.py` | `cms_apex_poller.py` |

**Вердикт:** composite не даёт **никакого преимущества в обходе кеша** —
CloudFront кеширует его идентично. Apex-путь строго лучше для детекции,
поскольку возвращает `releaseDate` (нужно для расчёта латентности).
Composite-канал оставлен только как канал сравнения/валидации.

---

## 4. Общая рекомендация

| Идея | Вердикт |
|------|---------|
| Polling origin-IP | **Не использовать в production.** Одноразовое пассивное исследование (`crt.sh`, `subfinder`) допустимо как проверка гипотезы, но реальный успех маловероятен, а риск бана IP / нарушения ToS высок. |
| HTML-скрапинг `list/48` | **Окончательный ответ: нет.** AWS WAF challenge + SPA XHR к тому же CMS REST. Строго хуже, чем CMS REST poller. |
| Бесплатные сторонние сервисы | **Не интегрировать как feed.** Опционально: подписаться на CryptoListing.ws SpeedTrial как benchmark для измерения, действительно ли co-located-детектор обгоняет прямой WS. |

### Что мы оставили

Архитектура остаётся трёхканальной:

```
WS        → push анонсов      (<100 мс) ⭐ основной
CMS       → CloudFront REST   (~60–120 с)    резервный
EXCHANGE  → market data       (~1–5 с)       backup
```

Все сторонние источники либо медленнее, либо платные, либо мониторят тот же
upstream. Путь к `<150 мс` — продолжать оптимизировать существующий прямой
клиент Binance Announcements WebSocket (`announce_ws_client.py`): держать
hot path тонким, оставаться на VPS AWS Tokyo (уже низкий RTT к Binance),
одно long-lived WS-соединение, минимальная обработка на кадр, фоновые
воркеры для Telegram / БД.

---

## 5. Классификация каналов по типу события

Шесть каналов делятся на две группы по тому, что они реально детектят.

### События анонсов (публикация новости на Binance)

| Канал | Источник | Что детектит |
|-------|----------|--------------|
| `WS` | Binance Announcements WebSocket (`wss://api.binance.com/sapi/wss`) | Push события публикации анонса |
| `CMS_APEX` | CMS REST apex (`/bapi/apex/.../article/list/query`) | Новая статья в CMS (после TTL кеша CloudFront) |
| `CMS_COMPOSITE` | CMS REST composite (`/bapi/composite/.../catalog/list/query`) | Новая статья в CMS (канал сравнения) |
| `CLWS` / `CLWD` | CryptoListing.ws WebSocket | Push от co-located детектора; покрывает `spot_listing`, `futures_listing`, `spot_delisting`, `futures_delisting`, `hodler_airdrop`, `monitoring_tag_extend`, `monitoring_tag_remove`, `not_listing` |

### События market data (символ появляется в торговой инфраструктуре)

| Канал | Источник | Что детектит |
|-------|----------|--------------|
| `EXCHANGE` | `GET /api/v3/exchangeInfo` | Новая торговая пара зарегистрирована (символ добавлен на биржу) |
| `SNIPER` | `GET /api/v3/ticker/price` | Matching engine начал публиковать цены для пары (pair tradable now) |

### Таймлайн событий для типичного листинга

```
T=0    Binance публикует анонс
       ├─ WS получает push                       ← <100мс
       ├─ CMS_APEX видит в REST (после кеша)     ← ~60-120с
       ├─ CMS_COMPOSITE видит в REST             ← ~60-120с
       └─ CLWS/CLWD получает push                ← 0мс / +240мс

T+?    Символ добавлен в exchangeInfo
       └─ EXCHANGE детектит                      ← ~1-5с (poll interval)

T+??   Matching engine стартует, появляется первая цена
       └─ SNIPER детектит                        ← ~1мс + RTT
```

Каналы анонсов срабатывают первыми (WS — самый быстрый). Каналы market data
срабатывают позже — когда Binance фактически регистрирует пару и matching
engine начинает производить цены. Точный порядок между событиями анонса и
market data зависит от типа листинга (pre-announce, direct listing или
stealth listing).
