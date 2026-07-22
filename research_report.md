# announce_scanner — Research Report

## Overview

Цель: детектирование новых криптовалютных листингов на Binance с latency <150ms.
Исследование проводилось в рамках проекта announce_scanner на AWS Tokyo VPS (Ubuntu, Python asyncio).

---

## 1. Каналы обнаружения анонсов

### 1.1 Binance Announcements WebSocket (primary)

```
wss://api.binance.com/sapi/wss
Topic: com_announcement_en
```

- Push-канал: сообщения приходят сразу после публикации Binance
- `publishDateMs` — таймстемп публикации (эпоха, ms)
- **Latency observed: publish→recv = 1–4 seconds** на тестовых анонсах (не листинги!)
- Важно: мы ловили только новости (category 93 — "Latest Activities"), **ни одного листинга с пятницы 17 июля не было**. На листингах latency может быть другой
- Подтверждено: WS-сообщение содержит `publishDateMs` — единственный канал, где publish-таймстемп доступен напрямую

### 1.2 CMS REST API — Apex (fallback)

```
https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query
```

- REST за CloudFront CDN
- Возвращает `releaseDate` (эпоха, ms) — аналог `publishDateMs` из WS
- Структура: `data.catalogs[].articles[]` (вложенная)
- Параметры: `catalogId`, `pageNo`, `pageSize`
- Используется нашим `cms_apex_poller.py`

### 1.3 CMS REST API — Composite (альтернативный)

```
https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query
```

- Тот же CloudFront CDN
- **НЕ возвращает `releaseDate`** — только `id`, `code`, `title`
- Структура: `data.articles[]` (плоская)
- Используется проектом [rickstaa/crypto-listings-sniper](https://github.com/rickstaa/crypto-listings-sniper)
- Используется нашим `cms_composite_poller.py` как канал сравнения

### 1.4 CMS REST API — Apex Catalog (третий CMS, hybrid)

```
https://www.binance.com/bapi/apex/v1/public/apex/cms/article/catalog/list/query
```

- Тот же CloudFront CDN, `apex`-путь, но плоская структура (как composite)
- **НЕ возвращает `releaseDate`/`publishDate`** — только `id`, `code`, `title`
- Структура: `data.articles[]` (плоская) + `data.total` (общее количество статей)
- Детекция по article `id` (как composite)
- Канал `CMS_APEX_CATALOG` — используется для сравнения recv timing с apex и composite
- Дефолтный интервал: 60s (консервативно)

### 1.5 CryptoListing.ws (третий провайдер)

```
wss://cryptolisting.ws
Tier: SpeedTrial (0ms delay, REDACTED ticker) и FreeDelayed (+240ms, full payload)
```

- Сторонний коммерческий WebSocket-провайдер для pre-market detection
- Использует API-ключи: `dsk_<64 hex>`
- AWS Tokyo (тот же регион, что и наш VPS)
- **Наши тесты: CLWS стабильно быстрее нашего WS на 2-3 секунды**
- `dispatchTimestampUs ≈ detectedTimestampUs` (internal latency ≈ 0μs)
- **НЕ отдаёт `publishDate`** — сильный косвенный признак, что источник — CMS polling, а не Binance WS

### 1.6 Exchange Info (косвенный)

```
https://api.binance.com/api/v3/exchangeInfo
https://api-gcp.binance.com/api/v3/exchangeInfo
https://api4.binance.com/api/v3/exchangeInfo
https://api2.binance.com/api/v3/exchangeInfo (NEW)
```

- Не про анонсы, а про появление пары в trading engine
- Может детектить листинг до/после анонса (stealth listing)
- Нет `listed_at` — latency считается как poll interval

### 1.7 Ticker Price (косвенный)

```
https://api.binance.com/api/v3/ticker/price
```

- Мониторинг появления новых пар в ценах
- Те же хосты, что exchangeInfo

---

## 2. CloudFront — корень проблемы

### 2.1 Как работает CloudFront на www.binance.com

- Все CMS-эндпоинты живут за `www.binance.com` → CloudFront distribution
- **Cache TTL: ~60-75 секунд** (эмпирически)
- Cache key: `(catalogId, pageNo, pageSize)`
- `no-cache` / `Pragma: no-cache` заголовки НЕ пробивают кеш (CloudFront их игнорирует для dynamic content)
- Random query params → 400 Bad Request

### 2.2 Метрики кеша (X-Cache)

| Статус | Значение | Latency |
|---|---|---|
| `Hit from cloudfront` | Кеш попадание (данные старые) | ~110ms |
| `Miss from cloudfront` | Кеш промах (свежие данные от origin) | ~800ms |
| `RefreshHit from cloudfront` | Stale данные + фоновая ревалидация | ~110ms |
| `Error from cloudfront` | Ошибка CloudFront/nginx | — |

### 2.3 Проблема

Поллер не может получить свежий ответ быстрее, чем раз в TTL (~60-75s), **даже при поллинге раз в 1 секунду** — CloudFront просто возвращает Hit из кеша. Miss случается только при уникальном cache key.

---

## 3. Методы обхода CloudFront cache

### 3.1 Множественные cache keys (pageSize bounce)

CloudFront кеширует по URL, включая query-параметры. Используя разные `pageSize`, мы создаём уникальные cache keys. Вероятность Miss для одного ключа ~20% (эмпирически).

**Strategy A — фиксированный набор (устарел):**
5 pageSizes: `[1, 3, 5, 15, 20]`
Вероятность хотя бы одного Miss за цикл: 1 - 0.8⁵ ≈ 67%

**Strategy B — рандомизированный (текущий):**
N случайных pageSize из 1..49, новые на каждый цикл.
`CMS_NUM_CACHE_KEYS=10` → 1 - 0.8¹⁰ ≈ 89% за цикл
`CMS_NUM_CACHE_KEYS=5` → 1 - 0.8⁵ ≈ 67%

### 3.2 Реактивный burst (adaptive polling)

При обнаружении Miss/RefreshHit — переключение в fast-режим:
- `CMS_POLL_FAST_INTERVAL_S=0.3s` (было 0.5s)
- `CMS_POLL_FAST_CYCLES=10` (было 4)
- Это увеличивает шанс поймать свежий Miss в следующих циклах

### 3.3 Пул поллеров со сдвигом (Multi-instance)

Гипотеза (требует ресурсов): N независимых серверов, стартуют со сдвигом +T ms.
CloudFront видит каждого как отдельного клиента — кеш сбрасывается независимо.
Это вероятно использует CryptoListing для достижения sub-100ms latency.

### 3.4 Origin bypass (не работает)

Проверено:

| Хост | Результат |
|---|---|
| `bapi.binance.com` | 302 → `www.binance.com/en` |
| `bapi-gcp.binance.com` | 302 (те же IP) |
| `bapi4.binance.com` | 302 (тот же nginx) |
| `bapi2.binance.com` | 302 (тот же nginx) |

**Все bapi* хосты — фронтенд-редиректоры, а не API-бэкенд.** CMS API живёт исключительно за `www.binance.com` (CloudFront).

### 3.5 Origin IP discovery (не работает)

CloudFront origin принимает трафик только от своих edge-серверов.
Даже при знании origin IP — AWS WAF завернёт прямой запрос.
**Бесперспективно без дорогих инфраструктурных решений.**

---

## 4. Хосты Binance API — карта

### 4.1 Market Data (exchangeInfo / ticker) — есть обход

| Хост | CDN | Тип | Используется |
|---|---|---|---|
| `api.binance.com` | CloudFront (d3h36i1mno13q3) | AWS | ✅ exchange, sniper |
| `api-gcp.binance.com` | GCP GLB (no cache) | Google Cloud | ✅ exchange, sniper |
| `api4.binance.com` | nginx direct | AWS EC2 | ✅ exchange, sniper |
| `api2.binance.com` | nginx direct (NEW) | AWS EC2 | ✅ exchange, sniper (добавлен) |

Эти хосты обслуживают Spot REST API (`/api/v3/exchangeInfo`, `/api/v3/ticker/price`).
**CMS-эндпоинтов на них нет** — только market data.

### 4.2 CMS Announcements — обхода нет

| Хост | Результат |
|---|---|
| `www.binance.com/bapi/apex/...` | ✅ CloudFront |
| `www.binance.com/bapi/composite/...` | ✅ CloudFront |
| `bapi*.binance.com` | ❌ 302 redirect |
| `api-gcp.binance.com/bapi/...` | ❌ 404 |
| `api4.binance.com/bapi/...` | ❌ 404 |
| `api2.binance.com/bapi/...` | ❓ не проверялся, но вероятно 404 |

**Вывод:** CMS-канал привязан к `www.binance.com` (CloudFront).
GCP/non-CloudFront зеркала CMS-эндпоинта анонсов **не существует**.

### 4.3 Найденные CMS-эндпоинты

| Endpoint | `releaseDate` | Структура |
|---|---|---|
| `/bapi/apex/.../article/list/query` | ✅ `releaseDate` (ms) | `data.catalogs[].articles[]` |
| `/bapi/composite/.../article/catalog/list/query` | ❌ | `data.articles[]` (плоская) |
| `/bapi/apex/.../article/catalog/list/query` | ❌ `publishDate` = None | `data.articles[]` + `total` |

Третий эндпоинт (`catalog/list/query`) — `publishDate` всегда `None`, но поднят как канал `CMS_APEX_CATALOG` для сравнения `recv_ts` с другими каналами.

---

## 5. CryptoListing.ws — анализ конкурента

### 5.1 Что мы знаем

- Co-located в AWS Tokyo (как и наш VPS)
- **SpeedTrial tier: 0ms delay** — dispatch сразу после detect
- `dispatchTimestampUs ≈ detectedTimestampUs` — internal latency ~0μs
- Стабильно быстрее нашего WS канала на **2-3 секунды**
- **НЕ отдаёт `publishDateMs`** в сообщениях

### 5.2 Почему это CMS polling, а не Binance WS

1. **Отсутствие publishDateMs** — если бы они были подписаны на Binance Announcements WS, publishDateMs был бы в каждом сообщении. Сознательно вырезать это поле бессмысленно (8 байт, сервис про транзакционность).
2. **dispatch ≈ detect** — dispatch происходит в ту же микросекунду, что и detect. Для WS-релея нужна хотя бы минимальная задержка на обработку + запись в сокет (sub-ms, но не 0).
3. **Схема поллинга** — их метрика "Dispatch delay = dispatchTimestampUs − detectedTimestampUs" идентична логике CMS поллера.

### 5.3 Почему CL быстрее нашего WS

- Наш WS: publish→recv = 1-4s. WS-канал Binance имеет внутреннюю задержку (возможно batch-агрегация или очередь)
- CL: detect ≈ publish + 900ms (co-located CMS polling, первый Miss после публикации)
- **Разница ~2-3 секунды** — CL детектит через CMS быстрее, чем официальный WS доставляет

### 5.4 Как CL может обходить CloudFront

Наиболее вероятное объяснение — **пул поллеров со сдвигом**:
- N параллельных серверов в AWS Tokyo
- Каждый поллит CMS с уникальными cache keys (pageSize)
- Стартовый сдвиг между инстансами: +50-100ms
- CloudFront кеш независим для каждого сервера
- Результат: хотя бы один Miss каждые N×interval ms

---

## 6. Прямой HTML-скрапинг

Рассматривался как метод, но отклонён:

- Страница `https://www.binance.com/en/support/announcement/list/48`
- Под капотом — XHR к тем же CMS endpoint'ам (`bapi/apex/...`)
- CloudFront + JS-рендеринг делают HTML-скрапинг медленнее прямого API-поллинга
- **Наш CMS поллер делает те же запросы, но без лишней обёртки HTML+JS**

---

## 7. Ограничения и выводы

### 7.1 Текущие лимиты

- **CMS поллер упирается в CloudFront TTL** — даже с 10 cache keys и burst-режимом
- **Binance WS имеет unexplained latency** — 1-4s от publish до recv
- **CL быстрее нас** — вероятно за счёт пула поллеров
- **CL ≈1s latency на новостях** — на тех анонсах (НЕ листинги, категория новостей), которые мы детектили, CL давал ~1s задержку. Для листинг-сценариев история может быть другой — на реальных листингах CL может быть быстрее или медленнее, мы ещё не детектили

### 7.2 Направления оптимизации

1. **CMS:** подбор граничной частоты запросов (без rate limit), масштабирование через N инстансов со сдвигом (multi-instance polling)
2. **WS:** мониторинг latency на реальных листингах (возможно на listing-ах WS быстрее, чем на новостях)

