# announce_scanner
```
Реализован сканер новых анонсов Binance с несколькими независимыми каналами детекции, сравнением latency и системой нотификаций.
> CMS — Content Management System (Binance API, возвращающий список статей анонсов, то что под капотом у HTML через xhr)  
> H / M / R — CloudFront cache header X-Cache: Hit (кеш), Miss (промах, пошли в origin - свежие данные), RefreshHit (stale + фоновая ревалидация)
## Каналы детекции
1. Binance Announcements WS (primary)
`wss://api.binance.com/sapi/wss` → topic `com_announcement_en`
- Единственный канал с publishDateMs от Binance напрямую
- На тестовых новостях: publish→recv = 1-4s (аномально много)
- На реальных листингах latency может быть другой — не детектили ещё
2. CMS REST — Apex (fallback)
`/bapi/apex/.../article/list/query`
- Есть releaseDate (ms) — publish-таймстемп
- CloudFront TTL ~60-75s — ключевое ограничение
3. CMS REST — Composite (альтернативный)
`/bapi/composite/.../article/catalog/list/query`
- Нет releaseDate/publishDate — сравниваем recv timing
- Плоская структура, используется rickstaa/crypto-listings-sniper
4. CMS REST — Apex Catalog (третий CMS)
`/bapi/apex/.../article/catalog/list/query`
- Нет publishDate, поднят как отдельный канал для сравнения recv_ts
5. CryptoListing.ws (сторонний провайдер)
- AWS Tokyo co-location (как у нас)
- SpeedTrial: 0ms delay, dispatch≈detect (~0μs internal)
- Стабильно быстрее нашего WS на 2-3s
- НЕ отдаёт publishDateMs — сильный признак CMS polling, а не WS relay
- На новостях ≈1s задержка; для листингов может быть иначе
6. Exchange Info / Ticker Price (косвенные, не имеют отношения к анонсам, а напрямую к появлению пары в trading engine)
- Может детектить листинг раньше анонса (stealth listing) или сразу после появления в ордербуке
- Хосты: api, api-gcp, api4, api2.binance.com — есть обход CloudFront через GCP/nginx direct
---
## CloudFront — корень проблемы
- Все CMS эндпоинты за www.binance.com → CloudFront
- Cache TTL ~60-75s, ключ = (catalogId, pageNo, pageSize)
- no-cache заголовки игнорируются
- Random params → 400 Bad Request
Попытка обходов кеша: случайные pageSize (1..49) — N ключей больше вероятность Miss
---
## CryptoListing — как они работают (наш best guess)
1. CMS polling, не WS — иначе отдавали бы publishDateMs
2. Пул поллеров со сдвигом — N серверов стартуют с шагом +50-100ms, CloudFront кеш независим для каждого
3. Co-location в AWS Tokyo даёт ~1ms RTT до Binance
---
## Лимиты и направления
- CMS упирается в CloudFront TTL — без пула инстансов не обойти (масштабирование)
- Binance WS имеет unexplained latency 1-4s (возможно batch-агрегация)
- bapi* хосты — 302 redirect, CMS только за www.binance.com (CloudFront)
- Origin bypass не работает — CloudFront WAF блокирует прямой трафик
- Для более репрезентативной статистики нужны замеры на реальных листингах (с пт 17го не было ни одного листинга)
- текущие данные получены на новостных анонсах (категория 93/Latest Activities) 
- на листингах latency всех каналов может отличаться
```
