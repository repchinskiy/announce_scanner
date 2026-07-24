#!/usr/bin/env python3
"""Проверить X-Cache заголовки для всех CMS эндпоинтов с разными бэкендами."""

import asyncio
from http_client import create_http_client

URLS = {
    "CMS_COMPOSITE": "https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query?catalogId=48&pageNo=1&pageSize=10",
    "CMS_APEX":      "https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query?type=1&catalogId=48&pageNo=1&pageSize=5",
    "CMS_CATALOG":   "https://www.binance.com/bapi/apex/v1/public/apex/cms/article/catalog/list/query?catalogId=48&pageNo=1&pageSize=10",
}


async def test(backend: str):
    print(f"\n{'='*60}")
    print(f"  backend = {backend}")
    print(f"{'='*60}")
    try:
        async with create_http_client(backend=backend, timeout=10.0) as session:
            for name, url in URLS.items():
                print(f"\n  --- {name} ---")
                for i in range(3):
                    resp = await session.get(url, headers={"User-Agent": "Mozilla/5.0"})
                    cache_val = resp.header("X-Cache", "—MISSING—")
                    raw_keys = [k for k in resp.headers if k.lower() == "x-cache"]
                    print(f"    req#{i+1}:  {resp.status_code}  "
                          f"value={cache_val!r}  raw_keys={raw_keys}")
    except Exception as e:
        print(f"  ERROR: {e}")


async def main():
    for b in ("aiohttp", "curl_cffi", "httpx"):
        await test(b)


if __name__ == "__main__":
    asyncio.run(main())
