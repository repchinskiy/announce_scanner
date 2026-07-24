#!/usr/bin/env python3
"""Минимальный тест curl_cffi напрямую, без нашей обёртки."""

import asyncio
import sys


async def main():
    print(f"Python {sys.version}")
    print()

    from curl_cffi import AsyncSession, __version__
    print(f"curl_cffi version: {__version__}")

    url = "https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query?catalogId=48&pageNo=1&pageSize=10"

    # Тест 1: AsyncSession без impersonate (как обычный curl)
    print(f"\n--- Test 1: AsyncSession (no impersonate) ---")
    try:
        session = AsyncSession(timeout=10)
        resp = await session.get(url, headers={"User-Agent": "Mozilla/5.0"})
        print(f"  resp type: {type(resp)}")
        print(f"  resp is None: {resp is None}")
        if resp is not None:
            print(f"  status_code: {resp.status_code}")
            print(f"  headers type: {type(resp.headers)}")
            print(f"  has .items(): {hasattr(resp.headers, 'items')}")
            print(f"  X-Cache: {resp.headers.get('X-Cache', 'MISSING')}")
        await session.close()
    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {e}")

    # Тест 2: AsyncSession с impersonate=chrome146
    print(f"\n--- Test 2: AsyncSession (impersonate=chrome146) ---")
    try:
        session = AsyncSession(timeout=10, impersonate="chrome146")
        resp = await session.get(url, headers={"User-Agent": "Mozilla/5.0"})
        print(f"  resp type: {type(resp)}")
        print(f"  resp is None: {resp is None}")
        if resp is not None:
            print(f"  status_code: {resp.status_code}")
            print(f"  X-Cache: {resp.headers.get('X-Cache', 'MISSING')}")
        await session.close()
    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {e}")

    # Тест 3: curl_cffi requests API (синхронный)
    print(f"\n--- Test 3: requests-style API ---")
    try:
        from curl_cffi import requests
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, impersonate="chrome146")
        print(f"  status_code: {r.status_code}")
        print(f"  X-Cache: {r.headers.get('X-Cache', 'MISSING')}")
    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {e}")


if __name__ == "__main__":
    asyncio.run(main())
