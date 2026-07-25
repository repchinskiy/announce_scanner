#!/usr/bin/env python3
"""
grpc_explore.py — исследование gRPC endpoints Binance.

Зависимости (устанавливать вручную, не входят в requirements.txt):
    pip install grpcio grpcio-tools grpcio-reflection

Запуск:
    python grpc_explore.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Проверка наличия grpc ДО первого import
# ---------------------------------------------------------------------------
try:
    import grpc  # noqa: F401
except ModuleNotFoundError:
    print("❌ Модуль 'grpc' не установлен.", file=sys.stderr)
    print("Установи вручную (не входит в requirements.txt проекта):", file=sys.stderr)
    print("    pip install grpcio grpcio-tools grpcio-reflection", file=sys.stderr)
    sys.exit(1)

import grpc
import hashlib
import hmac
import time
from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc

# ---------------------------------------------------------------------------
# Настройка логирования
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("grpc_explore")

# ---------------------------------------------------------------------------
# Загрузка .env (как везде в проекте)
# ---------------------------------------------------------------------------
from dotenv import load_dotenv  # noqa: E402

load_dotenv(Path(__file__).resolve().parent / ".env")

API_KEY = os.environ.get("BINANCE_API_KEY", "")
API_SECRET = os.environ.get("BINANCE_API_SECRET", "")

# ---------------------------------------------------------------------------
# gRPC хосты
# ---------------------------------------------------------------------------
HOSTS = [
    "grpc.binance.com:443",
    "grpc-api.binance.com:443",
]

# ---------------------------------------------------------------------------
# Схемы аутентификации
# ---------------------------------------------------------------------------
def _make_auth_schemes() -> list[dict]:
    schemes: list[dict] = [
        {"name": "no auth", "metadata": []},
    ]

    if API_KEY:
        schemes.append({
            "name": "x-mbx-apikey (lowercase)",
            "metadata": lambda: [("x-mbx-apikey", API_KEY)],
        })
        schemes.append({
            "name": "x-mbx-apikey + HMAC-SHA256",
            "metadata": lambda: _signed_metadata(),
        })
        schemes.append({
            "name": "authorization Bearer",
            "metadata": [("authorization", f"Bearer {API_KEY}")],
        })
        schemes.append({
            "name": "x-api-key",
            "metadata": [("x-api-key", API_KEY)],
        })
        schemes.append({
            "name": "apikey (short)",
            "metadata": [("apikey", API_KEY)],
        })

    return schemes


def _signed_metadata() -> list[tuple[str, str]]:
    ts = int(time.time() * 1000)
    msg = f"timestamp={ts}".encode("utf-8")
    sig = hmac.new(API_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()
    return [
        ("x-mbx-apikey", API_KEY),
        ("x-mbx-timestamp", str(ts)),
        ("x-mbx-signature", sig),
    ]


# ---------------------------------------------------------------------------
# Попытка gRPC reflection
# ---------------------------------------------------------------------------
async def try_reflection(
    host: str,
    metadata: list[tuple[str, str]] | None = None,
) -> bool:
    md = metadata or []

    try:
        channel = grpc.aio.secure_channel(host, grpc.ssl_channel_credentials())
        stub = reflection_pb2_grpc.ServerReflectionStub(channel)

        request = reflection_pb2.ServerReflectionRequest()
        request.list_services = ""

        log.info("  sending reflection request (timeout=5s) …")
        async for response in stub.ServerReflectionInfo(
            iter([request]),
            timeout=5.0,
            metadata=md,
        ):
            if response.HasField("list_services_response"):
                services = response.list_services_response.service
                log.info("  ✅ SUCCESS — %d service(s):", len(services))
                for svc in services:
                    log.info("    • %s", svc.name)
                return True
            else:
                log.info("  response field: %s", response.WhichOneof("message_response"))
                return True

        await channel.close()
        return True

    except grpc.aio.AioRpcError as e:
        code = e.code()
        details = e.details() or ""
        if "464" in details:
            log.info("  ❌ WAF blocked (HTTP 464)")
        elif code == grpc.StatusCode.UNAUTHENTICATED:
            log.info("  ❌ UNAUTHENTICATED — возможно, нужен другой ключ")
        elif code == grpc.StatusCode.PERMISSION_DENIED:
            log.info("  ❌ PERMISSION_DENIED — ключ есть, доступа нет")
        elif code == grpc.StatusCode.UNIMPLEMENTED:
            log.info("  ❌ UNIMPLEMENTED — reflection не поддерживается")
        elif code == grpc.StatusCode.INTERNAL:
            log.info("  ❌ INTERNAL — %s", details[:120])
        else:
            log.info("  ❌ %s — %s", code, details[:120])
        return False
    except Exception as e:
        log.info("  ❌ %s: %s", type(e).__name__, e)
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main() -> None:
    log.info("=" * 60)
    log.info("gRPC Explorer for Binance")
    log.info("API_KEY present: %s", bool(API_KEY))
    log.info("API_SECRET present: %s", bool(API_SECRET))
    log.info("=" * 60)

    auth_schemes = _make_auth_schemes()

    for host in HOSTS:
        log.info("\n——— Host: %s ———", host)

        for scheme in auth_schemes:
            name = scheme["name"]
            md = scheme["metadata"]
            if callable(md):
                md = md()

            log.info("  >> auth: %s", name)
            log.info("     metadata: %s", md)
            ok = await try_reflection(host, metadata=md)
            if ok:
                log.info("  ✅ %s — auth works!", name)
                break
            log.info("")

    log.info("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
