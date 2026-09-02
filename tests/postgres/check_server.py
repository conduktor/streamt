"""Fail CI early when its explicit PostgreSQL service is not the requested major."""

from __future__ import annotations

import os

import psycopg


def main() -> None:
    dsn = os.environ.get("STREAMT_TEST_POSTGRES_ADMIN_DSN")
    expected_text = os.environ.get("STREAMT_TEST_POSTGRES_MAJOR")
    if not dsn or not expected_text:
        raise SystemExit("explicit PostgreSQL conformance environment is unavailable")

    expected_major = int(expected_text)
    with psycopg.connect(dsn, connect_timeout=5) as connection:
        actual_major = connection.info.server_version // 10_000
    if actual_major != expected_major:
        raise SystemExit(
            f"expected PostgreSQL major {expected_major}, got {actual_major}"
        )
    print(f"PostgreSQL {actual_major} conformance service is ready")


if __name__ == "__main__":
    main()
