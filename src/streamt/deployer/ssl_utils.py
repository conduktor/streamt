"""Shared SSL utilities for requests-based deployers."""

from __future__ import annotations

import ssl
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context


class SSLAdapter(HTTPAdapter):
    """HTTPAdapter that uses a custom ssl.SSLContext (supports key passwords)."""

    def __init__(self, ssl_context: ssl.SSLContext, **kwargs):
        self._ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self._ssl_context
        return super().init_poolmanager(*args, **kwargs)


def configure_session_ssl(
    session: requests.Session,
    ssl_ca_location: Optional[str] = None,
    ssl_certificate_location: Optional[str] = None,
    ssl_key_location: Optional[str] = None,
    ssl_key_password: Optional[str] = None,
) -> None:
    """Configure SSL/mTLS on a requests Session.

    When ssl_key_password is provided, creates a custom SSLContext and mounts
    an SSLAdapter. Otherwise uses the simpler session.verify/session.cert API.
    """
    if ssl_ca_location:
        session.verify = ssl_ca_location

    if ssl_key_password and ssl_certificate_location and ssl_key_location:
        ctx = create_urllib3_context()
        if ssl_ca_location:
            ctx.load_verify_locations(ssl_ca_location)
        ctx.load_cert_chain(
            certfile=ssl_certificate_location,
            keyfile=ssl_key_location,
            password=ssl_key_password,
        )
        adapter = SSLAdapter(ctx)
        session.mount("https://", adapter)
    elif ssl_certificate_location and ssl_key_location:
        session.cert = (ssl_certificate_location, ssl_key_location)
    elif ssl_certificate_location:
        session.cert = ssl_certificate_location
