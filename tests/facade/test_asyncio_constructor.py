"""Constructor surface for redis_rs_py.asyncio.Redis."""

import pytest
from redis_rs_py.asyncio import Redis


@pytest.mark.asyncio
async def test_default_constructor_accepts_no_kwargs(valkey_url: str) -> None:
    from urllib.parse import urlparse

    parts = urlparse(valkey_url)
    r = Redis(host=parts.hostname, port=parts.port)
    assert await r.ping() is True
    await r.aclose()


@pytest.mark.asyncio
async def test_constructor_accepts_full_redis_py_kwarg_surface() -> None:
    from redis_rs_py.exceptions import ConnectionError as RedisConnectionError

    with pytest.raises(RedisConnectionError):
        Redis(
            host="127.0.0.1",
            port=1,
            db=0,
            password=None,
            socket_timeout=None,
            socket_connect_timeout=None,
            socket_keepalive=False,
            socket_keepalive_options=None,
            connection_pool=None,
            unix_socket_path=None,
            encoding="utf-8",
            encoding_errors="strict",
            charset=None,
            errors=None,
            decode_responses=False,
            retry_on_timeout=False,
            retry_on_error=None,
            ssl=False,
            ssl_keyfile=None,
            ssl_certfile=None,
            ssl_cert_reqs="required",
            ssl_ca_certs=None,
            ssl_ca_path=None,
            ssl_ca_data=None,
            ssl_check_hostname=False,
            ssl_password=None,
            ssl_validate_ocsp=False,
            ssl_validate_ocsp_stapled=False,
            ssl_ocsp_context=None,
            ssl_ocsp_expected_cert=None,
            ssl_min_version=None,
            ssl_ciphers=None,
            max_connections=None,
            single_connection_client=False,
            health_check_interval=0,
            client_name=None,
            lib_name="redis-rs-py",
            lib_version="0.0.0",
            username=None,
            retry=None,
            redis_connect_func=None,
            credential_provider=None,
            protocol=2,
            cache=None,
            cache_config=None,
            event_dispatcher=None,
        )
