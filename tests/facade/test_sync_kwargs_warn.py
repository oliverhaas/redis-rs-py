"""Tests for the accept-and-warn behaviour on unknown / unimplemented kwargs."""

from __future__ import annotations

import warnings

import pytest


@pytest.fixture(autouse=True)
def _reset_warn_state():
    """Clear the OnceLock dedup state before every test so warnings fire afresh."""
    from redis_rs_py._driver import _facade_reset_warn_state

    _facade_reset_warn_state()
    yield
    _facade_reset_warn_state()


def test_unimplemented_known_kwarg_warns():
    """A redis-py kwarg we don't implement yet emits exactly one UserWarning."""
    from redis_rs_py import Redis

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        r = Redis(retry_on_timeout=True)
        r.close()

    user_warns = [x for x in w if issubclass(x.category, UserWarning)]
    assert len(user_warns) == 1
    assert "retry_on_timeout" in str(user_warns[0].message)


def test_unimplemented_kwarg_warns_once():
    """The same unimplemented kwarg only emits one warning per process (deduped)."""
    from redis_rs_py import Redis

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        r1 = Redis(retry_on_timeout=True)
        r1.close()
        r2 = Redis(retry_on_timeout=True)
        r2.close()

    user_warns = [x for x in w if issubclass(x.category, UserWarning)]
    assert len(user_warns) == 1, "Expected exactly one warning for repeated kwarg"


def test_unknown_kwarg_emits_runtime_warning():
    """A kwarg not in the redis-py 5.x signature at all emits a RuntimeWarning."""
    from redis_rs_py import Redis

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        r = Redis(totally_made_up_kwarg=42)
        r.close()

    runtime_warns = [x for x in w if issubclass(x.category, RuntimeWarning)]
    assert len(runtime_warns) == 1
    assert "totally_made_up_kwarg" in str(runtime_warns[0].message)


def test_implemented_kwargs_no_warning(valkey_url: str):
    """Implemented kwargs (host, port, db, …) produce no warnings."""
    from redis_rs_py import Redis

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        r = Redis.from_url(valkey_url)
        r.close()

    user_warns = [x for x in w if issubclass(x.category, (UserWarning, RuntimeWarning))]
    assert user_warns == [], f"Unexpected warnings: {user_warns}"


def test_multiple_unimplemented_kwargs_each_warn_once():
    """Multiple distinct unimplemented kwargs each produce one warning."""
    from redis_rs_py import Redis

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        r = Redis(retry_on_timeout=True, retry_on_error=[])
        r.close()

    user_warns = [x for x in w if issubclass(x.category, UserWarning)]
    assert len(user_warns) == 2
    names = {str(x.message) for x in user_warns}
    assert any("retry_on_timeout" in n for n in names)
    assert any("retry_on_error" in n for n in names)
