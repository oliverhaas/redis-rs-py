"""Top-level re-exports for redis-py compatibility.

`from redis_rs_py import RedisError` must work, and the class must be
identical to the one importable from `redis_rs_py.exceptions`.
"""

import importlib

import pytest

PUBLIC_NAMES = [
    "AskError",
    "AuthenticationError",
    "AuthenticationWrongNumberOfArgsError",
    "BusyLoadingError",
    "ClusterCrossSlotError",
    "ClusterDownError",
    "ClusterError",
    "ConnectionError",
    "DataError",
    "ExecAbortError",
    "InvalidResponse",
    "LockError",
    "LockNotOwnedError",
    "MasterDownError",
    "ModuleError",
    "MovedError",
    "NoPermissionError",
    "NoScriptError",
    "OutOfMemoryError",
    "PubSubError",
    "ReadOnlyError",
    "RedisError",
    "ResponseError",
    "SlaveError",
    "TimeoutError",
    "TryAgainError",
    "WatchError",
]


@pytest.mark.parametrize("name", PUBLIC_NAMES)
def test_top_level_reexport_is_identical_to_module_class(name: str) -> None:
    pkg = importlib.import_module("redis_rs_py")
    mod = importlib.import_module("redis_rs_py.exceptions")
    assert getattr(pkg, name) is getattr(mod, name)


def test_redis_py_user_idiom_works() -> None:
    """A redis-py user does `from redis_rs_py import RedisError, ConnectionError`
    and catches both. We must not collide with builtins.ConnectionError."""
    import builtins  # noqa: PLC0415

    from redis_rs_py import ConnectionError, RedisError  # noqa: PLC0415

    assert issubclass(ConnectionError, RedisError)
    # Use the `builtins` module rather than `__builtins__` indexing — the latter
    # is dict-typed in module scope but module-typed under `__main__` (CPython
    # quirk) and is wholly different on PyPy.
    assert ConnectionError is not builtins.ConnectionError
