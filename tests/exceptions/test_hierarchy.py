"""The exception hierarchy must mirror redis.exceptions exactly."""

import pytest

EXPECTED_EXCEPTIONS = {
    "RedisError": ("Exception",),
    "ConnectionError": ("RedisError",),
    "TimeoutError": ("ConnectionError",),
    "BusyLoadingError": ("ConnectionError",),
    "AuthenticationError": ("ConnectionError",),
    "AuthenticationWrongNumberOfArgsError": ("AuthenticationError",),
    "ResponseError": ("RedisError",),
    "DataError": ("RedisError",),
    "InvalidResponse": ("RedisError",),
    "OutOfMemoryError": ("ResponseError",),
    "NoScriptError": ("ResponseError",),
    "ExecAbortError": ("ResponseError",),
    "ReadOnlyError": ("ResponseError",),
    "NoPermissionError": ("ResponseError",),
    "ModuleError": ("ResponseError",),
    "LockError": ("RedisError",),
    "LockNotOwnedError": ("LockError",),
    "WatchError": ("RedisError",),
    "PubSubError": ("RedisError",),
    "MasterDownError": ("ConnectionError",),
    "SlaveError": ("RedisError",),
    "ClusterError": ("RedisError",),
    "ClusterDownError": ("ResponseError", "ClusterError"),
    "ClusterCrossSlotError": ("ResponseError", "ClusterError"),
    "MovedError": ("ClusterError",),
    "AskError": ("ClusterError",),
    "TryAgainError": ("ClusterError",),
}


@pytest.mark.parametrize("name,bases", list(EXPECTED_EXCEPTIONS.items()))
def test_exception_class_exists_with_bases(name: str, bases: tuple[str, ...]) -> None:
    from redis_rs_py.exceptions import __dict__ as exc_dict

    assert name in exc_dict, f"{name} missing"
    cls = exc_dict[name]
    assert issubclass(cls, Exception)

    # Every declared base must appear in the MRO.
    mro_names = {b.__name__ for b in cls.__mro__}
    for base in bases:
        assert base in mro_names, f"{name} MRO missing {base}: {sorted(mro_names)}"


def test_redis_error_is_root() -> None:
    from redis_rs_py.exceptions import RedisError

    assert issubclass(RedisError, Exception)


def test_python_builtin_connection_error_is_unrelated() -> None:
    """redis.exceptions.ConnectionError is *NOT* the Python builtin one
    (despite the name collision). We mirror that — our ConnectionError is
    a RedisError subclass, not the stdlib one."""
    import builtins

    from redis_rs_py.exceptions import ConnectionError as RedisConnectionError

    assert RedisConnectionError is not builtins.ConnectionError
    assert not issubclass(RedisConnectionError, builtins.ConnectionError)
