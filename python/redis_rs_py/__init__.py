"""redis-rs-py — high-performance, drop-in replacement for redis-py.

The public surface is added by plan 10 (sync facade) and plan 11
(asyncio facade). For now, only the low-level driver and the exception
hierarchy are exposed.
"""

from redis_rs_py import exceptions
from redis_rs_py._driver import Lock, Pipeline, PubSub, PubSubWorkerThread, Redis, RedisRsAwaitable, __version__
from redis_rs_py._scan_iter import scan_iter as _scan_iter
from redis_rs_py.asyncio._scan_iter import scan_iter_async as _scan_iter_async
from redis_rs_py.exceptions import (
    AskError,
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    BusyLoadingError,
    ClusterCrossSlotError,
    ClusterDownError,
    ClusterError,
    ConnectionError,
    DataError,
    ExecAbortError,
    InvalidResponse,
    LockError,
    LockNotOwnedError,
    MasterDownError,
    ModuleError,
    MovedError,
    NoPermissionError,
    NoScriptError,
    OutOfMemoryError,
    PubSubError,
    ReadOnlyError,
    RedisError,
    ResponseError,
    SlaveError,
    TimeoutError,
    TryAgainError,
    WatchError,
)

# Attach the Python-side scan_iter helpers to the Rust pyclass. Done
# here so users get `redis.scan_iter(...)` and `redis.scan_iter_async(...)`
# without an extra import step. (See _scan_iter.py for why these can't
# be Rust pyclass methods.)
Redis.scan_iter = _scan_iter  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
Redis.scan_iter_async = _scan_iter_async  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]

__all__ = [
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
    "Lock",
    "LockError",
    "LockNotOwnedError",
    "MasterDownError",
    "ModuleError",
    "MovedError",
    "NoPermissionError",
    "NoScriptError",
    "OutOfMemoryError",
    "Pipeline",
    "PubSub",
    "PubSubError",
    "PubSubWorkerThread",
    "ReadOnlyError",
    "Redis",
    "RedisError",
    "RedisRsAwaitable",
    "ResponseError",
    "SlaveError",
    "TimeoutError",
    "TryAgainError",
    "WatchError",
    "__version__",
    "exceptions",
]
