"""redis-rs-py — high-performance, drop-in replacement for redis-py.

The public surface is added by plan 10 (sync facade) and plan 11
(asyncio facade). For now, only the low-level driver and the exception
hierarchy are exposed.
"""

from redis_rs_py import exceptions
from redis_rs_py._driver import RedisRsAwaitable, RedisRsDriver, __version__
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
    "RedisRsAwaitable",
    "RedisRsDriver",
    "ResponseError",
    "SlaveError",
    "TimeoutError",
    "TryAgainError",
    "WatchError",
    "__version__",
    "exceptions",
]
