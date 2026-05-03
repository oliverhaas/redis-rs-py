"""Re-export the redis.exceptions-compatible hierarchy from the Rust core.

`from redis_rs_py.exceptions import RedisError` works.
`from redis_rs_py import RedisError` also works (via __init__.py).
"""

import sys

import redis_rs_py._driver as _drv

# PyO3 add_submodule does not automatically register the submodule in
# sys.modules under its dotted name. We register it here so that
# `import redis_rs_py._driver.exceptions` works as expected.
_exc_mod = _drv.exceptions
sys.modules.setdefault("redis_rs_py._driver.exceptions", _exc_mod)

AskError = _exc_mod.AskError
AuthenticationError = _exc_mod.AuthenticationError
AuthenticationWrongNumberOfArgsError = _exc_mod.AuthenticationWrongNumberOfArgsError
BusyLoadingError = _exc_mod.BusyLoadingError
ClusterCrossSlotError = _exc_mod.ClusterCrossSlotError
ClusterDownError = _exc_mod.ClusterDownError
ClusterError = _exc_mod.ClusterError
ConnectionError = _exc_mod.ConnectionError  # noqa: A001
DataError = _exc_mod.DataError
ExecAbortError = _exc_mod.ExecAbortError
InvalidResponse = _exc_mod.InvalidResponse
LockError = _exc_mod.LockError
LockNotOwnedError = _exc_mod.LockNotOwnedError
MasterDownError = _exc_mod.MasterDownError
ModuleError = _exc_mod.ModuleError
MovedError = _exc_mod.MovedError
NoPermissionError = _exc_mod.NoPermissionError
NoScriptError = _exc_mod.NoScriptError
OutOfMemoryError = _exc_mod.OutOfMemoryError
PubSubError = _exc_mod.PubSubError
ReadOnlyError = _exc_mod.ReadOnlyError
RedisError = _exc_mod.RedisError
ResponseError = _exc_mod.ResponseError
SlaveError = _exc_mod.SlaveError
TimeoutError = _exc_mod.TimeoutError  # noqa: A001
TryAgainError = _exc_mod.TryAgainError
WatchError = _exc_mod.WatchError

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
    "ResponseError",
    "SlaveError",
    "TimeoutError",
    "TryAgainError",
    "WatchError",
]
