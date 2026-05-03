import redis_rs_py
from redis_rs_py import _driver


def test_driver_module_imports() -> None:
    assert hasattr(_driver, "__version__")


def test_package_exports_version() -> None:
    assert isinstance(redis_rs_py.__version__, str)


def test_package_exports_driver_class() -> None:
    assert hasattr(redis_rs_py, "RedisRsDriver")


def test_package_exports_awaitable_class() -> None:
    assert hasattr(redis_rs_py, "RedisRsAwaitable")
