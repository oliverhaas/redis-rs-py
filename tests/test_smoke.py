import redis_rs_py
from redis_rs_py import _driver


def test_driver_module_imports() -> None:
    assert hasattr(_driver, "__version__")


def test_package_exports_version() -> None:
    assert isinstance(redis_rs_py.__version__, str)
