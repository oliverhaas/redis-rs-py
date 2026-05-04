"""run_in_thread + handler dispatch."""

import threading
import time

import pytest


def test_run_in_thread_dispatches_to_handler(redis_facade, publisher) -> None:
    ps = redis_facade.pubsub()
    received: list[dict] = []

    def handler(msg: dict) -> None:
        received.append(msg)

    ps.subscribe(ch1=handler)
    time.sleep(0.05)

    thread = ps.run_in_thread(sleep_time=0.05)
    try:
        for i in range(3):
            publisher.publish("ch1", f"m{i}".encode())

        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and len(received) < 3:
            time.sleep(0.05)
    finally:
        thread.stop()
        thread.join(timeout=2.0)

    assert len(received) == 3
    assert [m["data"] for m in received] == [b"m0", b"m1", b"m2"]


def test_run_in_thread_raises_when_no_handler(redis_facade) -> None:
    from redis_rs_py.exceptions import PubSubError

    ps = redis_facade.pubsub()
    try:
        ps.subscribe("orphan")
        with pytest.raises(PubSubError, match="orphan"):
            ps.run_in_thread()
    finally:
        ps.close()


def test_run_in_thread_invokes_exception_handler(redis_facade, publisher) -> None:
    ps = redis_facade.pubsub()
    crashes: list[BaseException] = []
    seen: list[dict] = []
    done = threading.Event()

    def boom(msg: dict) -> None:
        seen.append(msg)
        if len(seen) == 1:
            raise RuntimeError("first message kaboom")

    def on_exc(exc: BaseException, _pubsub, _thread) -> None:
        crashes.append(exc)
        done.set()

    ps.subscribe(boom_ch=boom)
    time.sleep(0.05)

    thread = ps.run_in_thread(sleep_time=0.05, exception_handler=on_exc)
    try:
        publisher.publish("boom_ch", b"first")
        assert done.wait(timeout=5.0)
        assert len(crashes) == 1
        assert isinstance(crashes[0], RuntimeError)
    finally:
        thread.stop()
        thread.join(timeout=2.0)


def test_run_in_thread_pattern_handler(redis_facade, publisher) -> None:
    ps = redis_facade.pubsub()
    received: list[dict] = []

    def handler(msg: dict) -> None:
        received.append(msg)

    ps.psubscribe(**{"sport.*": handler})
    time.sleep(0.05)

    thread = ps.run_in_thread(sleep_time=0.05)
    try:
        publisher.publish("sport.football", b"goal")
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not received:
            time.sleep(0.05)
    finally:
        thread.stop()
        thread.join(timeout=2.0)

    assert len(received) >= 1
    assert received[0]["pattern"] == b"sport.*"
    assert received[0]["channel"] == b"sport.football"


def test_thread_stop_is_idempotent(redis_facade) -> None:
    ps = redis_facade.pubsub()

    def h(_msg: dict) -> None:
        pass

    ps.subscribe(c1=h)
    thread = ps.run_in_thread(sleep_time=0.01)
    thread.stop()
    thread.stop()  # second call must not raise
    thread.join(timeout=2.0)
