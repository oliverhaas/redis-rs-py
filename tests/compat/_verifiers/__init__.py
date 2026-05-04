"""Per-command verifier registry.

A *verifier* is a callable
``f(rs_client, py_client) -> None`` that exercises one Redis command
through both clients with one or more representative inputs and
asserts the responses agree.

Verifiers live in family-specific modules (``strings.py``, ``lists.py``
...); each registers itself with this module's ``@verifier`` decorator.
The parity test collector looks up a command's verifier via ``get(name)``
and invokes it.

Conventions for verifier authors:

* The function name **does not** need to match the command — but the
  ``@verifier(...)`` argument **must** match the manifest's ``command``
  field exactly (case-sensitive).
* Use ``assert rs == py`` for byte-for-byte parity.
* For commands that return an unordered collection (e.g. SMEMBERS), the
  verifier MUST normalize before comparing: ``assert set(rs) == set(py)``.
* A verifier for a ``partial`` row goes through this registry exactly
  the same way; the test collector decides which test file invokes it.
* If both clients raise the same exception type with the same message,
  that's a pass; use ``_assert_same_error`` for that case.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Final

type VerifierFn = Callable[[object, object], None]

_REGISTRY: Final[dict[str, VerifierFn]] = {}


def verifier(command: str) -> Callable[[VerifierFn], VerifierFn]:
    """Decorator. Registers ``fn`` as the verifier for ``command``."""

    def _wrap(fn: VerifierFn) -> VerifierFn:
        if command in _REGISTRY:
            raise RuntimeError(f"duplicate verifier registration for {command}")
        _REGISTRY[command] = fn
        return fn

    return _wrap


def get(command: str) -> VerifierFn | None:
    """Return the verifier for ``command`` or None if unregistered."""
    return _REGISTRY.get(command)


def all_registered() -> list[str]:
    """List every command currently registered, in insertion order."""
    return list(_REGISTRY)


def _assert_same_error(rs_call: Callable[[], object], py_call: Callable[[], object]) -> None:
    """Run both callables; pass if both raise the same exception class."""
    rs_exc: BaseException | None = None
    py_exc: BaseException | None = None
    try:
        rs_call()
    except BaseException as e:
        rs_exc = e
    try:
        py_call()
    except BaseException as e:
        py_exc = e
    assert rs_exc is not None, f"rs_client did not raise; py_client raised {type(py_exc).__name__}"
    assert py_exc is not None, f"py_client did not raise; rs_client raised {type(rs_exc).__name__}"
    assert type(rs_exc).__name__ == type(py_exc).__name__, (
        f"different exception classes: rs={type(rs_exc).__name__} vs py={type(py_exc).__name__}"
    )


# Side-effect imports: each module's @verifier decorators register on import.
# Keep these alphabetical so a missing import is obvious.
from . import admin as _admin  # noqa: E402, F401
from . import hashes as _hashes  # noqa: E402, F401
from . import lists as _lists  # noqa: E402, F401
from . import scripts as _scripts  # noqa: E402, F401
from . import sets as _sets  # noqa: E402, F401
from . import streams as _streams  # noqa: E402, F401
from . import strings as _strings  # noqa: E402, F401
from . import zsets as _zsets  # noqa: E402, F401
