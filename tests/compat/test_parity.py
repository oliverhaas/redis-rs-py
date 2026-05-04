"""Parity tests — every ``implemented`` manifest entry, both clients.

The test ID for each entry is the manifest's ``command`` field, so a
failing test reads as

    FAILED tests/compat/test_parity.py::test_parity[GET]

Pytest collects one test per implemented entry. Skipped if no verifier
is registered (which would also fail the verifier-registration smoke
check, so we never expect this in CI).
"""

from __future__ import annotations

import pytest

from tests._compat_manifest import ManifestEntry, by_status
from tests.compat._verifiers import get as get_verifier

# Pin the entire parity suite to the "redis_global_state" xdist worker group.
# FLUSHALL, BGSAVE, BGREWRITEAOF, SCRIPT FLUSH, and FUNCTION FLUSH all affect
# server-global state (all databases, AOF file, Lua script cache). Running them
# in parallel with other tests in different groups would corrupt shared state.
# Using "redis_global_state" merges with the existing group used by
# test_commands_scripts.py and the facade admin tests, so all
# server-global operations serialize onto one worker.
pytestmark = pytest.mark.xdist_group("redis_global_state")

_IMPLEMENTED = by_status("implemented")


def _id(entry: ManifestEntry) -> str:
    return entry["command"]


@pytest.mark.parametrize("entry", _IMPLEMENTED, ids=_id)
def test_parity(entry: ManifestEntry, rs_client, py_client) -> None:
    fn = get_verifier(entry["command"])
    if fn is None:
        pytest.fail(
            f"no verifier registered for implemented command {entry['command']}; "
            f"add one to tests/compat/_verifiers/{entry['family']}.py",
        )
    fn(rs_client, py_client)
