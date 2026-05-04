"""Partial-parity tests — every ``partial`` manifest entry runs through
the same verifier registry, but its verifier asserts only the relaxed
invariants documented in the entry's ``notes`` field.
"""

from __future__ import annotations

import pytest

from tests._compat_manifest import ManifestEntry, by_status
from tests.compat._verifiers import get as get_verifier

# Pin the entire partial-parity suite to the same xdist worker group as the parity suite.
# See test_parity.py for rationale.
pytestmark = pytest.mark.xdist_group("redis_global_state")

_PARTIAL = by_status("partial")


def _id(entry: ManifestEntry) -> str:
    return entry["command"]


@pytest.mark.parametrize("entry", _PARTIAL, ids=_id)
def test_partial(entry: ManifestEntry, rs_client, py_client) -> None:
    """Run the partial-mode verifier (asserts only what's stable)."""
    fn = get_verifier(entry["command"])
    if fn is None:
        pytest.skip(
            f"no verifier yet for partial command {entry['command']} (notes: {entry['notes']!r})",
        )
    fn(rs_client, py_client)


@pytest.mark.parametrize("entry", _PARTIAL, ids=_id)
def test_partial_has_notes(entry: ManifestEntry) -> None:
    """The manifest validator already enforces this, but a per-entry
    failure makes the divergence-without-explanation case loud."""
    assert entry["notes"], f"partial entry {entry['command']} must have a notes field explaining the divergence"
